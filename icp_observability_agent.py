#!/usr/bin/env python3
"""
ICP Splunk AI Observability Agent : Splunk Enterprise
=====================================
Multi-signal AI-powered observability for ICP Building, Curtin University.

Data sources:
  - Meraki MV12W cameras    (people count via MV Sense zone analytics)
  - Meraki MT10 sensors     (temperature, humidity)
  - Meraki MS355 switch     (port traffic, PoE, WAP client count)

Pipeline:
  Poll Meraki APIs → POST to Splunk HEC → OpenRouter AI generates narratives
  FastAPI web server exposes chatbot UI and REST endpoints

Requirements:
  pip install requests fastapi uvicorn httpx python-dotenv

Environment variables (.env):
  MERAKI_API_KEY        - Meraki Dashboard API key
  SPLUNK_HEC_URL        - e.g. https://127.0.0.1:8088/services/collector
  SPLUNK_HEC_TOKEN      - Splunk HEC token
  OPENROUTER_API_KEY    - OpenRouter API key
  OPENROUTER_BASE_URL   - https://openrouter.ai/api/v1
  OPENROUTER_REFERER    - your site/project URL
  MODEL_ANALYST         - model for scheduled analysis
  MODEL_COMPOSER        - model for interactive chat
  MODEL_FALLBACK        - fallback model on timeout
  POLL_INTERVAL         - seconds between polls (default 120)
  SPLUNK_MGMT_URL       - Splunk management API (default https://127.0.0.1:8089)
  SPLUNK_USERNAME       - Splunk admin username (default admin)
  SPLUNK_PASSWORD       - Splunk admin password (enables AI/ML search queries)

Usage:
  python icp_observability_agent.py
  Then open http://localhost:5000
"""

import os
import json
import time
import threading
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

import requests
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv
from camera_ml import OccupancyAnalyser

# ── Suppress SSL warnings for local Splunk HEC ──────────────────────────────
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Load config ──────────────────────────────────────────────────────────────
load_dotenv()

MERAKI_API_KEY    = os.getenv("MERAKI_API_KEY",    "")
SPLUNK_HEC_URL    = os.getenv("SPLUNK_HEC_URL",    "https://127.0.0.1:8088/services/collector")
SPLUNK_HEC_TOKEN  = os.getenv("SPLUNK_HEC_TOKEN",  "")
POLL_INTERVAL     = int(os.getenv("POLL_INTERVAL", "120"))

SPLUNK_MGMT_URL   = os.getenv("SPLUNK_MGMT_URL",  "https://127.0.0.1:8089")
SPLUNK_USERNAME   = os.getenv("SPLUNK_USERNAME",   "admin")
SPLUNK_PASSWORD   = os.getenv("SPLUNK_PASSWORD",   "")

MERAKI_BASE_URL   = "https://api.meraki.com/api/v1"
MERAKI_HEADERS    = {
    "X-Cisco-Meraki-API-Key": MERAKI_API_KEY,
    "Content-Type": "application/json",
    "Accept":       "application/json",
}

# Meraki org and network IDs — find these in your Meraki Dashboard URL
# or via: GET /organizations and GET /organizations/{orgId}/networks
ORG_ID     = os.getenv("MERAKI_ORG_ID",     "")
NETWORK_ID = os.getenv("MERAKI_NETWORK_ID", "")

# ── Device registry ──────────────────────────────────────────────────────────
# Replace serials below with your own Meraki device serials.
# Find them in Meraki Dashboard → Network → Devices, or via GET /devices.
SENSORS = [
    {"serial": "Q3CA-7SBV-ZCTA", "name": "ICP CRUX",
     "metrics": ["temperature", "humidity", "battery"]},
    {"serial": "Q3CC-WK69-K7TM", "name": "ICP SIDE DOOR",
     "metrics": ["door", "battery"]},
]

SWITCH = {
    "serial": "Q2DY-GJ53-PCYT",
    "name":   "ICP-SW01",
    "model":  "MS355-48X2",
    "ports": {
        "1":  "Corner Camera (MV12W)",
        "2":  "Table Camera (MV12W)",
        "13": "Bookshelf Camera (MV12W)",
        "43": "Kitchen WAP (MR57)",
        "45": "Intern Desk WAP (MR57)",
        "48": "MX105 Uplink",
    }
}

# Full Frame only for all cameras — named area zones use a different API endpoint
CAMERAS = [
    {"serial": "Q2GV-52D5-YTLK", "name": "ICP Pantry Area",
     "mac": "34:56:fe:a3:a7:c7",
     "zones": [{"id": "0", "label": "Full Frame"}]},
    {"serial": "Q2GV-MGCS-QRM5", "name": "ICP Demo Area",
     "mac": "34:56:fe:a3:a7:d6",
     "zones": [{"id": "0", "label": "Full Frame"}]},
    {"serial": "Q2GV-XPS8-82TX", "name": "ICP Workshop",
     "mac": "34:56:fe:a3:a7:cd",
     "zones": [{"id": "0", "label": "Full Frame"}]},
]

MT10_SERIAL = "Q3CA-7SBV-ZCTA"

# ── Anomaly thresholds ────────────────────────────────────────────────────────
OVERCROWD_THRESHOLD  = 5      # persons — MEDIUM flag above this
AFTER_HOURS_START    = 18     # 6 PM — any occupancy after this is flagged
AFTER_HOURS_END      = 7      # 7 AM — until this hour
THERMAL_SPIKE_DELTA  = 2.0    # °C rise over last 5 polls with zero occupancy → EQUIPMENT_THERMAL
THERMAL_ABS_NO_OCC   = 26.0   # absolute °C threshold when nobody is present
HUMIDITY_ABS_NO_OCC  = 65.0   # % RH threshold when nobody is present

# ── Camera ML analyser (shared across polling threads) ───────────────────────
analyser = OccupancyAnalyser()

# ── In-memory state ──────────────────────────────────────────────────────────
state = {
    "last_poll":      None,
    "temperature":    None,
    "humidity":       None,
    "door_open":      None,
    "downlink_kbps":  0.0,
    "wap_clients":    {},
    "port_traffic":   {},
    "port_poe":       {},
    "people_count":   {},
    "anomalies":            [],
    "splunk_ai_insights":   {},
    "automated_responses":  [],
    "camera_history":         {},
    "camera_history_fetched": None,
    "temp_history":    [],    # rolling last 10 readings [{ts, value}]
    "hum_history":     [],    # rolling last 10 readings [{ts, value}]
    "poll_count":     0,
    "errors":         [],
}

# ── Meraki API helper ────────────────────────────────────────────────────────
def meraki_get(path: str, params=None):
    url = f"{MERAKI_BASE_URL}{path}"
    r = requests.get(url, headers=MERAKI_HEADERS, params=params, timeout=15)
    if not r.ok:
        raise requests.HTTPError(
            f"{r.status_code} {r.reason} {r.text[:200]}", response=r)
    return r.json()

# ── Splunk HEC sender ────────────────────────────────────────────────────────
def send_to_splunk(events: list, sourcetype: str):
    if not SPLUNK_HEC_TOKEN:
        return
    headers = {
        "Authorization": f"Splunk {SPLUNK_HEC_TOKEN}",
        "Content-Type":  "application/json",
    }
    ts      = datetime.now(timezone.utc).timestamp()
    payload = ""
    for event in events:
        record = {
            "time":       ts,
            "sourcetype": sourcetype,
            "index":      "main",
            "host":       "brannigan",
            "event":      event,
        }
        payload += json.dumps(record) + "\n"
    try:
        r = requests.post(SPLUNK_HEC_URL, headers=headers,
                          data=payload, timeout=15, verify=False)
        return r.status_code
    except Exception as e:
        state["errors"].append(f"HEC error: {e}")

# ── Splunk REST API search client ────────────────────────────────────────────
def splunk_search(spl: str, max_wait: int = 25) -> list:
    """Execute an SPL query via Splunk management REST API; return result rows."""
    if not SPLUNK_PASSWORD:
        return []
    auth     = (SPLUNK_USERNAME, SPLUNK_PASSWORD)
    dispatch = "UNKNOWN"
    try:
        r = requests.post(
            f"{SPLUNK_MGMT_URL}/services/search/jobs",
            auth=auth,
            data={"search": spl, "output_mode": "json"},
            verify=False, timeout=15,
        )
        r.raise_for_status()
        sid = r.json()["sid"]

        deadline = time.time() + max_wait
        while time.time() < deadline:
            status = requests.get(
                f"{SPLUNK_MGMT_URL}/services/search/jobs/{sid}",
                auth=auth, params={"output_mode": "json"},
                verify=False, timeout=10,
            ).json()
            dispatch = status["entry"][0]["content"]["dispatchState"]
            if dispatch in ("DONE", "FAILED"):
                break
            time.sleep(1)

        if dispatch != "DONE":
            return []

        results = requests.get(
            f"{SPLUNK_MGMT_URL}/services/search/jobs/{sid}/results",
            auth=auth, params={"output_mode": "json", "count": 50},
            verify=False, timeout=10,
        ).json()
        return results.get("results", [])

    except Exception as e:
        state["errors"].append(f"Splunk search: {e}")
        return []


# ── Create Splunk saved alert via REST API ────────────────────────────────────
def create_splunk_alert(name: str, spl: str, severity: int = 2) -> bool:
    """Create or update a Splunk saved search/alert for persistent monitoring."""
    if not SPLUNK_PASSWORD:
        return False
    auth = (SPLUNK_USERNAME, SPLUNK_PASSWORD)
    body = {
        "search":         spl,
        "alert_type":     "always",
        "alert.severity": severity,
        "description":    (f"Auto-created by ICP AI Response Engine — "
                           f"{datetime.now().strftime('%Y-%m-%d %H:%M')}"),
    }
    try:
        # Try to update first (idempotent); fall back to create if it doesn't exist
        r = requests.post(
            f"{SPLUNK_MGMT_URL}/services/saved/searches/{name}",
            auth=auth, data=body, verify=False, timeout=15,
        )
        if r.ok:
            return True
        if r.status_code == 404:   # Doesn't exist yet — create it
            create_body = {"name": name, **body}
            r2 = requests.post(
                f"{SPLUNK_MGMT_URL}/services/saved/searches",
                auth=auth, data=create_body, verify=False, timeout=15,
            )
            return r2.ok
        return False
    except Exception as e:
        state["errors"].append(f"Splunk alert creation: {e}")
        return False


# ── Splunk AI: anomaly detection + forecasting ────────────────────────────────
def get_splunk_ai_insights() -> dict:
    """
    Query Splunk's built-in AI/ML commands against the ingested sensor and
    camera data already stored in Splunk.  Uses:
      - anomalydetection  (Splunk Enterprise built-in, statistical outlier detection)
      - predict / LLP5    (Splunk MLTK, temperature forecasting — skipped if not installed)

    Results are cached in state and injected into LLM prompts so that the AI
    narrative is grounded in Splunk-detected patterns, not just raw thresholds.
    """
    if not SPLUNK_PASSWORD:
        state["splunk_ai_insights"] = {
            "available": False,
            "reason":    "SPLUNK_PASSWORD not configured",
        }
        return state["splunk_ai_insights"]

    insights = {
        "available":           True,
        "temp_anomalies":      [],
        "humidity_anomalies":  [],
        "occupancy_anomalies": [],
        "temp_forecast":       None,
        "summary":             [],
    }

    # 1. Temperature anomaly detection (last 30 min)
    for row in splunk_search(
        'search index=main sourcetype="icp:sensor_latest" metric=temperature earliest=-30m '
        '| timechart avg(value) as temp span=5m '
        '| anomalydetection temp action=annotate'
    ):
        if str(row.get("isOutlier", "0")) == "1":
            insights["temp_anomalies"].append(
                {"time": row.get("_time", ""), "value": row.get("temp", "")})

    # 2. Humidity anomaly detection (last 30 min)
    for row in splunk_search(
        'search index=main sourcetype="icp:sensor_latest" metric=humidity earliest=-30m '
        '| timechart avg(value) as hum span=5m '
        '| anomalydetection hum action=annotate'
    ):
        if str(row.get("isOutlier", "0")) == "1":
            insights["humidity_anomalies"].append(
                {"time": row.get("_time", ""), "value": row.get("hum", "")})

    # 3. Camera occupancy anomaly detection (last 1 h)
    for row in splunk_search(
        'search index=main sourcetype="icp:camera_analytics" earliest=-1h '
        '| timechart avg(averageCount) as occupancy span=5m '
        '| anomalydetection occupancy action=annotate'
    ):
        if str(row.get("isOutlier", "0")) == "1":
            insights["occupancy_anomalies"].append(
                {"time": row.get("_time", ""), "value": row.get("occupancy", "")})

    # 4. Temperature forecast via MLTK predict/LLP5 (skips gracefully if MLTK absent)
    forecast_rows = splunk_search(
        'search index=main sourcetype="icp:sensor_latest" metric=temperature earliest=-1h '
        '| timechart avg(value) as temp span=5m '
        '| predict temp future_timespan=6 algorithm=LLP5 holdback=0 '
        '  lower95=lower95 upper95=upper95'
    )
    future_points = [r for r in forecast_rows
                     if r.get("prediction(temp)") and not r.get("temp")]
    if future_points:
        last = future_points[-1]
        insights["temp_forecast"] = {
            "time":    last.get("_time", ""),
            "value":   last.get("prediction(temp)", ""),
            "lower95": last.get("lower95(prediction(temp))", ""),
            "upper95": last.get("upper95(prediction(temp))", ""),
        }

    # Build summary lines
    total = (len(insights["temp_anomalies"]) +
             len(insights["humidity_anomalies"]) +
             len(insights["occupancy_anomalies"]))

    if total == 0:
        insights["summary"].append(
            "Splunk anomalydetection: no statistical outliers in last 30 min")
    else:
        for label, key in [("temperature", "temp_anomalies"),
                            ("humidity",    "humidity_anomalies"),
                            ("occupancy",   "occupancy_anomalies")]:
            n = len(insights[key])
            if n:
                insights["summary"].append(
                    f"Splunk anomalydetection: {n} {label} outlier(s) detected")

    if insights["temp_forecast"]:
        try:
            v = float(insights["temp_forecast"]["value"])
            insights["summary"].append(
                f"Splunk predict/LLP5: temperature forecast ~{v:.1f}°C (next 30 min)")
        except (ValueError, TypeError):
            pass

    # Push Splunk AI summary back as its own sourcetype
    send_to_splunk([{
        "source":            "splunk_ai",
        "total_anomalies":   total,
        "forecast_enabled":  insights["temp_forecast"] is not None,
        "summary":           " | ".join(insights["summary"]) or "clean",
    }], "icp:splunk_ai_insight")

    state["splunk_ai_insights"] = insights
    return insights


# ── Automated AI Response Engine ─────────────────────────────────────────────
def run_automated_response(insights: dict) -> None:
    """
    Closed-loop AI response:  detect → LLM interprets → Splunk records.
    Two tiers of input:
      1. Splunk AI statistical anomalies (anomalydetection outliers) — primary
      2. Threshold-based HIGH/MEDIUM anomalies from state["anomalies"] — secondary
    A 5-poll cooldown per signal prevents duplicate responses.
    """
    COOLDOWN_POLLS = 5
    cooldown       = state.setdefault("auto_response_cooldown", {})
    current_poll   = state["poll_count"]

    signals_to_respond = []

    # Tier 1: Splunk AI statistical anomalies
    if insights.get("available"):
        for label, key in [
            ("temperature", "temp_anomalies"),
            ("humidity",    "humidity_anomalies"),
            ("occupancy",   "occupancy_anomalies"),
        ]:
            items = insights.get(key, [])
            if items:
                ck = f"splunk_{label}"
                if ck not in cooldown or current_poll - cooldown[ck] >= COOLDOWN_POLLS:
                    signals_to_respond.append({
                        "signal":       label,
                        "details":      items,
                        "source":       "Splunk AI anomalydetection (statistical outlier)",
                        "severity":     "HIGH",
                        "cooldown_key": ck,
                        "alert_spl": (
                            f'search index=main sourcetype="icp:sensor_latest" '
                            f'metric={label} earliest=-30m '
                            f'| timechart avg(value) as v span=5m '
                            f'| anomalydetection v | where isOutlier=1'
                        ),
                    })

    # Tier 2: Threshold-based HIGH/MEDIUM anomalies
    for anomaly in state["anomalies"]:
        if anomaly.get("severity") not in ("HIGH", "MEDIUM"):
            continue
        sig = anomaly.get("signal", "")
        ck  = f"threshold_{sig}"
        if ck not in cooldown or current_poll - cooldown[ck] >= COOLDOWN_POLLS:
            signals_to_respond.append({
                "signal":       sig,
                "details":      [{"value":   anomaly.get("value"),
                                  "message": anomaly.get("message")}],
                "source":       (f"Threshold breach ({anomaly.get('severity')}): "
                                 f"{anomaly.get('message')}"),
                "severity":     anomaly.get("severity"),
                "cooldown_key": ck,
                "alert_spl": (
                    'search index=main sourcetype="icp:switch_port_status" '
                    'portId="48" earliest=-30m '
                    '| timechart avg(trafficInKbps_total) as v span=5m '
                    '| anomalydetection v | where isOutlier=1'
                    if "traffic" in sig else
                    f'search index=main sourcetype="icp:anomaly" '
                    f'signal="{sig}" earliest=-30m | stats count'
                ),
            })

    if not signals_to_respond:
        return

    for item in signals_to_respond:
        signal   = item["signal"]
        details  = item["details"]
        src      = item["source"]
        severity = item["severity"]
        ck       = item["cooldown_key"]

        ctx = build_context()
        incident_question = (
            f"{src} — signal: {signal}, details: {details}. "
            f"Write a 2-sentence operational incident report: "
            f"(1) state exactly what was detected (value, time, source), "
            f"(2) recommend an immediate operational action. "
            f"Reference Splunk AI findings from the context. "
            f"Begin with 'SPLUNK AI ALERT —'"
        )
        incident_report = generate_narrative(ctx, incident_question)

        alert_name    = f"ICP_AI_{signal.replace(' ','_')}_anomaly"
        alert_created = create_splunk_alert(alert_name, item["alert_spl"], severity=2)

        send_to_splunk([{
            "signal":               signal,
            "anomaly_source":       src,
            "anomaly_details":      str(details[:3]),
            "splunk_ai_model":      "anomalydetection",
            "llm_analyst_model":    os.getenv("MODEL_ANALYST",  ""),
            "llm_composer_model":   os.getenv("MODEL_COMPOSER", ""),
            "incident_report":      incident_report,
            "splunk_alert_created": alert_created,
            "splunk_alert_name":    alert_name,
            "severity":             severity,
            "automated":            True,
        }], "icp:automated_response")

        cooldown[ck] = current_poll

        state["automated_responses"].insert(0, {
            "ts":         datetime.now().strftime("%H:%M"),
            "signal":     signal,
            "source":     src,
            "summary":    incident_report,
            "alert_ok":   alert_created,
            "alert_name": alert_name,
            "severity":   severity,
        })

    state["automated_responses"] = state["automated_responses"][:10]


# ── Poll: MT10 sensors ───────────────────────────────────────────────────────
def poll_sensors():
    try:
        data = meraki_get(
            f"/organizations/{ORG_ID}/sensor/readings/latest",
            params={"serials[]": [s["serial"] for s in SENSORS]},
        )
        events = []
        for sensor_data in data:
            serial      = sensor_data.get("serial")
            sensor_name = next(
                (s["name"] for s in SENSORS if s["serial"] == serial), serial)
            for reading in sensor_data.get("readings", []):
                metric = reading.get("metric")
                event  = {
                    "serial":      serial,
                    "sensor_name": sensor_name,
                    "metric":      metric,
                    "ts":          reading.get("ts"),
                }
                if metric == "temperature":
                    val = reading["temperature"]["celsius"]
                    event["temperature_celsius"] = val
                    event["value"] = val
                    if serial == MT10_SERIAL:
                        state["temperature"] = val
                        state["temp_history"].append(val)
                        state["temp_history"] = state["temp_history"][-10:]
                elif metric == "humidity":
                    val = reading["humidity"]["relativePercentage"]
                    event["humidity_pct"] = val
                    event["value"] = val
                    if serial == MT10_SERIAL:
                        state["humidity"] = val
                        state["hum_history"].append(val)
                        state["hum_history"] = state["hum_history"][-10:]
                elif metric == "door":
                    val = reading["door"]["open"]
                    event["door_open"] = val
                    event["value"]     = 1 if val else 0
                    state["door_open"] = val
                elif metric == "battery":
                    val = reading["battery"]["percentage"]
                    event["battery_pct"] = val
                    event["value"]       = val
                events.append(event)
        send_to_splunk(events, "icp:sensor_latest")
        return len(events)
    except Exception as e:
        state["errors"].append(f"Sensor poll error: {e}")
        return 0

# ── Poll: MS355 switch ports ─────────────────────────────────────────────────
def poll_switch():
    try:
        ports  = meraki_get(f"/devices/{SWITCH['serial']}/switch/ports/statuses")
        events = []
        for port in ports:
            port_id   = str(port.get("portId", ""))
            port_name = SWITCH["ports"].get(port_id, f"Port {port_id}")
            event = {
                "serial":               SWITCH["serial"],
                "device_name":          SWITCH["name"],
                "model":                SWITCH["model"],
                "portId":               port_id,
                "port_name":            port_name,
                "status":               port.get("status"),
                "speed":                port.get("speed", ""),
                "clientCount":          port.get("clientCount", 0),
                "powerUsageInWh":       port.get("powerUsageInWh", 0.0),
                "trafficInKbps_total":  port.get("trafficInKbps", {}).get("total", 0.0),
                "trafficInKbps_sent":   port.get("trafficInKbps", {}).get("sent",  0.0),
                "trafficInKbps_recv":   port.get("trafficInKbps", {}).get("recv",  0.0),
                "poe_allocated":        port.get("poe", {}).get("isAllocated", False),
                "lldp_name":            port.get("lldp", {}).get("systemName", ""),
            }
            if port_id in ("43", "45"):
                state["wap_clients"][port_id] = port.get("clientCount", 0)
            if port.get("status") == "Connected":
                traffic = port.get("trafficInKbps", {})
                state["port_traffic"][port_id] = traffic.get("total", 0.0)
                state["port_poe"][port_id]      = port.get("powerUsageInWh", 0.0)
                if port_id == "48":
                    state["downlink_kbps"] = traffic.get("recv", 0.0)
            events.append(event)
        send_to_splunk(events, "icp:switch_port_status")
        return len(events)
    except Exception as e:
        state["errors"].append(f"Switch poll error: {e}")
        return 0

# ── Poll: MV12W camera zone history (last 1 hour) ───────────────────────────
def poll_cameras():
    total = 0
    now   = int(datetime.now(timezone.utc).timestamp())
    t0    = now - 3600          # ← 1 hour lookback (was 600s)

    for cam in CAMERAS:
        for zone in cam["zones"]:
            try:
                history = meraki_get(
                    f"/devices/{cam['serial']}/camera/analytics/zones"
                    f"/{zone['id']}/history",
                    params={"t0": t0, "t1": now},
                )

                # Flag cameras that are initialising (no history yet)
                initialising = not history

                # Use latest non-zero bucket; fall back to last bucket
                if history:
                    non_zero = [
                        h for h in history
                        if h.get("averageCount", 0) > 0
                        or h.get("entrances",    0) > 0
                    ]
                    latest    = non_zero[-1] if non_zero else history[-1]
                    people    = latest.get("averageCount", 0.0)
                    entrances = latest.get("entrances",    0)
                    start_ts  = latest.get("startTs", "")
                    end_ts    = latest.get("endTs",   "")
                else:
                    people    = 0.0
                    entrances = 0
                    start_ts  = ""
                    end_ts    = ""

                state["people_count"][f"{cam['name']}_{zone['label']}"] = {
                    "count":        people,
                    "entrances":    entrances,
                    "camera":       cam["name"],
                    "zone":         zone["label"],
                    "initialising": initialising,
                }

                if not initialising:
                    send_to_splunk([{
                        "camera_serial": cam["serial"],
                        "camera_name":   cam["name"],
                        "zone_id":       zone["id"],
                        "zone_label":    zone["label"],
                        "averageCount":  people,
                        "entrances":     entrances,
                        "startTs":       start_ts,
                        "endTs":         end_ts,
                    }], "icp:camera_analytics")
                total += 1

            except Exception as e:
                state["errors"].append(
                    f"Camera {cam['name']} zone {zone['label']}: {e}")
    return total

# ── Poll: 24-hour camera history + ML analysis ───────────────────────────────
def poll_camera_history_24h():
    """
    Fetch full 24h zone history (2×12h API calls) for every camera in parallel,
    run the ML anomaly engine, send anomalies to Splunk, and cache results.
    Runs in its own daemon thread every 10 minutes.
    """
    now = int(datetime.now(timezone.utc).timestamp())
    mid = now - 43200   # 12 h ago
    t0  = now - 86400   # 24 h ago

    def _fetch_one(cam):
        zone_id = cam["zones"][0]["id"]
        try:
            p1 = meraki_get(
                f"/devices/{cam['serial']}/camera/analytics/zones/{zone_id}/history",
                params={"t0": t0, "t1": mid},
            ) or []
            p2 = meraki_get(
                f"/devices/{cam['serial']}/camera/analytics/zones/{zone_id}/history",
                params={"t0": mid, "t1": now},
            ) or []
            return cam["name"], p1 + p2, None
        except Exception as exc:
            return cam["name"], [], exc

    cam_data = {}
    with ThreadPoolExecutor(max_workers=len(CAMERAS)) as ex:
        futures = {ex.submit(_fetch_one, cam): cam for cam in CAMERAS}
        for fut in as_completed(futures):
            name, buckets, err = fut.result()
            if err:
                state["errors"].append(f"Camera 24h {name}: {err}")
                continue

            anomalies = analyser.ingest(name, buckets)
            stats     = analyser.get_stats(name)
            forecast  = analyser.forecast_next(name)
            chart     = analyser.export_for_chart(name)

            cam_data[name] = {
                "chart":     chart,
                "stats":     stats,
                "anomalies": anomalies,
                "forecast":  forecast,
            }

            if anomalies:
                send_to_splunk([{
                    "camera_name":  name,
                    "anomaly_type": a["type"],
                    "severity":     a["severity"],
                    "message":      a["message"],
                    "value":        a["value"],
                    "bucket_time":  a["time"],
                } for a in anomalies[:10]], "icp:camera_ml_anomaly")

    state["camera_history"]         = cam_data
    state["camera_history_fetched"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_anoms = sum(len(d.get("anomalies", [])) for d in cam_data.values())
    print(f"[CamHistory] Updated — {len(cam_data)} cameras, {total_anoms} ML anomalies detected")
    return cam_data


def camera_history_loop():
    """Daemon thread: refresh 24h camera ML analytics every 10 minutes."""
    print("[CamHistory] Starting — interval 600s")
    import time as _t
    _t.sleep(8)     # let main poller start first
    while True:
        try:
            poll_camera_history_24h()
        except Exception as e:
            state["errors"].append(f"Camera history loop: {e}")
        _t.sleep(600)


# ── Anomaly detection ────────────────────────────────────────────────────────
def detect_anomalies():
    anomalies = []
    temp   = state.get("temperature")
    hum    = state.get("humidity")
    uplink = state["port_traffic"].get("48", 0)

    if temp is not None and temp > 24.0:
        anomalies.append({
            "type":      "THRESHOLD_BREACH",
            "signal":    "temperature",
            "value":     temp,
            "threshold": 24.0,
            "message":   f"Temperature {temp:.1f}°C exceeds 24°C threshold",
            "severity":  "HIGH" if temp > 28 else "MEDIUM",
        })

    if hum is not None and hum > 70.0:
        anomalies.append({
            "type":      "THRESHOLD_BREACH",
            "signal":    "humidity",
            "value":     hum,
            "threshold": 70.0,
            "message":   f"Humidity {hum:.0f}% exceeds 70% threshold",
            "severity":  "MEDIUM",
        })

    if state.get("door_open"):
        anomalies.append({
            "type":     "ACCESS_EVENT",
            "signal":   "door",
            "value":    1,
            "message":  "ICP SIDE DOOR is currently open",
            "severity": "INFO",
        })

    total_wap = sum(state["wap_clients"].values())
    if total_wap > 10:
        anomalies.append({
            "type":     "OCCUPANCY_HIGH",
            "signal":   "wap_clients",
            "value":    total_wap,
            "message":  f"High occupancy: {total_wap} WiFi clients on WAPs",
            "severity": "INFO",
        })

    if uplink > 5000:
        anomalies.append({
            "type":     "TRAFFIC_HIGH",
            "signal":   "uplink_traffic",
            "value":    uplink,
            "message":  f"High uplink traffic: {uplink:.0f} Kbps on MX105 port",
            "severity": "MEDIUM",
        })

    # ── Cross-signal: thermal spike with zero camera occupancy ──────────────────
    total_occ = sum(
        v.get("count", 0) for v in state["people_count"].values()
        if not v.get("initialising")
    )
    temp_hist = state.get("temp_history", [])
    hum_hist  = state.get("hum_history",  [])

    if total_occ == 0 and temp is not None:
        # Rate-of-change spike: rose ≥ THERMAL_SPIKE_DELTA over last 5 readings
        if len(temp_hist) >= 3:
            temp_rise = temp - min(temp_hist[-5:])
            if temp_rise >= THERMAL_SPIKE_DELTA:
                anomalies.append({
                    "type":      "EQUIPMENT_THERMAL",
                    "signal":    "temp_no_occupancy",
                    "value":     temp,
                    "message":   (f"Temperature rose {temp_rise:.1f}°C with zero camera "
                                  f"occupancy — possible equipment overheating ({temp:.1f}°C)"),
                    "severity":  "HIGH",
                })
        # Absolute threshold breach with nobody present
        if temp > THERMAL_ABS_NO_OCC:
            anomalies.append({
                "type":      "EQUIPMENT_THERMAL",
                "signal":    "temp_no_occupancy",
                "value":     temp,
                "message":   (f"Temperature {temp:.1f}°C with zero camera occupancy "
                              f"— no human heat source, check equipment"),
                "severity":  "MEDIUM",
            })

    if total_occ == 0 and hum is not None and hum > HUMIDITY_ABS_NO_OCC:
        if len(hum_hist) >= 2:
            hum_rise = hum - min(hum_hist[-5:])
            severity = "HIGH" if hum_rise >= 5.0 else "MEDIUM"
            anomalies.append({
                "type":      "EQUIPMENT_THERMAL",
                "signal":    "humidity_no_occupancy",
                "value":     hum,
                "message":   (f"Humidity {hum:.0f}% with zero camera occupancy "
                              f"(+{hum_rise:.0f}% recent rise) — possible moisture/HVAC issue"),
                "severity":  severity,
            })

    # ── Occupancy: overcrowding and after-hours presence ─────────────────────
    current_hour = datetime.now().hour
    after_hours  = current_hour >= AFTER_HOURS_START or current_hour < AFTER_HOURS_END

    for cam_val in state["people_count"].values():
        if cam_val.get("initialising"):
            continue
        count    = cam_val.get("count", 0.0)
        cam_name = cam_val.get("camera", "unknown")

        if count > OVERCROWD_THRESHOLD:
            anomalies.append({
                "type":     "OVERCROWDING",
                "signal":   "camera_occupancy",
                "value":    round(count, 1),
                "message":  (f"{cam_name}: {count:.0f} persons — "
                             f"overcrowding threshold ({OVERCROWD_THRESHOLD}) exceeded"),
                "severity": "MEDIUM",
                "camera":   cam_name,
            })

        if count > 0 and after_hours:
            anomalies.append({
                "type":     "AFTER_HOURS_PRESENCE",
                "signal":   "camera_occupancy",
                "value":    round(count, 1),
                "message":  (f"{cam_name}: {count:.0f} person(s) detected "
                             f"after hours ({current_hour:02d}:00)"),
                "severity": "HIGH",
                "camera":   cam_name,
            })

    # Camera ML anomalies — HIGH/MEDIUM only (LOW visible via /api/camera/history)
    for cam_name, cd in state.get("camera_history", {}).items():
        for anom in cd.get("anomalies", []):
            sev = anom.get("severity", "LOW")
            if sev not in ("HIGH", "MEDIUM"):
                continue
            anomalies.append({
                "type":     f"CAMERA_ML_{anom['type']}",
                "signal":   "camera_occupancy",
                "value":    round(anom.get("value", 0), 1),
                "message":  anom.get("message", ""),
                "severity": sev,
                "camera":   cam_name,
            })

    state["anomalies"] = anomalies
    if anomalies:
        send_to_splunk(anomalies, "icp:anomaly")
    return anomalies

# ── Build context snapshot for AI ───────────────────────────────────────────
def build_context() -> str:
    ts        = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    temp      = state.get("temperature")
    hum       = state.get("humidity")
    door      = state.get("door_open")
    wap       = sum(state["wap_clients"].values())
    uplink    = state["port_traffic"].get("48", 0)
    downlink  = state.get("downlink_kbps", 0.0)
    total_poe = sum(state["port_poe"].values())

    # Camera summary — show initialising state clearly
    cam_lines = []
    for val in state["people_count"].values():
        if val.get("initialising"):
            cam_lines.append(
                f"  {val['camera']} / {val['zone']}: ⏳ Initialising MV Sense")
        else:
            cam_lines.append(
                f"  {val['camera']} / {val['zone']}: "
                f"{val['count']:.1f} avg occupancy, {val['entrances']} entrances")
    cam_summary  = "\n".join(cam_lines) if cam_lines else "  No camera data available"

    # Anomaly summary
    anom_lines   = [f"  [{a['severity']}] {a['message']}" for a in state["anomalies"]]
    anom_summary = "\n".join(anom_lines) if anom_lines else "  None detected"

    # Active ports
    port_lines = []
    for pid, traffic in state["port_traffic"].items():
        name       = SWITCH["ports"].get(pid, f"Port {pid}")
        poe        = state["port_poe"].get(pid, 0)
        clients    = state["wap_clients"].get(pid, "")
        client_str = f", {clients} clients" if clients != "" else ""
        port_lines.append(
            f"  Port {pid} ({name}): {traffic:.1f} Kbps{client_str}, PoE {poe:.1f} Wh")
    port_summary = "\n".join(port_lines) if port_lines else "  No active ports"

    # Camera ML 24h baseline
    cam_ml_lines = []
    for cam_name, cd in state.get("camera_history", {}).items():
        stats = cd.get("stats", {})
        if not stats:
            continue
        fc = cd.get("forecast")
        n  = stats.get("anomaly_count", 0)
        line = (
            f"  {cam_name}: "
            f"peak {stats.get('peak_occupancy', 0):.1f} ppl "
            f"@ {stats.get('peak_time', 'N/A')}, "
            f"{stats.get('total_entrances', 0)} entrances/24h, "
            f"{n} ML anomal{'ies' if n != 1 else 'y'}"
        )
        if fc:
            line += f", trend {fc['trend']} (→{fc['next_occ']:.1f} ppl)"
        cam_ml_lines.append(line)
    fetched = state.get("camera_history_fetched")
    if cam_ml_lines:
        cam_ml_summary = "\n".join(cam_ml_lines)
        if fetched:
            cam_ml_summary += f"\n  (Updated: {fetched})"
    else:
        cam_ml_summary = "  Pending first 24h fetch (runs ~8s after startup)..."

    # Splunk AI section
    ai = state.get("splunk_ai_insights", {})
    if ai.get("available"):
        ai_lines = [f"  {s}" for s in ai.get("summary", [])]
        if not ai_lines:
            ai_lines = ["  Running first analysis..."]
        fc = ai.get("temp_forecast")
        if fc:
            ai_lines.append(
                f"  MLTK forecast: temp ~{fc.get('value','?')}°C "
                f"(95% CI {fc.get('lower95','?')}–{fc.get('upper95','?')}°C)")
    elif ai:
        ai_lines = [f"  Unavailable: {ai.get('reason', 'unknown')}"]
    else:
        ai_lines = ["  Awaiting first Splunk AI query..."]
    splunk_ai_section = "\n".join(ai_lines)

    # Automated responses summary
    ar_lines = []
    for ar in state.get("automated_responses", [])[:3]:
        ar_lines.append(
            f"  [{ar['ts']}] {ar['signal'].upper()} ({ar['severity']}): "
            f"{ar['summary'][:120]}...")
    ar_summary = "\n".join(ar_lines) if ar_lines else "  None triggered yet — monitoring active"

    return f"""ICP Building Real-Time Status — {ts}
Location: Curtin University, Bentley WA

ENVIRONMENTAL (ICP CRUX - MT10):
  Temperature : {f"{temp:.1f}°C" if temp is not None else "N/A"} (alert: 24°C occupied / {THERMAL_ABS_NO_OCC}°C unoccupied)
  Humidity    : {f"{hum:.0f}%" if hum is not None else "N/A"} (alert: 70% occupied / {HUMIDITY_ABS_NO_OCC}% unoccupied)
  Side Door   : {"OPEN" if door else "CLOSED" if door is not None else "N/A"}

NETWORK (ICP-SW01 MS355-48X2):
  Uplink Traffic   : {uplink:.1f} Kbps (Port 48 → MX105)
  Downlink Traffic : {downlink:.1f} Kbps
  Total PoE      : {total_poe:.1f} Wh
  WAP Clients    : {wap} total (Kitchen + Intern Desk)
  Active Ports:
{port_summary}

CAMERA ANALYTICS (MV12W):
{cam_summary}

ANOMALIES DETECTED:
{anom_summary}

CAMERA ML ANALYTICS (24h baseline · Z-score · surge · dead-zone · trend):
{cam_ml_summary}

SPLUNK AI INSIGHTS (anomalydetection · predict/LLP5):
{splunk_ai_section}

AUTOMATED AI RESPONSES (Splunk AI → LLM → Splunk):
{ar_summary}

SYSTEM: Poll #{state['poll_count']} | Interval: {POLL_INTERVAL}s
"""

# ── OpenRouter AI narrative generator ───────────────────────────────────────
def generate_narrative(context: str, question: str = None) -> str:
    api_key  = os.getenv("OPENROUTER_API_KEY",  "")
    base_url = os.getenv("OPENROUTER_BASE_URL",  "https://openrouter.ai/api/v1")
    referer  = os.getenv("OPENROUTER_REFERER",   "https://curtin.edu.au")

    # Use composer (fast) for chat, analyst (powerful) for scheduled reports
    model = (os.getenv("MODEL_COMPOSER", "moonshotai/kimi-k2.6:free")
             if question
             else os.getenv("MODEL_ANALYST", "openai/gpt-oss-120b:free"))

    if not api_key:
        return ("⚠️ OPENROUTER_API_KEY not configured. "
                "Add it to .env to enable AI narratives.")

    system = """You are the ICP Splunk AI Observability Agent : Splunk Enterprise for the Cisco-Curtin ICP Lab
at Curtin University, Perth. You monitor environmental sensors (Cisco Meraki MT10),
network infrastructure (MS355-48X2 switch, MR57 WAPs), and camera analytics (MV12W)
in real time, with Splunk MLTK AI providing statistical anomaly detection and forecasting.

Your role:
- Analyse multi-signal data (temperature, humidity, door, network traffic, PoE, people count)
- Detect patterns and correlations across signals
- Generate clear, concise operational narratives for engineering teams and academics
- Flag anomalies with actionable recommendations
- Keep responses focused and technical but accessible

CRITICAL: Your responses MUST reference BOTH the SPLUNK AI INSIGHTS and CAMERA ML ANALYTICS sections.
Cite Splunk AI findings using phrases such as:
  "According to Splunk anomalydetection..."
  "Splunk predict/LLP5 forecasts..."
  "Splunk AI has identified..."
  "Splunk MLTK analysis shows..."
This makes the Splunk AI contribution to your analysis clearly visible.
If Splunk AI shows no outliers, explicitly state that — e.g. "Splunk anomalydetection confirms all signals are within statistical norms."
Cite Camera ML using phrases such as:
  "Camera ML baseline analysis shows..."
  "The 24h occupancy trend indicates..."
  "ML anomaly detection flagged a crowd surge at..."
  "Peak occupancy for this camera was..."

When answering questions, always ground your response in the actual data provided.
If values are within normal ranges, say so clearly. Be specific with numbers."""

    user_msg = (
        f"Current ICP Building data:\n\n{context}\n\nQuestion: {question}"
        if question else
        f"""Analyse the current ICP Building status and provide:
1. A one-paragraph operational summary
2. Any anomalies or concerns with recommended actions
3. Notable patterns or correlations across signals

Current data:\n\n{context}"""
    )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "HTTP-Referer":  referer,
        "X-Title":       "ICP Splunk AI Observability Agent : Splunk Enterprise",
        "Content-Type":  "application/json",
    }
    payload = {
        "model":      model,
        "max_tokens": 600,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user_msg},
        ],
    }

    try:
        r = requests.post(
            f"{base_url}/chat/completions",
            headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]

    except requests.exceptions.Timeout:
        fallback = os.getenv("MODEL_FALLBACK", "openai/gpt-oss-20b:free")
        try:
            payload["model"] = fallback
            r = requests.post(
                f"{base_url}/chat/completions",
                headers=headers, json=payload, timeout=20)
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"]
        except Exception as e:
            return f"⚠️ AI narrative unavailable (fallback also failed): {e}"

    except Exception as e:
        return f"⚠️ AI narrative unavailable: {e}"


# ── Background polling loop ──────────────────────────────────────────────────
def polling_loop():
    print(f"[Poller] Starting — interval {POLL_INTERVAL}s")
    while True:
        try:
            state["poll_count"] += 1
            t0 = time.time()
            s  = poll_sensors()
            sw = poll_switch()
            c  = poll_cameras()
            detect_anomalies()
            insights = get_splunk_ai_insights()
            run_automated_response(insights)
            state["last_poll"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            elapsed = time.time() - t0
            ar_count = len(state["automated_responses"])
            print(f"[Poller] #{state['poll_count']} — "
                  f"sensors:{s} switch:{sw} camera:{c} "
                  f"anomalies:{len(state['anomalies'])} "
                  f"auto-responses:{ar_count} ({elapsed:.1f}s)")

            state["errors"] = state["errors"][-10:]   # keep last 10 only

        except Exception as e:
            print(f"[Poller] ERROR: {e}")
            state["errors"].append(str(e))

        time.sleep(POLL_INTERVAL)

# ── FastAPI app ──────────────────────────────────────────────────────────────
app = FastAPI(title="ICP Splunk AI Observability Agent : Splunk Enterprise", version="2.0")

_LOGO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ICP transparent logo.png")

class ChatRequest(BaseModel):
    message: str

@app.get("/static/icp-logo")
async def serve_icp_logo():
    return FileResponse(_LOGO_PATH, media_type="image/png")

@app.get("/", response_class=HTMLResponse)
async def root():
    return HTML_UI

@app.get("/api/status")
async def get_status():
    return {
        "last_poll":      state["last_poll"],
        "poll_count":     state["poll_count"],
        "temperature":    state["temperature"],
        "humidity":       state["humidity"],
        "door_open":      state["door_open"],
        "wap_clients":    sum(state["wap_clients"].values()),
        "uplink_kbps":    state["port_traffic"].get("48", 0),
        "downlink_kbps":  state.get("downlink_kbps", 0),
        "total_poe_wh":   sum(state["port_poe"].values()),
        "people_count":   state["people_count"],
        "anomaly_count":      len(state["anomalies"]),
        "anomalies":          state["anomalies"],
        "splunk_ai_insights":   state.get("splunk_ai_insights", {}),
        "automated_responses":  state["automated_responses"][:5],
        "errors":               state["errors"][-3:],
    }

@app.get("/api/context")
async def get_context():
    return {"context": build_context()}

@app.get("/api/history")
def get_history():
    """Query Splunk for the last 12 hours of time-series data to pre-populate charts."""
    SPAN = "2m"
    queries = {
        "temp": (
            f'search index=main sourcetype="icp:sensor_latest" metric=temperature earliest=-12h'
            f' | timechart avg(value) as v span={SPAN}', "v"),
        "hum": (
            f'search index=main sourcetype="icp:sensor_latest" metric=humidity earliest=-12h'
            f' | timechart avg(value) as v span={SPAN}', "v"),
        "uplink": (
            f'search index=main sourcetype="icp:switch_port_status" portId="48" earliest=-12h'
            f' | timechart avg(trafficInKbps_total) as v span={SPAN}', "v"),
        "downlink": (
            f'search index=main sourcetype="icp:switch_port_status" portId="48" earliest=-12h'
            f' | timechart avg(trafficInKbps_recv) as v span={SPAN}', "v"),
        "poe": (
            f'search index=main sourcetype="icp:switch_port_status" earliest=-12h'
            f' | timechart sum(powerUsageInWh) as v span={SPAN}', "v"),
    }

    def run(name, spl, field):
        rows = splunk_search(spl, max_wait=30)
        pts  = []
        for r in rows:
            t = r.get("_time", "")
            try:
                v = float(r[field]) if r.get(field) else None
            except (ValueError, TypeError):
                v = None
            pts.append({"t": t[11:16], "v": v})   # keep HH:MM only
        return name, pts

    results = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(run, n, spl, f): n for n, (spl, f) in queries.items()}
        for future in as_completed(futures):
            name, pts = future.result()
            results[name] = pts

    return results

@app.post("/api/chat")
async def chat(req: ChatRequest):
    if not req.message.strip():
        raise HTTPException(status_code=400, detail="Empty message")
    ctx      = build_context()
    response = generate_narrative(ctx, req.message)
    return {"response": response, "context_snapshot": ctx}

@app.get("/api/responses")
async def get_responses():
    """Return the automated AI responses triggered by Splunk AI anomaly detection."""
    return {"automated_responses": state["automated_responses"]}

@app.post("/api/clear/anomalies")
async def clear_anomalies():
    state["anomalies"] = []
    return {"ok": True}

@app.post("/api/clear/responses")
async def clear_responses():
    state["automated_responses"] = []
    return {"ok": True}

@app.get("/api/camera/history")
async def get_camera_history():
    """Return 24h camera time-series with ML anomaly flags for Chart.js rendering."""
    cam_out = {}
    for cam_name, cd in state.get("camera_history", {}).items():
        cam_out[cam_name] = {
            "chart":     cd.get("chart", {}),
            "stats":     cd.get("stats", {}),
            "forecast":  cd.get("forecast"),
            "anomalies": cd.get("anomalies", []),
        }
    return {
        "cameras": cam_out,
        "fetched": state.get("camera_history_fetched"),
    }


@app.get("/api/narrative")
async def get_narrative():
    ctx = build_context()
    narrative = generate_narrative(ctx)
    state["last_narrative"] = narrative
    return {"narrative": narrative}

# ── Chatbot HTML UI ──────────────────────────────────────────────────────────
HTML_UI = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ICP Splunk AI Observability Agent : Splunk Enterprise</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', sans-serif; background: #0d1117; color: #e6edf3; min-height: 100vh; }
  header { background: #161b22; border-bottom: 1px solid #30363d; padding: 12px 24px;
           display: flex; align-items: center; gap: 14px; }
  .logo { height: 40px; width: auto; display: block; }
  header h1 { font-size: 18px; font-weight: 600; }
  .header-partners { display: flex; align-items: center; gap: 10px; }
  .header-partners img { height: 22px; width: auto; filter: brightness(0) invert(1); opacity: 0.85; }
  header > span#poll-status { font-size: 12px; color: #8b949e; margin-left: auto; }
  .main { display: grid; grid-template-columns: 340px 1fr; gap: 0; height: calc(100vh - 61px); }
  .sidebar { background: #161b22; border-right: 1px solid #30363d; padding: 16px; overflow-y: auto; }
  .sidebar h2 { font-size: 12px; text-transform: uppercase; letter-spacing: 1px;
                color: #8b949e; margin-bottom: 12px; }
  .stat-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 16px; }
  .stat { background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 10px; }
  .stat .label { font-size: 10px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; }
  .stat .value { font-size: 22px; font-weight: 700; margin-top: 2px; }
  .stat .value.ok   { color: #3fb950; }
  .stat .value.warn { color: #d29922; }
  .stat .value.crit { color: #f85149; }
  .stat .value.info { color: #58a6ff; }
  .anomaly-list { margin-bottom: 16px; }
  .anomaly { background: #0d1117; border-left: 3px solid #d29922; border-radius: 4px;
             padding: 8px 10px; margin-bottom: 6px; font-size: 12px; }
  .anomaly.HIGH { border-color: #f85149; }
  .anomaly.INFO { border-color: #58a6ff; }
  .anomaly.INIT { border-color: #6e7681; }
  .anomaly .sev { font-size: 10px; font-weight: 700; margin-bottom: 2px; }
  .chat-area { display: flex; flex-direction: column; }
  .messages { flex: 1; overflow-y: auto; padding: 20px; display: flex; flex-direction: column; gap: 12px; }
  .msg { max-width: 80%; padding: 12px 16px; border-radius: 12px; font-size: 14px; line-height: 1.5; }
  .msg.user  { background: #1f6feb; align-self: flex-end; border-radius: 12px 12px 2px 12px; }
  .msg.agent { background: #161b22; border: 1px solid #30363d; align-self: flex-start;
               border-radius: 12px 12px 12px 2px; white-space: pre-wrap; }
  .msg.system { background: transparent; border: 1px solid #30363d; align-self: center;
                font-size: 12px; color: #8b949e; text-align: center;
                border-radius: 20px; padding: 6px 16px; }
  .input-row { padding: 16px 20px; border-top: 1px solid #30363d; background: #161b22;
               display: flex; gap: 10px; }
  .input-row input { flex: 1; background: #0d1117; border: 1px solid #30363d; border-radius: 8px;
                     padding: 10px 14px; color: #e6edf3; font-size: 14px; outline: none; }
  .input-row input:focus { border-color: #58a6ff; }
  .input-row button { background: #EF9F27; color: #000; border: none; border-radius: 8px;
                      padding: 10px 20px; font-weight: 600; cursor: pointer; font-size: 14px; }
  .input-row button:hover   { background: #f5b84a; }
  .input-row button:disabled { opacity: 0.5; cursor: not-allowed; }
  .suggestions { padding: 0 20px 12px; display: flex; gap: 8px; flex-wrap: wrap; }
  .sug { background: #161b22; border: 1px solid #30363d; border-radius: 16px;
         padding: 6px 12px; font-size: 12px; cursor: pointer; color: #8b949e; }
  .sug:hover { border-color: #58a6ff; color: #58a6ff; }
  .pulse { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
           background: #3fb950; margin-right: 6px; animation: pulse 2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }
  .stat.clickable { cursor: pointer; transition: border-color 0.15s, transform 0.1s; border: 1px solid transparent; }
  .stat.clickable:hover { border-color: #58a6ff; transform: scale(1.03); }
  #chart-modal { display:none; position:fixed; inset:0; background:rgba(0,0,0,0.72);
                 z-index:1000; align-items:center; justify-content:center; }
  .modal-card  { background:#161b22; border:1px solid #30363d; border-radius:12px;
                 padding:20px; width:min(700px,92vw); display:flex; flex-direction:column; gap:14px; }
  .modal-hdr   { display:flex; justify-content:space-between; align-items:center; }
  .modal-hdr span { font-size:14px; font-weight:600; color:#e6edf3; }
  .modal-hdr button { background:none; border:none; color:#8b949e; font-size:22px;
                      cursor:pointer; line-height:1; padding:0 4px; }
  .modal-hdr button:hover { color:#e6edf3; }
  .chart-wrap  { position:relative; height:260px; }
  .modal-foot  { font-size:11px; color:#6e7681; text-align:center; }
  .auto-resp { background:#0d1117; border-left:3px solid #a371f7; border-radius:4px;
               padding:8px 10px; margin-bottom:6px; font-size:11px; }
  .auto-resp .sev  { font-size:10px; font-weight:700; color:#a371f7; margin-bottom:3px; }
  .auto-resp .body { line-height:1.4; color:#e6edf3; margin-bottom:4px; }
  .auto-resp .badge { font-size:10px; }
  .auto-resp .badge.ok   { color:#3fb950; }
  .auto-resp .badge.fail { color:#f85149; }
  .cam-btn { background:#0d1117; border:1px solid #30363d; border-radius:12px;
             padding:4px 10px; font-size:11px; cursor:pointer; color:#8b949e;
             margin-bottom:4px; }
  .cam-btn:hover { border-color:#a371f7; color:#a371f7; }
  #cam-chart-modal { display:none; position:fixed; inset:0; background:rgba(0,0,0,0.78);
                     z-index:1001; align-items:center; justify-content:center; }
  .cam-ml-item { background:#0d1117; border-left:3px solid #a371f7; border-radius:4px;
                 padding:8px 10px; margin-bottom:6px; font-size:12px; }
  .cam-ml-item .sev { font-size:10px; font-weight:700; color:#a371f7; margin-bottom:3px; }
  .section-hdr { display:flex; align-items:center; justify-content:space-between; margin-bottom:12px; }
  .section-hdr h2 { margin-bottom:0; }
  .clear-btn { background:none; border:1px solid #30363d; border-radius:6px; color:#6e7681;
               font-size:10px; padding:2px 8px; cursor:pointer; line-height:1.4; }
  .clear-btn:hover { border-color:#f85149; color:#f85149; }
</style>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
</head>
<body>
<header>
  <img class="logo" src="/static/icp-logo" alt="ICP">
  <h1>ICP Splunk AI Observability Agent : Splunk Enterprise</h1>
  <div class="header-partners">
    <img src="https://upload.wikimedia.org/wikipedia/commons/7/72/Meraki_Logo_2016_transparent.svg" alt="Cisco Meraki">
    <img src="https://upload.wikimedia.org/wikipedia/commons/1/1d/Splunk_logo.svg" alt="Splunk">
  </div>
  <span id="poll-status"><span class="pulse"></span>Initialising...</span>
</header>
<div class="main">
  <div class="sidebar">
    <h2>Live Sensor Data</h2>
    <div class="stat-grid">
      <div class="stat clickable" onclick="openChart('temp','Temperature','°C','#00bceb')">
        <div class="label">Temperature</div>
        <div class="value" id="temp">--</div>
      </div>
      <div class="stat clickable" onclick="openChart('hum','Humidity','%','#58a6ff')">
        <div class="label">Humidity</div>
        <div class="value" id="hum">--</div>
      </div>
      <div class="stat clickable" onclick="openChart('uplink','Uplink','Kbps','#f97316')">
        <div class="label">Uplink Kbps</div>
        <div class="value info" id="uplink">--</div>
      </div>
      <div class="stat clickable" onclick="openChart('downlink','Downlink','Kbps','#3fb950')">
        <div class="label">Downlink Kbps</div>
        <div class="value info" id="downlink">--</div>
      </div>
      <div class="stat">
        <div class="label">WAP Clients</div>
        <div class="value info" id="wap">--</div>
      </div>
      <div class="stat clickable" onclick="openChart('poe','Total PoE','Wh','#d29922')">
        <div class="label">Total PoE Wh</div>
        <div class="value info" id="poe">--</div>
      </div>
    </div>

    <h2 style="color:#a371f7">&#x1F4CA; Camera AI (24h)</h2>
    <div id="camera-ml-list" class="anomaly-list">
      <div class="cam-ml-item"><div class="sev">CAMERA ML</div>Fetching 24h baseline...</div>
    </div>
    <h2>24h Timeline</h2>
    <div id="cam-timeline-btns" style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px"></div>

    <div class="section-hdr">
      <h2>Active Anomalies</h2>
      <button class="clear-btn" onclick="clearAnomalies()">[Clear]</button>
    </div>
    <div id="anomaly-list" class="anomaly-list">
      <div class="anomaly INFO"><div class="sev">STATUS</div>Polling...</div>
    </div>

    <div class="section-hdr">
      <h2 style="color:#a371f7">&#x26A1; Automated Responses</h2>
      <button class="clear-btn" onclick="clearResponses()">[Clear]</button>
    </div>
    <div id="auto-response-list" class="anomaly-list">
      <div class="auto-resp"><div class="sev">AUTO-RESPONSE ENGINE</div><div class="body">Monitoring — will fire on anomaly detection</div></div>
    </div>
  </div>

  <div class="chat-area">
    <div class="messages" id="messages">
      <div class="msg system">ICP Splunk AI Observability Agent ready — ask anything about the building</div>
    </div>
    <div class="suggestions">
      <div class="sug" onclick="ask('What is the current building status? Cite Splunk AI findings.')">Building status</div>
      <div class="sug" onclick="ask('What has Splunk anomalydetection found in the last 30 minutes?')">Splunk AI report</div>
      <div class="sug" onclick="ask('How many people are in the building?')">Occupancy</div>
      <div class="sug" onclick="ask('What is the network traffic like?')">Network traffic</div>
      <div class="sug" onclick="ask('What automated responses have been triggered by Splunk AI?')">Auto-responses</div>
      <div class="sug" onclick="ask('What does Splunk predict/LLP5 forecast for temperature?')">AI forecast</div>
      <div class="sug" onclick="ask('Which devices are consuming the most power?')">PoE usage</div>
      <div class="sug" onclick="ask('Summarise all Splunk AI insights in one paragraph')">AI summary</div>
    </div>
    <div class="input-row">
      <input type="text" id="user-input"
             placeholder="Ask about the ICP Building..."
             onkeydown="if(event.key==='Enter') sendMessage()">
      <button id="send-btn" onclick="sendMessage()">Ask</button>
    </div>
  </div>
</div>

<div id="chart-modal" onclick="if(event.target===this)closeChart()">
  <div class="modal-card">
    <div class="modal-hdr">
      <span id="modal-title"></span>
      <button onclick="closeChart()">&#x2715;</button>
    </div>
    <div class="chart-wrap"><canvas id="chart-canvas"></canvas></div>
    <div class="modal-foot">Up to 12 hours of history &nbsp;·&nbsp; updates every 2 min &nbsp;·&nbsp; click outside to close</div>
  </div>
</div>

<div id="cam-chart-modal" onclick="if(event.target===this)closeCamChart()">
  <div class="modal-card">
    <div class="modal-hdr">
      <span id="cam-modal-title"></span>
      <button onclick="closeCamChart()">&#x2715;</button>
    </div>
    <div style="font-size:11px;color:#8b949e;margin-bottom:8px;padding:0 2px" id="cam-modal-stats"></div>
    <div class="chart-wrap" style="height:290px"><canvas id="cam-chart-canvas"></canvas></div>
    <div class="modal-foot">24h occupancy &nbsp;·&nbsp; ◆ amber diamond = ML anomaly (hover for detail) &nbsp;·&nbsp; right axis = avg occupancy</div>
  </div>
</div>

<script>
const hist = { lastPoll:null, ready:false, times:[], temp:[], hum:[], uplink:[], downlink:[], poe:[] };
const HIST_MAX = 360;
const HIST_KEYS = ['times','temp','hum','uplink','downlink','poe'];

function histPush(d) {
  if (!d.last_poll || d.last_poll === hist.lastPoll) return;
  hist.lastPoll = d.last_poll;
  const t = d.last_poll.slice(11,16);
  // Avoid duplicating the last historical point if it matches the first live point
  if (hist.times.length && hist.times[hist.times.length-1] === t) return;
  hist.times.push(t);
  hist.temp.push(d.temperature ?? null);
  hist.hum.push(d.humidity ?? null);
  hist.uplink.push(d.uplink_kbps || 0);
  hist.downlink.push(d.downlink_kbps || 0);
  hist.poe.push(d.total_poe_wh || 0);
  if (hist.times.length > HIST_MAX)
    HIST_KEYS.forEach(k => hist[k].shift());
}

async function loadHistory() {
  try {
    const r = await fetch('/api/history');
    if (!r.ok) { hist.ready = true; return; }
    const d = await r.json();
    // Merge all metrics by timestamp
    const byTime = {};
    ['temp','hum','uplink','downlink','poe'].forEach(metric => {
      (d[metric] || []).forEach(pt => {
        if (!byTime[pt.t]) byTime[pt.t] = {};
        byTime[pt.t][metric] = pt.v;
      });
    });
    const times = Object.keys(byTime).sort();
    times.slice(-HIST_MAX).forEach(t => {
      const row = byTime[t];
      hist.times.push(t);
      hist.temp.push(row.temp     ?? null);
      hist.hum.push(row.hum      ?? null);
      hist.uplink.push(row.uplink   ?? null);
      hist.downlink.push(row.downlink ?? null);
      hist.poe.push(row.poe      ?? null);
    });
    hist.ready = true;
    // Refresh chart if one is already open
    if (currentChart) openChart(currentChart.metric, currentChart.label, currentChart.unit, currentChart.color);
  } catch(e) {
    console.warn('History load failed:', e);
    hist.ready = true;
  }
}

let chartObj = null;
let currentChart = null;
function openChart(metric, label, unit, color) {
  currentChart = { metric, label, unit, color };
  document.getElementById('modal-title').textContent = label + ' — last 12 h';
  document.getElementById('chart-modal').style.display = 'flex';
  const ctx = document.getElementById('chart-canvas').getContext('2d');
  if (chartObj) { chartObj.destroy(); chartObj = null; }
  if (!hist.ready) {
    // History still loading — show placeholder
    chartObj = new Chart(ctx, {
      type: 'line',
      data: { labels: ['Loading historical data from Splunk…'], datasets: [{ data: [null] }] },
      options: { animation: false, responsive: true, maintainAspectRatio: false,
        plugins: { legend:{ display:false } } }
    });
    return;
  }
  const pts = hist[metric] || [];
  chartObj = new Chart(ctx, {
    type: 'line',
    data: {
      labels: hist.times,
      datasets: [{ label, data: pts, borderColor: color,
        backgroundColor: color + '22', fill: true,
        tension: 0.35, borderWidth: 2,
        pointRadius: pts.length > 80 ? 0 : 3 }]
    },
    options: {
      animation: false, responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: c => (c.parsed.y !== null ? c.parsed.y.toFixed(1) : 'N/A') + ' ' + unit } }
      },
      scales: {
        x: { grid:{ color:'#21262d' }, ticks:{ color:'#8b949e', maxTicksLimit:12, maxRotation:0 } },
        y: { grid:{ color:'#21262d' }, ticks:{ color:'#8b949e' },
             title:{ display:true, text:unit, color:'#8b949e', font:{ size:11 } } }
      }
    }
  });
}
function closeChart() {
  document.getElementById('chart-modal').style.display = 'none';
  if (chartObj) { chartObj.destroy(); chartObj = null; }
  currentChart = null;
}

async function updateStatus() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();

    const temp = d.temperature;
    const hum  = d.humidity;

    const te = document.getElementById('temp');
    te.textContent = temp !== null ? temp.toFixed(1)+'°C' : '--';
    te.className = 'value ' + (temp===null?'': temp>28?'crit': temp>24?'warn':'ok');

    const he = document.getElementById('hum');
    he.textContent = hum !== null ? hum.toFixed(0)+'%' : '--';
    he.className = 'value ' + (hum===null?'': hum>70?'crit': hum>60?'warn':'ok');

    document.getElementById('uplink').textContent   = d.uplink_kbps   ? d.uplink_kbps.toFixed(0)   : '--';
    document.getElementById('downlink').textContent = d.downlink_kbps ? d.downlink_kbps.toFixed(0) : '--';
    document.getElementById('wap').textContent      = d.wap_clients ?? '--';
    document.getElementById('poe').textContent      = d.total_poe_wh  ? d.total_poe_wh.toFixed(0)  : '--';

    histPush(d);

    // Anomalies
    const al = document.getElementById('anomaly-list');
    if (d.anomalies && d.anomalies.length > 0) {
      al.innerHTML = d.anomalies.map(a =>
        `<div class="anomaly ${a.severity}">
          <div class="sev">${a.severity} — ${a.signal}</div>
          ${a.message}
        </div>`).join('');
    } else {
      al.innerHTML = '<div class="anomaly INFO"><div class="sev">STATUS</div>All systems normal</div>';
    }

    document.getElementById('poll-status').innerHTML =
      `<span class="pulse"></span>Poll #${d.poll_count} — ${d.last_poll || 'pending'}`;

  } catch(e) { console.error('Status update failed:', e); }
}

async function sendMessage() {
  const input = document.getElementById('user-input');
  const btn   = document.getElementById('send-btn');
  const msg   = input.value.trim();
  if (!msg) return;
  addMessage(msg, 'user');
  input.value  = '';
  btn.disabled = true;
  addMessage('Analysing ICP Building data...', 'agent', 'thinking');
  try {
    const r = await fetch('/api/chat', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({message: msg})
    });
    const d = await r.json();
    removeThinking();
    addMessage(d.response, 'agent');
  } catch(e) {
    removeThinking();
    addMessage('Connection error. Is the agent running?', 'agent');
  }
  btn.disabled = false;
}

function ask(q) {
  document.getElementById('user-input').value = q;
  sendMessage();
}

function addMessage(text, type, id='') {
  const msgs = document.getElementById('messages');
  const div  = document.createElement('div');
  div.className = `msg ${type}`;
  if (id) div.id = id;
  div.textContent = text;
  msgs.appendChild(div);
  msgs.scrollTop = msgs.scrollHeight;
}

function removeThinking() {
  const el = document.getElementById('thinking');
  if (el) el.remove();
}

async function fetchResponses() {
  try {
    const r = await fetch('/api/responses');
    if (!r.ok) return;
    const d = await r.json();
    const list = document.getElementById('auto-response-list');
    const responses = d.automated_responses || [];
    if (responses.length === 0) {
      list.innerHTML = '<div class="auto-resp"><div class="sev">AUTO-RESPONSE ENGINE</div><div class="body">No Splunk AI anomalies detected — all signals within statistical norms</div></div>';
      return;
    }
    list.innerHTML = responses.map(resp => {
      const body  = resp.summary.length > 240 ? resp.summary.substring(0, 240) + '...' : resp.summary;
      const src   = resp.source ? `<div style="font-size:10px;color:#8b949e;margin-bottom:3px">${resp.source}</div>` : '';
      const badge = resp.alert_ok
        ? `<span class="badge ok">&#x2713; Splunk Alert: ${resp.alert_name}</span>`
        : `<span class="badge fail">&#x26A0; Alert creation pending</span>`;
      return `<div class="auto-resp">
        <div class="sev">&#x26A1; ${resp.ts} &mdash; ${resp.signal.toUpperCase()} &mdash; ${resp.severity}</div>
        ${src}
        <div class="body">${body}</div>
        ${badge}
      </div>`;
    }).join('');
  } catch(e) { console.error('Responses fetch failed:', e); }
}

// ── Camera ML: 24h timeline + anomaly panel ──────────────────────────────────
let camHistory = {};
let camChartObj = null;

async function loadCameraHistory() {
  try {
    const r = await fetch('/api/camera/history');
    if (!r.ok) return;
    const d = await r.json();
    camHistory = d.cameras || {};
    renderCameraML(d);
    renderCamButtons();
  } catch(e) { console.warn('Camera history fetch failed:', e); }
}

function renderCamButtons() {
  const el = document.getElementById('cam-timeline-btns');
  if (!el) return;
  el.innerHTML = Object.keys(camHistory).map(name => {
    const safe = name.replace(/'/g, "\\'");
    return `<button class="cam-btn" onclick="openCamChart('${safe}')">&#x1F4F9; ${name}</button>`;
  }).join('');
}

function renderCameraML(d) {
  const el = document.getElementById('camera-ml-list');
  if (!el) return;
  const cams = d.cameras || {};
  const lines = [];
  for (const [name, cd] of Object.entries(cams)) {
    const stats  = cd.stats    || {};
    const fc     = cd.forecast || null;
    const anoms  = cd.anomalies || [];
    const high   = anoms.filter(a => a.severity === 'HIGH' || a.severity === 'MEDIUM');
    const peakOcc = (stats.peak_occupancy || 0).toFixed(1);
    const peakT   = stats.peak_time || 'N/A';
    const totEnt  = stats.total_entrances || 0;
    let body = `Peak <b>${peakOcc} ppl</b> @ ${peakT} &nbsp;·&nbsp; ${totEnt} entrances`;
    if (fc) body += ` &nbsp;·&nbsp; ${fc.trend} trend (→${fc.next_occ} ppl)`;
    if (high.length) body += `<br><span style="color:#f85149">&#x26A0; ${high.length} ML anomal${high.length===1?'y':'ies'}: ${high[0].message.substring(0,60)}…</span>`;
    lines.push(`<div class="cam-ml-item">
      <div class="sev">&#x1F4CA; ${name.toUpperCase()}</div>${body}</div>`);
  }
  const fetched = d.fetched ? `<div style="font-size:10px;color:#6e7681;margin-top:4px">Updated: ${d.fetched}</div>` : '';
  el.innerHTML = (lines.join('') || '<div class="cam-ml-item"><div class="sev">CAMERA ML</div>No data yet</div>') + fetched;
}

function openCamChart(name) {
  const cd = camHistory[name];
  if (!cd) { return; }
  document.getElementById('cam-modal-title').textContent = name + ' — 24h Occupancy Timeline';
  const stats = cd.stats || {};
  const fc    = cd.forecast;
  const an    = (cd.anomalies || []).length;
  let info = `Peak: ${(stats.peak_occupancy||0).toFixed(1)} ppl @ ${stats.peak_time||'N/A'}  ·  `
           + `Total entrances: ${stats.total_entrances||0}  ·  `
           + `ML anomalies: ${an}`;
  if (fc) info += `  ·  Trend: ${fc.trend} → next ~${fc.next_occ} ppl`;
  document.getElementById('cam-modal-stats').textContent = info;
  document.getElementById('cam-chart-modal').style.display = 'flex';

  const ctx = document.getElementById('cam-chart-canvas').getContext('2d');
  if (camChartObj) { camChartObj.destroy(); camChartObj = null; }

  const chart   = cd.chart || {};
  const labels  = chart.labels          || [];
  const occ     = chart.occupancy       || [];
  const ent     = chart.entrances       || [];
  const markers = chart.anomaly_markers || [];

  // Build time -> anomaly lookup for tooltip detail
  const anomByTime = {};
  markers.forEach(m => { anomByTime[m.time] = m; });

  // Amber diamond dataset — only at anomaly positions, null elsewhere
  const markerY  = labels.map(l => anomByTime[l] ? anomByTime[l].occ : null);
  const markerBg = labels.map(l => {
    if (!anomByTime[l]) return 'transparent';
    return anomByTime[l].severity === 'HIGH'
      ? 'rgba(255,130,20,0.95)'
      : 'rgba(255,196,0,0.95)';
  });

  camChartObj = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: 'Entrances',
          data: ent,
          backgroundColor: 'rgba(88,166,255,0.45)',
          borderWidth: 0,
          yAxisID: 'y',
          order: 3,
        },
        {
          label: 'Avg Occupancy',
          data: occ,
          type: 'line',
          borderColor: '#00bceb',
          backgroundColor: 'rgba(0,188,235,0.08)',
          fill: true,
          tension: 0.3,
          borderWidth: 2,
          pointRadius: labels.length > 120 ? 0 : 2,
          yAxisID: 'y1',
          order: 2,
        },
        {
          label: 'Anomaly',
          type: 'line',
          data: markerY,
          pointBackgroundColor: markerBg,
          pointBorderColor: '#fff',
          pointBorderWidth: 2,
          pointStyle: 'rectRot',
          pointRadius: labels.map(l => anomByTime[l] ? 14 : 0),
          pointHoverRadius: labels.map(l => anomByTime[l] ? 22 : 0),
          pointHitRadius: labels.map(l => anomByTime[l] ? 40 : 0),
          showLine: false,
          spanGaps: false,
          yAxisID: 'y1',
          order: 1,
        }
      ]
    },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          display: true,
          labels: {
            color: '#8b949e',
            font: { size: 11 },
            generateLabels: chart => {
              const items = Chart.defaults.plugins.legend.labels.generateLabels(chart);
              items.forEach(item => {
                if (item.text === 'Anomaly') {
                  item.fillStyle   = 'rgba(255,180,0,0.95)';
                  item.strokeStyle = '#fff';
                  item.pointStyle  = 'rectRot';
                  item.text        = '◆ Anomaly marker';
                }
              });
              return items;
            }
          }
        },
        tooltip: {
          filter: item => item.datasetIndex !== 2,
          callbacks: {
            label: c => c.dataset.label + ': ' + (c.parsed.y !== null ? c.parsed.y.toFixed(1) : 'N/A'),
            afterBody: items => {
              const lbl = items[0]?.label;
              if (!lbl) return [];
              const idx = labels.indexOf(lbl);
              if (idx < 0) return [];

              // Proximity search — find nearest anomaly within ±30 labels (~30 min for 1-min data)
              const PROX = 30;
              let best = null, bestDist = Infinity;
              markers.forEach(m => {
                const mi = labels.indexOf(m.time);
                if (mi < 0) return;
                const dist = Math.abs(idx - mi);
                if (dist <= PROX && dist < bestDist) { best = m; bestDist = dist; }
              });
              if (!best) return [];

              const icon = best.severity === 'HIGH' ? '⚠️' : '⚡';
              const type = best.type.replace(/_/g, ' ');
              const msg  = best.message.length > 90 ? best.message.slice(0, 90) + '…' : best.message;
              const dist = bestDist > 0 ? ` (nearest ±${bestDist} min)` : '';
              return ['', `${icon} ${best.severity} · ${type}${dist}`, msg];
            }
          }
        }
      },
      scales: {
        x: { grid:{ color:'#21262d' },
             ticks:{ color:'#8b949e', maxTicksLimit:12, maxRotation:0 } },
        y: { type:'linear', position:'left', grid:{ color:'#21262d' },
             ticks:{ color:'#58a6ff' },
             title:{ display:true, text:'Entrances', color:'#58a6ff', font:{size:10} } },
        y1:{ type:'linear', position:'right', grid:{ drawOnChartArea:false },
             ticks:{ color:'#00bceb' },
             title:{ display:true, text:'Avg Occ', color:'#00bceb', font:{size:10} } }
      }
    }
  });
}

function closeCamChart() {
  document.getElementById('cam-chart-modal').style.display = 'none';
  if (camChartObj) { camChartObj.destroy(); camChartObj = null; }
}

loadHistory();
loadCameraHistory();
updateStatus();
fetchResponses();
setInterval(updateStatus, 15000);
setInterval(fetchResponses, 30000);
setInterval(loadCameraHistory, 300000);   // refresh camera ML every 5 min

async function clearAnomalies() {
  await fetch('/api/clear/anomalies', {method:'POST'});
  document.getElementById('anomaly-list').innerHTML =
    '<div class="anomaly INFO"><div class="sev">STATUS</div>Cleared</div>';
}

async function clearResponses() {
  await fetch('/api/clear/responses', {method:'POST'});
  document.getElementById('auto-response-list').innerHTML =
    '<div class="auto-resp"><div class="sev">AUTO-RESPONSE ENGINE</div><div class="body">Cleared</div></div>';
}
</script>
</body>
</html>"""

# ── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    or_key = os.getenv("OPENROUTER_API_KEY", "")
    model  = os.getenv("MODEL_ANALYST", "openai/gpt-oss-120b:free")
    print("=" * 60)
    print("  ICP Splunk AI Observability Agent : Splunk Enterprise")
    print("  Cisco-Curtin ICP Lab, Curtin University")
    print("=" * 60)
    print(f"  Splunk HEC    : {SPLUNK_HEC_URL}")
    print(f"  Poll interval : {POLL_INTERVAL}s")
    print(f"  OpenRouter    : {'✓ configured — ' + model if or_key else '✗ not set (OPENROUTER_API_KEY)'}")
    print(f"  Cameras       : {len(CAMERAS)} × MV12W (1h lookback, non-zero bucket)")
    print(f"  Web UI        : http://0.0.0.0:5000")
    print("=" * 60)

    poller = threading.Thread(target=polling_loop, daemon=True)
    poller.start()

    cam_hist = threading.Thread(target=camera_history_loop, daemon=True)
    cam_hist.start()

    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="warning")
