#!/usr/bin/env python3
"""
Quick test of the Splunk AI integration:
  1. Verifies management API connectivity (auth + basic search)
  2. Runs anomalydetection on temperature data
  3. Runs predict/LLP5 on temperature data (MLTK)
  4. Prints the full get_splunk_ai_insights() result
"""
import os, json, sys
from dotenv import load_dotenv
load_dotenv()

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import requests, time

SPLUNK_MGMT_URL = os.getenv("SPLUNK_MGMT_URL", "https://127.0.0.1:8089")
SPLUNK_USERNAME = os.getenv("SPLUNK_USERNAME", "admin")
SPLUNK_PASSWORD = os.getenv("SPLUNK_PASSWORD", "")

print("=" * 60)
print("  Splunk AI Integration Test")
print("=" * 60)
print(f"  URL  : {SPLUNK_MGMT_URL}")
print(f"  User : {SPLUNK_USERNAME}")
print(f"  Pass : {'set (' + str(len(SPLUNK_PASSWORD)) + ' chars)' if SPLUNK_PASSWORD else 'NOT SET'}")
print()

if not SPLUNK_PASSWORD:
    print("ERROR: SPLUNK_PASSWORD not set in .env — aborting")
    sys.exit(1)

auth = (SPLUNK_USERNAME, SPLUNK_PASSWORD)

# ── Step 1: Auth / connectivity ──────────────────────────────────────────────
print("[1] Testing Splunk management API connectivity...")
try:
    r = requests.get(
        f"{SPLUNK_MGMT_URL}/services/server/info",
        auth=auth, params={"output_mode": "json"},
        verify=False, timeout=10,
    )
    if r.ok:
        info = r.json()["entry"][0]["content"]
        print(f"    OK — Splunk {info.get('version')} | build {info.get('build')}")
        print(f"    Product: {info.get('productName')}")
    else:
        print(f"    FAIL — HTTP {r.status_code}: {r.text[:200]}")
        sys.exit(1)
except Exception as e:
    print(f"    ERROR: {e}")
    sys.exit(1)

# ── Step 2: Check for data in index ─────────────────────────────────────────
print("\n[2] Checking for icp:sensor_latest events in Splunk...")

def run_search(spl, label, max_wait=30):
    try:
        r = requests.post(
            f"{SPLUNK_MGMT_URL}/services/search/jobs",
            auth=auth,
            data={"search": spl, "output_mode": "json"},
            verify=False, timeout=15,
        )
        r.raise_for_status()
        sid = r.json()["sid"]

        dispatch = "UNKNOWN"
        deadline = time.time() + max_wait
        while time.time() < deadline:
            s = requests.get(
                f"{SPLUNK_MGMT_URL}/services/search/jobs/{sid}",
                auth=auth, params={"output_mode": "json"},
                verify=False, timeout=10,
            ).json()
            dispatch = s["entry"][0]["content"]["dispatchState"]
            done_pct = s["entry"][0]["content"].get("doneProgress", 0)
            if dispatch in ("DONE", "FAILED"):
                break
            print(f"    ... {dispatch} ({done_pct*100:.0f}%)", end="\r")
            time.sleep(1)
        print()

        if dispatch != "DONE":
            print(f"    FAIL — job ended with state: {dispatch}")
            return None

        res = requests.get(
            f"{SPLUNK_MGMT_URL}/services/search/jobs/{sid}/results",
            auth=auth, params={"output_mode": "json", "count": 10},
            verify=False, timeout=10,
        ).json()
        rows = res.get("results", [])
        print(f"    {label}: {len(rows)} rows returned")
        return rows

    except Exception as e:
        print(f"    ERROR in '{label}': {e}")
        return None

# Data availability check
rows = run_search(
    'search index=main sourcetype="icp:sensor_latest" earliest=-30m | stats count by metric',
    "sensor data check"
)
if rows:
    for row in rows:
        print(f"      metric={row.get('metric', '?')}  count={row.get('count', '?')}")

# ── Step 3: anomalydetection on temperature ───────────────────────────────────
print("\n[3] Running anomalydetection on temperature (last 30 min)...")
rows = run_search(
    'search index=main sourcetype="icp:sensor_latest" metric=temperature earliest=-30m '
    '| timechart avg(value) as temp span=5m '
    '| anomalydetection temp action=annotate',
    "anomalydetection/temp"
)
if rows is not None:
    outliers = [r for r in rows if str(r.get("isOutlier", "0")) == "1"]
    print(f"    Total buckets: {len(rows)}  |  Outliers flagged: {len(outliers)}")
    if outliers:
        for o in outliers:
            print(f"      Outlier at {o.get('_time','')} — temp={o.get('temp','?')}")

# ── Step 4: anomalydetection on humidity ─────────────────────────────────────
print("\n[4] Running anomalydetection on humidity (last 30 min)...")
rows = run_search(
    'search index=main sourcetype="icp:sensor_latest" metric=humidity earliest=-30m '
    '| timechart avg(value) as hum span=5m '
    '| anomalydetection hum action=annotate',
    "anomalydetection/humidity"
)
if rows is not None:
    outliers = [r for r in rows if str(r.get("isOutlier", "0")) == "1"]
    print(f"    Total buckets: {len(rows)}  |  Outliers flagged: {len(outliers)}")

# ── Step 5: anomalydetection on occupancy ─────────────────────────────────────
print("\n[5] Running anomalydetection on camera occupancy (last 1 h)...")
rows = run_search(
    'search index=main sourcetype="icp:camera_analytics" earliest=-1h '
    '| timechart avg(averageCount) as occupancy span=5m '
    '| anomalydetection occupancy action=annotate',
    "anomalydetection/occupancy"
)
if rows is not None:
    outliers = [r for r in rows if str(r.get("isOutlier", "0")) == "1"]
    print(f"    Total buckets: {len(rows)}  |  Outliers flagged: {len(outliers)}")

# ── Step 6: MLTK predict/LLP5 (may fail if MLTK not installed) ───────────────
print("\n[6] Running predict/LLP5 temperature forecast (requires MLTK)...")
rows = run_search(
    'search index=main sourcetype="icp:sensor_latest" metric=temperature earliest=-1h '
    '| timechart avg(value) as temp span=5m '
    '| predict temp future_timespan=6 algorithm=LLP5 holdback=0 '
    '  lower95=lower95 upper95=upper95',
    "predict/LLP5"
)
if rows is not None:
    future = [r for r in rows if r.get("prediction(temp)") and not r.get("temp")]
    print(f"    Total rows: {len(rows)}  |  Future forecast points: {len(future)}")
    if future:
        last = future[-1]
        print(f"    Last forecast: {last.get('_time','')} "
              f"→ {last.get('prediction(temp)','?')}°C "
              f"(CI {last.get('lower95(prediction(temp))','?')}–{last.get('upper95(prediction(temp))','?')}°C)")
    elif not rows:
        print("    NOTE: No rows returned — MLTK may not be installed (predict command unavailable)")

print("\n" + "=" * 60)
print("  Test complete")
print("=" * 60)
