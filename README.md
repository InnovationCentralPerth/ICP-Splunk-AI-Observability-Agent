# ICP Splunk AI Observability Agent

**Closed-loop AI observability for smart buildings using Cisco Meraki hardware + Splunk Enterprise MLTK AI + in-process Camera AI + multi-model LLMs.**

Built by [Innovation Central Perth](https://icp.curtin.edu.au) at Curtin University for the Splunk AI Observability Hackathon.

![Architecture Diagram](architecture.svg)

---

## What This Does

Real Cisco Meraki hardware (cameras, sensors, switches) streams data into Splunk Enterprise every 2 minutes. Three **parallel anomaly detection layers** operate simultaneously:

1. **Splunk MLTK AI** — `anomalydetection` + `predict/LLP5` on ingested time-series data
2. **In-process Camera AI** — `camera_ml.py` fetches 24 hours of per-minute occupancy data per camera, runs crowd-surge detection, and overlays amber anomaly markers directly on Chart.js timeline charts
3. **Cross-signal correlation** — equipment thermal events, overcrowding, and after-hours presence detected by correlating camera occupancy with sensor readings

When anomalies are detected, the **Automated AI Response Engine** generates LLM-written incident reports, creates Splunk saved alerts via REST API, and logs `icp:automated_response` events back to Splunk — closing the AI loop.

```
Cisco Meraki Hardware
  MV Cameras (people count)  ─┐
  MT Sensors (temp/humidity)  ├─ Meraki API ─► ICP Agent ─► Splunk HEC
  MS Switch (traffic/PoE)    ─┘       │
                                       │◄── Splunk AI results (anomalydetection · predict/LLP5)
                                       │
                                 camera_ml.py (24h occupancy ML, every 10 min)
                                       │
                                 [Anomaly layers]
                                       │
                          ┌────────────┴──────────────────────┐
                          │  LLM Incident Report              │
                          │  icp:automated_response → Splunk  │
                          │  Splunk Saved Alert (REST API)    │
                          │  Operations Dashboard + Chat UI   │
                          └───────────────────────────────────┘
```

---

## Key Features

- **Camera AI (24h)** — `camera_ml.py` fetches 24h of per-minute occupancy from each Meraki MV camera every 10 minutes; Chart.js dual-axis timeline shows occupancy + entrances; amber diamond markers flag ML anomalies with mouseover detail
- **Multi-layer anomaly detection**:
  - **CROWD_SURGE** — entrance spike ≥3× hourly baseline (in-process, no Splunk dependency)
  - **Equipment thermal** — temperature/humidity spike with zero camera occupancy (cross-signal: possible overheating)
  - **Overcrowding** — >5 persons → MEDIUM alert
  - **After-hours presence** — any persons detected 6 PM–7 AM → HIGH alert
- **Splunk AI (MLTK)** — `anomalydetection` for statistical outlier detection across temperature, humidity, and camera occupancy; `predict/LLP5` for temperature forecasting
- **Automated Response Engine** — on anomaly: LLM generates incident report, creates Splunk saved alert via REST API, pushes `icp:automated_response` back to Splunk
- **Bidirectional Splunk loop** — data INTO Splunk via HEC, AI results OUT to drive LLM context and automated responses back IN — Splunk is the AI brain, not just a log sink
- **[Clear] controls** — one-click clear buttons on Active Anomalies and Automated Responses panels
- **In-application LLM model switching** — GPT-4o mini (analyst), Llama 3.3 70B (chat composer), Gemini Flash 2.0 (fallback) — demonstrates multi-model AI routing
- **PTA GPS Tracker** — phone app sends live GPS + incident flags to FastAPI → Splunk HEC; Google Maps page at `/pta` shows live bus position, yellow route polyline (all raw points), cyan breadcrumb markers (UTC+8 timestamps, ≥1 min/≥25 m filter), and orange incident triangle markers with auto-generated INC-MMDD-HHmmSS IDs; RESET button clears all Splunk history via `| delete`
- **Natural language chat** — every LLM response explicitly cites Splunk AI findings
- **Historical trend charts** — sensor tiles open 12-hour Chart.js graphs from Splunk `timechart` queries; camera AI tiles open 24h occupancy timelines with anomaly markers
- **FastAPI web UI** — live sensor tiles, camera AI panel, automated responses panel, NL chat interface

---

## Hardware Requirements

| Device | Purpose | Meraki API Used |
|---|---|---|
| Meraki MV cameras (MV12W etc.) | People counting via MV Sense zone analytics | `/devices/{serial}/camera/analytics/zones/{id}/history` |
| Meraki MT sensors (MT10 etc.) | Temperature, humidity, door state | `/organizations/{orgId}/sensor/readings/latest` |
| Meraki MS switch (MS355 etc.) | Per-port traffic (Kbps), PoE (Wh), WAP client counts | `/devices/{serial}/switch/ports/statuses` |
| Ubuntu server (20.04+) | Runs Splunk Enterprise + observability agent | — |

> All hardware components are independently optional — comment out any polling function in `icp_observability_agent.py`.

---

## Software Requirements

| Component | Version | Notes |
|---|---|---|
| Splunk Enterprise | 10.x | Developer License: 10 GB/day free at splunk.com |
| Splunk Machine Learning Toolkit | Latest | Required for `anomalydetection` and `predict/LLP5` |
| Python for Scientific Computing | Latest | Splunkbase dependency for MLTK |
| Python | 3.10+ | Agent runtime |
| OpenRouter account | — | Free tier available; provides GPT-4o mini, Llama 3.3, Gemini |

Install Python dependencies:
```bash
pip install requests fastapi uvicorn httpx python-dotenv
```

---

## Setup

### 1. Clone and Configure

```bash
git clone https://github.com/InnovationCentralPerth/ICP-Splunk-AI-Observability-Agent.git
cd ICP-Splunk-AI-Observability-Agent
cp icp_agent.env .env
```

Edit `.env` with your credentials:

```env
# Cisco Meraki
MERAKI_API_KEY=your_meraki_api_key_here
MERAKI_ORG_ID=your_org_id_here          # Meraki Dashboard URL → /o/{ORG_ID}/...
MERAKI_NETWORK_ID=your_network_id_here  # GET /organizations/{orgId}/networks

# Splunk HEC (HTTP Event Collector)
SPLUNK_HEC_URL=https://127.0.0.1:8088/services/collector
SPLUNK_HEC_TOKEN=your_splunk_hec_token_here

# Splunk Management API (enables Splunk AI queries)
SPLUNK_MGMT_URL=https://127.0.0.1:8089
SPLUNK_USERNAME=admin
SPLUNK_PASSWORD=your_splunk_admin_password_here

# OpenRouter
OPENROUTER_API_KEY=your_openrouter_api_key_here
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
OPENROUTER_REFERER=https://your-org.example.com

# Models (demonstrates in-app switching)
MODEL_ANALYST=openai/gpt-4o-mini
MODEL_COMPOSER=meta-llama/llama-3.3-70b-instruct
MODEL_FALLBACK=google/gemini-2.0-flash-001

POLL_INTERVAL=120
```

### 2. Configure Your Devices

Edit the device registry in `icp_observability_agent.py`:

```python
CAMERAS = [
    {"serial": "YOUR-CAMERA-SERIAL-1", "name": "ICP Workshop",
     "zones": [{"id": "0", "label": "Full Frame"}]},
    {"serial": "YOUR-CAMERA-SERIAL-2", "name": "ICP Demo Area",
     "zones": [{"id": "0", "label": "Full Frame"}]},
    {"serial": "YOUR-CAMERA-SERIAL-3", "name": "ICP Pantry Area",
     "zones": [{"id": "0", "label": "Full Frame"}]},
]

SENSORS = [
    {"serial": "YOUR-SENSOR-SERIAL", "name": "Main Sensor",
     "metrics": ["temperature", "humidity", "battery"]},
]

SWITCH = {
    "serial": "YOUR-SWITCH-SERIAL",
    "name":   "YOUR-SW01",
    "model":  "MS355-48X2",
    "ports": {"48": "Uplink"},
}
```

### 3. Enable Splunk HEC

In Splunk Web: **Settings → Data Inputs → HTTP Event Collector → New Token**
- Source type: leave blank (agent sets per event)
- Index: `main`

### 4. Enable Splunk MLTK

Install from Splunkbase:
1. [Splunk Machine Learning Toolkit](https://splunkbase.splunk.com/app/2890)
2. [Python for Scientific Computing](https://splunkbase.splunk.com/app/2881)

Verify MLTK works:
```bash
python3 test_splunk_ai.py
```

### 5. Run the Agent

```bash
python3 icp_observability_agent.py
```

Open **http://localhost:5000** in your browser.

#### Optional: Run as a systemd service

```bash
sudo cp icp-agent.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now icp-agent
sudo journalctl -u icp-agent -f
```

---

## Anomaly Detection Reference

| Layer | Type | Condition | Severity |
|---|---|---|---|
| Camera AI | `CROWD_SURGE` | Entrances ≥3× hourly baseline AND ≥3 entrances | HIGH |
| Cross-signal | Equipment thermal | Temp spike ≥2°C OR >26°C absolute, humidity >65%, zero occupancy | MEDIUM |
| Threshold | Overcrowding | >5 persons detected simultaneously | MEDIUM |
| Threshold | After-hours presence | Any persons detected 6 PM–7 AM | HIGH |
| Splunk MLTK | Statistical outlier | `anomalydetection` flags on temp/humidity/occupancy streams | HIGH |

---

## Splunk Sourcetypes

| Sourcetype | Description |
|---|---|
| `icp:sensor_latest` | MT sensor readings: temperature, humidity, door, battery |
| `icp:switch_port_status` | MS switch per-port: traffic Kbps, PoE Wh, client count |
| `icp:camera_analytics` | MV camera zone analytics: average count, entrances |
| `icp:camera_ml_anomaly` | Camera AI anomalies: crowd surge, thermal correlations |
| `icp:anomaly` | All threshold and cross-signal anomaly events |
| `icp:automated_response` | AI-generated incident reports + Splunk alert creation events |
| `pta:gps_location` | PTA GPS tracker: latitude, longitude, accuracy, speed, heading, vehicleId; incident-flagged pings include `incident=true` and `incidentNote` fields |

---

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Web UI (live dashboard + chat) |
| `GET /api/status` | Current sensor state, anomalies, automated responses |
| `POST /api/chat` | Natural language query — LLM response grounded in live data |
| `GET /api/history` | 12-hour sensor time-series from Splunk `timechart` queries |
| `GET /api/responses` | Automated AI responses triggered by anomaly detection |
| `GET /api/camera/history` | 24h per-camera occupancy + entrances + ML anomaly markers |
| `POST /api/clear/anomalies` | Clear the active anomalies list |
| `POST /api/clear/responses` | Clear the automated responses list |
| `GET /pta` | PTA GPS Tracker map — live position, route polyline, breadcrumbs, incidents |
| `POST /api/pta/telemetry` | Receive phone GPS payload (+ optional incident flag) → forward to Splunk HEC |
| `GET /api/pta/gps` | Latest GPS fix for any vehicle (all-time search) |
| `GET /api/pta/gps/history` | Filtered breadcrumbs (≥1 min, ≥25 m) + all incident markers |
| `GET /api/pta/gps/route` | All raw GPS points for route polyline (unfiltered) |
| `POST /api/pta/reset` | Delete all `pta:gps_location` events from Splunk via `\| delete` |

---

## Camera AI Module (`camera_ml.py`)

Pure stdlib — no numpy/scipy dependency.

```python
from camera_ml import OccupancyAnalyser

analyser = OccupancyAnalyser()
anomalies = analyser.ingest("ICP Workshop", meraki_zone_history_buckets)
chart_data = analyser.export_for_chart("ICP Workshop")
# chart_data keys: labels, occupancy, entrances, anomaly_markers
# anomaly_markers: [{time, type, severity, message, value, occ}]

stats    = analyser.get_stats("ICP Workshop")
forecast = analyser.forecast_next("ICP Workshop")   # OLS linear regression
```

The 24h timeline chart renders as:
- **Blue bars** — entrances per minute (left axis)
- **Green line** — average occupancy (right axis)
- **Amber diamond markers** — ML anomaly positions; hover for type, severity, and message (±30 minute proximity search for dense time-series)

---

## Architecture

See [`architecture.svg`](architecture.svg) for the full data flow diagram.

**The AI loop:**

1. **Collect** — Meraki REST API → agent polls every 2 minutes; camera 24h history every 10 minutes
2. **Ingest** — Structured JSON events pushed to Splunk via HEC
3. **Detect** — Three parallel layers: Splunk MLTK (statistical), Camera AI (occupancy ML), cross-signal correlation (thermal + camera)
4. **Interpret** — LLMs receive anomaly context, generate responses citing live findings
5. **Act** — Automated Response Engine: LLM writes incident report → Splunk saved alert via REST API → `icp:automated_response` logged back to Splunk
6. **Repeat** — Splunk AI results from the previous cycle feed the next LLM prompt

---

## Project Structure

```
icp_observability_agent.py    # Main agent: polling, anomaly detection, automated response, web UI
camera_ml.py                  # In-process Camera AI: 24h occupancy ML, crowd-surge detection
icp_agent.env                 # Environment variable template (copy to .env)
icp-agent.service             # systemd service unit
test_splunk_ai.py             # Validates Splunk MLTK connectivity and AI commands
architecture.svg              # Architecture diagram
ICP transparent logo.png      # ICP logo used in web UI header
icp_splunk_ai_observability_poster.html    # A4 solution overview poster (v1)
icp_splunk_ai_observability_poster_v2.html # A4 solution overview poster (v2 — Camera AI)
demo_narration.txt            # 3-minute demo audio script (v1)
demo_narration.mp3            # Demo audio (v1)
demo_narration_v2.txt         # 3-minute demo audio script (v2 — Camera AI)
demo_narration_v2.mp3         # Demo audio (v2, gTTS)
icp_building_sensor_dashboard.xml          # Splunk dashboard XML for sensor data
icp_sw01_dashboard.xml                     # Splunk dashboard XML for switch data
splunk_meraki_dashboard_setup.md           # Detailed Splunk setup guide
MV12W_kitchen.py              # Reference: standalone 24h camera history terminal viewer
pta_gps_dashboard.xml         # Splunk dashboard: PTA GPS tracker map iframe + ping history
Phone-App-GPS-Splunk.txt      # PTA GPS tracker — integration reference, API, gotchas
PTA-Splunk-Dash-Setup.txt     # PTA GPS tracker — step-by-step setup guide
```

---

## Live Deployment

This system runs on real hardware at **Innovation Central Perth**, Curtin University, Perth WA:
- 3× Cisco Meraki MV12W cameras with MV Sense AI occupancy analytics
  - ICP Workshop (main event space)
  - ICP Demo Area (hardware showcase)
  - ICP Pantry Area (kitchen/social space)
- 1× Cisco Meraki MT10 environmental sensor
- 1× Cisco Meraki MS355-48X2 switch
- 2× Cisco Meraki MR57 WAPs
- 1× Cisco Meraki MX105 security gateway
- Splunk Enterprise 10.2.3 on Ubuntu 22.04

---

## License

MIT License — see [LICENSE](LICENSE)

---

## Authors

- Stanley Chong — Innovation Central Perth, Curtin University (stanley.chong@curtin.edu.au)
