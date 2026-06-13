# ICP Splunk AI Observability Agent

**Closed-loop AI observability for smart buildings using Cisco Meraki hardware + Splunk Enterprise MLTK AI + multi-model LLMs.**

Built by [Innovation Central Perth](https://icp.curtin.edu.au) at Curtin University for the Splunk AI Observability Hackathon.

![Architecture Diagram](architecture.svg)

---

## What This Does

Real Cisco Meraki hardware (cameras, sensors, switches) streams data into Splunk Enterprise every 2 minutes. Splunk's MLTK AI (`anomalydetection` + `predict/LLP5`) runs statistical analysis on the ingested time-series data. When anomalies are detected, the **Automated AI Response Engine** generates LLM-written incident reports, creates Splunk saved alerts via REST API, and logs `icp:automated_response` events back to Splunk — closing the AI loop.

```
Cisco Meraki Hardware
  MV Cameras (people count)  ─┐
  MT Sensors (temp/humidity)  ├─ Meraki API ─► ICP Agent ─► Splunk HEC
  MS Switch (traffic/PoE)    ─┘                    │
                                                    │◄── Splunk AI results (anomalydetection · predict/LLP5)
                                              [Anomaly?]
                                                    │
                                            LLM Incident Report
                                                    │
                               ┌────────────────────┴──────────────────────┐
                               │  icp:automated_response → Splunk          │
                               │  Splunk Saved Alert created (REST API)    │
                               │  Operations Dashboard + NL Chat UI        │
                               └───────────────────────────────────────────┘
```

---

## Key Features

- **Splunk AI (MLTK)** — `anomalydetection` for statistical outlier detection across temperature, humidity, and camera occupancy; `predict/LLP5` for temperature forecasting
- **Automated Response Engine** — when Splunk AI flags an anomaly, automatically: generates an LLM incident report, creates a Splunk saved alert via REST API, and pushes `icp:automated_response` events back to Splunk
- **Bidirectional Splunk loop** — data flows INTO Splunk via HEC, Splunk AI results flow BACK into LLM context, and automated responses flow BACK INTO Splunk — Splunk is the AI brain, not just a log sink
- **In-application LLM model switching** — GPT-4o mini (analyst), Llama 3.3 70B (chat composer), Gemini Flash 2.0 (fallback) — demonstrates multi-model AI routing
- **Natural language chat** — every LLM response explicitly cites Splunk AI findings
- **Historical trend charts** — clickable tiles open 12-hour Chart.js graphs pre-populated from Splunk `timechart` queries
- **FastAPI web UI** — live sensor tiles, Splunk AI insights panel, automated responses panel, NL chat interface

---

## Hardware Requirements

| Device | Purpose | Meraki API Used |
|---|---|---|
| Meraki MV cameras (MV12W etc.) | People counting via MV Sense zone analytics | `/devices/{serial}/camera/analytics/zones/{id}/history` |
| Meraki MT sensors (MT10 etc.) | Temperature, humidity, door state | `/organizations/{orgId}/sensor/readings/latest` |
| Meraki MS switch (MS355 etc.) | Per-port traffic (Kbps), PoE (Wh), WAP client counts | `/devices/{serial}/switch/ports/statuses` |
| Ubuntu server (20.04+) | Runs Splunk Enterprise + observability agent | — |

> All hardware components are independently optional — comment out any polling function you don't need in `icp_observability_agent.py`.

---

## Software Requirements

| Component | Version | Notes |
|---|---|---|
| Splunk Enterprise | 10.x | Developer License: 10 GB/day free at splunk.com |
| Splunk Machine Learning Toolkit | Latest | Required for `anomalydetection` and `predict/LLP5` |
| Python for Scientific Computing | Latest | Splunkbase dependency for MLTK |
| Python | 3.10+ | Agent runtime |
| OpenRouter account | — | Free tier available; provides access to GPT-4o mini, Llama 3.3, Gemini |

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
# Replace serials with your own Meraki device serials
SENSORS = [
    {"serial": "YOUR-SENSOR-SERIAL", "name": "Main Sensor",
     "metrics": ["temperature", "humidity", "battery"]},
]

SWITCH = {
    "serial": "YOUR-SWITCH-SERIAL",
    "name":   "YOUR-SW01",
    "model":  "MS355-48X2",
    "ports": {
        "48": "Uplink",
        # Add your camera/WAP ports here
    }
}

CAMERAS = [
    {"serial": "YOUR-CAMERA-SERIAL", "name": "Main Area",
     "zones": [{"id": "0", "label": "Full Frame"}]},
]
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
# Edit icp-agent.service — update User= and WorkingDirectory= for your system
sudo cp icp-agent.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now icp-agent
sudo journalctl -u icp-agent -f
```

---

## Splunk Sourcetypes

| Sourcetype | Description |
|---|---|
| `icp:sensor_latest` | MT sensor readings: temperature, humidity, door, battery |
| `icp:switch_port_status` | MS switch per-port: traffic Kbps, PoE Wh, client count |
| `icp:camera_analytics` | MV camera zone analytics: average count, entrances |
| `icp:anomaly` | Threshold-based anomaly events |
| `icp:splunk_ai_insight` | Summary of Splunk AI analysis results per poll |
| `icp:automated_response` | **AI-generated incident reports + Splunk alert creation events** |

---

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Web UI (live dashboard + chat) |
| `GET /api/status` | Current sensor state, anomalies, Splunk AI insights, automated responses |
| `POST /api/chat` | Natural language query — returns LLM response grounded in live data |
| `GET /api/history` | 12-hour time-series history from Splunk (5 parallel `timechart` queries) |
| `GET /api/responses` | Automated AI responses triggered by Splunk AI anomaly detection |

---

## Architecture

See [`architecture.svg`](architecture.svg) for the full data flow diagram.

**The AI loop:**

1. **Collect** — Meraki REST API → Python agent polls every 2 minutes
2. **Ingest** — Structured JSON events pushed to Splunk via HEC
3. **Detect** — Splunk MLTK `anomalydetection` (statistical outliers) + `predict/LLP5` (forecasting) run on ingested data
4. **Interpret** — LLMs receive Splunk AI results as context, generate responses that explicitly cite Splunk findings
5. **Act** — Automated Response Engine: LLM writes incident report → Splunk saved alert created via REST API → `icp:automated_response` logged back to Splunk
6. **Repeat** — Splunk AI results from the previous cycle feed the next LLM prompt

---

## Project Structure

```
icp_observability_agent.py    # Main agent: polling, Splunk AI, automated response engine, web UI
icp_agent.env                 # Environment variable template (copy to .env)
icp-agent.service             # systemd service unit
test_splunk_ai.py             # Validates Splunk MLTK connectivity and AI commands
architecture.svg              # Architecture diagram
ICP transparent logo.png      # ICP logo used in web UI header
icp_splunk_ai_observability_poster.html  # A4 solution overview poster
icp_building_sensor_dashboard.xml        # Splunk dashboard XML for sensor data
icp_sw01_dashboard.xml                   # Splunk dashboard XML for switch data
splunk_meraki_dashboard_setup.md         # Detailed Splunk setup guide
```

---

## Live Deployment

This system runs on real hardware at **Innovation Central Perth**, Curtin University, Perth WA:
- 3× Cisco Meraki MV12W cameras with MV Sense AI occupancy analytics
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
