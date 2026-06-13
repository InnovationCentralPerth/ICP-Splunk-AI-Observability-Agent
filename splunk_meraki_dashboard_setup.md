# Splunk Enterprise + Meraki Sensor, Switch & AI Agent Setup
**ICP Building — Curtin University**
**Splunk Enterprise 10.2.3 | Ubuntu 22.04 | Tailscale Remote Access**

---

## Environment Overview

| Component | Details |
|---|---|
| Splunk Host | `<your-hostname>` (Ubuntu 22.04) |
| Splunk Version | Enterprise 10.2.3 |
| Splunk Web Port | 8008 |
| Splunk HEC Port | 8088 (HTTPS) |
| Remote Access | Tailscale (`<your-server-hostname>`) |
| Tailscale IP | `<your-tailscale-ip>` |
| Meraki Organisation | ICP_PERTH (ID: `703124491823221434`) |
| Meraki Network | ICP Building (`L_703124491823231484`) |
| AI Agent Port | 5000 |
| Python Environment | `.venv` at `/home/stanley/projects/icp-agent/.venv` |

---

## Part 1 — Splunk Enterprise Installation

### 1.1 Install the .deb Package
```bash
sudo dpkg -i splunk-<version>-linux-amd64.deb
```

### 1.2 First Start (Run as Root)
```bash
sudo /opt/splunk/bin/splunk start --accept-license --run-as-root
```
On first run, Splunk prompts for admin username and password. These become your login credentials.

> **Note:** Running as root triggers a deprecation warning but is acceptable for this setup. Do not use `sudo` without `--run-as-root` or the session/ownership will be inconsistent.

### 1.3 Enable Boot Start via systemd
```bash
sudo /opt/splunk/bin/splunk enable boot-start \
  -systemd-managed 1 \
  --run-as-root
sudo systemctl daemon-reload
sudo systemctl enable Splunkd
sudo systemctl start Splunkd
```

Verify on reboot:
```bash
sudo systemctl status Splunkd
```

### 1.4 Open Firewall Ports
```bash
sudo ufw allow 8008/tcp   # Splunk Web UI
sudo ufw allow 8088/tcp   # HTTP Event Collector (HEC)
sudo ufw allow 5000/tcp   # ICP Observability Agent
sudo ufw allow in on tailscale0 to any port 8008 proto tcp
sudo ufw allow in on tailscale0 to any port 5000 proto tcp
sudo ufw reload
sudo ufw status
```

### 1.5 Configure web.conf for External Access
```bash
sudo nano /opt/splunk/etc/system/local/web.conf
```
```ini
[settings]
httpport = 8008
server.socket_host = 0.0.0.0
enableSplunkWebSSL = false
max_upload_size = 500
response.timeout = 60
verifyCookiesWorkDuringLogin = false
root_endpoint = /
```

> **Invalid keys to avoid** — these cause startup warnings in Splunk 10.2.3:
> `SplunkdConnectionTimeout`, `allowEmbedTokenAuth`, `hostnameAndPort`

### 1.6 Configure server.conf
```bash
sudo nano /opt/splunk/etc/system/local/server.conf
```
```ini
[general]
serverName = <your-server-hostname>
hostnameOption = fullyqualifiedname
Pass4SymmKey = <keep existing encrypted value>

[httpServer]
acceptFrom = *

[sslConfig]
sslPassword = <keep existing encrypted value>

# Keep all auto-generated lmpool stanzas intact
[lmpool:auto_generated_pool_download-trial]
...
```

> **Critical:** Never duplicate `[general]` stanzas. Merge all keys into one stanza. Removing `Pass4SymmKey` will break internal cluster auth.

### 1.7 Apply Developer License
The Splunk Developer Personal License (10 GB/day, 1 year) replaces the default 500 MB trial.

```bash
# SCP license file to Brannigan first
scp Splunk.License stanley@<your-tailscale-ip>:/home/stanley/

# Apply license
sudo /opt/splunk/bin/splunk add licenses /home/stanley/Splunk.License \
  --run-as-root -auth <splunk-user>:<splunk-password>

# Restart to activate
sudo /opt/splunk/bin/splunk restart --run-as-root
```

Verify:
```bash
sudo /opt/splunk/bin/splunk list licenses --run-as-root -auth <splunk-user>:<splunk-password> \
  2>/dev/null | grep -E "label|quota|stack_id|status"
```
Expected: `label: Splunk Developer Personal License`, `quota: 10737418240`, `stack_id: enterprise`

### 1.8 Remote Access — Tailscale + Corporate DNS Workaround
Corporate DNS (e.g. Curtin: `<your-corporate-dns>`) cannot resolve Tailscale MagicDNS hostnames. Fix:

**Option A — Windows hosts file (permanent):**
```
C:\Windows\System32\drivers\etc\hosts
```
```
<your-tailscale-ip>    <your-server-hostname>
```
```powershell
ipconfig /flushdns
```

**Option B — SSH port forward (session-based, bypasses all CSRF issues):**
```powershell
# Tunnel both Splunk and Agent in one command
ssh -L 8008:127.0.0.1:8008 -L 5000:127.0.0.1:5000 stanley@<your-tailscale-ip>
```
Then access:
- Splunk: `http://127.0.0.1:8008`
- Agent: `http://127.0.0.1:5000`

**Create a Windows desktop shortcut (`icp_lab.bat`):**
```batch
start http://127.0.0.1:8008
start http://127.0.0.1:5000
ssh -L 8008:127.0.0.1:8008 -L 5000:127.0.0.1:5000 stanley@<your-tailscale-ip>
```

### 1.9 Splunk Login CSRF Issue (Post-Reboot)
After every reboot, the web UI may show `Server Error` on login due to stale CSRF tokens. Fix:

```bash
sudo /opt/splunk/bin/splunk stop --run-as-root
sudo rm -rf /opt/splunk/var/run/splunk/sessions/*
sudo rm -f /opt/splunk/var/run/splunk/csrf_token
sudo /opt/splunk/bin/splunk start --run-as-root
```
Wait 60 seconds then use incognito browser or SSH tunnel (Option B above).

> **Root cause:** Splunk running as root causes session file ownership conflicts on restart. Resolved permanently when migrating to Splunk Cloud.

---

## Part 2 — Cisco Meraki Add-on Installation

### 2.1 Install Splunk_TA_cisco_meraki
```
Apps → Find More Apps → Search "Cisco Meraki" → Install
```
Installs to: `/opt/splunk/etc/apps/Splunk_TA_cisco_meraki/`

### 2.2 Configure Meraki Organisation Account
```
Apps → Cisco Meraki Add-on → Configuration → Add
```
| Field | Value |
|---|---|
| Account Name | `ICP_PERTH` |
| API Key | `<your Meraki API key>` |
| Organisation ID | `703124491823221434` |

API key location: `Meraki Dashboard → My Profile → API Access → Generate API Key`

---

## Part 3 — Meraki Sensor Input Configuration (MT10)

### 3.1 Sensors in Use
| Serial | Type | Location | Metrics |
|---|---|---|---|
| Q3CA-7SBV-ZCTA | MT10 | ICP CRUX | Temperature, Humidity, Battery |
| Q3CC-WK69-K7TM | MT10 | ICP SIDE DOOR | Door, Battery |

### 3.2 inputs.conf Configuration
```bash
sudo nano /opt/splunk/etc/apps/Splunk_TA_cisco_meraki/local/inputs.conf
```
```ini
[cisco_meraki_sensor_readings_history://MT10_ICP_CRUX]
index = main
interval = 1800
organization_name = ICP_PERTH
start_from_days_ago = 7

[cisco_meraki_cameras://ICP_CAMERAS]
index = main
interval = 86400
organization_name = ICP_PERTH

[cisco_meraki_devices_availabilities://ICP_SW01_AVAILABILITY]
index = main
interval = 3600
organization_name = ICP_PERTH

[cisco_meraki_summary_switch_power_history://ICP_SW01_POWER]
index = main
interval = 3600
start_from_days_ago = 7
organization_name = ICP_PERTH

[cisco_meraki_power_modules_statuses_by_device://ICP_SW01_PSU]
index = main
interval = 3600
organization_name = ICP_PERTH
```

> **Note:** Meraki API retains only 7 days of sensor history. Create inputs promptly.

### 3.3 Key SPL Notes
Sourcetype uses **single colon**: `meraki:sensorreadingshistory`

Dotted field names require **single quotes** in eval:
```spl
| eval display=round('temperature.celsius',1)
```

Do **not** concatenate units in eval if also using the panel `unit` option — causes double units (`% %`).

---

## Part 4 — Environmental Sensor Dashboard

**Title:** `ICP Building - Environmental Sensor Monitor`
**Type:** Classic Dashboard (not Dashboard Studio)

### 4.1 Key Field Names
| Field | Description |
|---|---|
| `temperature.celsius` | Temperature (single-quote in eval) |
| `humidity.relativePercentage` | Humidity (single-quote in eval) |
| `battery.percentage` | Battery level |
| `door.open` | Boolean door state |

### 4.2 Dashboard Panels
- Current Temperature stat — colour threshold at 24°C
- Current Humidity stat — colour threshold at 70%
- Door Status stat (ICP SIDE DOOR)
- Battery Level stat (ICP CRUX)
- Temperature time series 7-day with 24°C threshold line
- Humidity time series 7-day with 70% threshold line
- Temperature vs Humidity correlation (24h)
- Door open events by hour (7-day column chart)
- Temperature anomaly detection (`anomalydetection` command)
- Temperature 24h forecast (`predict` with `LLP5` algorithm)
- Statistics summary table
- Threshold breach events table

### 4.3 Forecasting Algorithm
```spl
| predict temperature future_timespan=24 algorithm=LLP5
```
**LLP5** = Local Level with Period 5 — exponential smoothing for daily HVAC cycles.

### 4.4 Import
```
Dashboards → Create New Dashboard → Classic Dashboard
→ Edit → Source (</>) → Paste XML → Save
```

---

## Part 5 — Switch Input Configuration (MS355-48X2)

### 5.1 Switch Details
| Field | Value |
|---|---|
| Name | ICP-SW01 |
| Model | MS355-48X2 |
| Serial | Q2DY-GJ53-PCYT |
| LAN IP | 192.168.6.29 |
| Firmware | switch-17-2-2 |
| Total Ports | 56 (6 active) |

### 5.2 Connected Devices
| Port | Device | Type | ~Traffic | ~PoE |
|---|---|---|---|---|
| 1 | MRWA MV12W Corner Of Office | Camera | 22 Kbps | 103 Wh |
| 2 | MRWA MV12W Table Test | Camera | 24 Kbps | 104 Wh |
| 13 | MRWA MV12W ICP Bookshelf | Camera | 80 Kbps | 106 Wh |
| 43 | Meraki MR57 - ICP Kitchen WAP | Access Point | 1942 Kbps | 396 Wh |
| 45 | Meraki MR57 - ICP Intern Desk WAP | Access Point | 909 Kbps | 402 Wh |
| 48 | Meraki MX105 - ICP-MX | Firewall/Uplink | 2815 Kbps | 0 Wh |

### 5.3 Known TA Limitations
- `cisco_meraki_switch_ports_by_switch` → **404** (requires higher Meraki license)
- `cisco_meraki_switches` → **count=0** (inventory endpoint incompatible)
- Use Python HEC poller for per-port data (Part 6)

### 5.4 Enable HEC
```bash
sudo /opt/splunk/bin/splunk http-event-collector enable \
  -uri https://localhost:8089 \
  -auth <splunk-user>:<splunk-password> --run-as-root

sudo /opt/splunk/bin/splunk restart --run-as-root
```

Create token:
```
Settings → Data Inputs → HTTP Event Collector → New Token
→ Name: meraki-switch → Submit → Copy Token
```

Test HEC (note: HTTPS with `-k` flag required):
```bash
curl -k https://127.0.0.1:8088/services/collector \
  -H "Authorization: Splunk <token>" \
  -d '{"event":"HEC test","sourcetype":"test"}'
```
Expected: `{"text":"Success","code":0}`

---

## Part 6 — Meraki Switch HEC Poller (systemd Service)

The switch poller runs as a standalone Python script outside the `.venv`, posting directly to Splunk HEC every 5 minutes.

Script location: `/home/stanley/meraki_switch_poller.py`

### 6.1 systemd Service
```bash
sudo tee /etc/systemd/system/meraki-switch-poller.service << 'EOF'
[Unit]
Description=Meraki ICP-SW01 Switch Port Poller
After=network.target

[Service]
Type=simple
User=stanley
ExecStart=/usr/bin/python3 /home/stanley/meraki_switch_poller.py
Restart=always
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable meraki-switch-poller
sudo systemctl start meraki-switch-poller
sudo systemctl status meraki-switch-poller
```

Monitor:
```bash
sudo journalctl -u meraki-switch-poller -f
```

### 6.2 Verify Data
```spl
index=main sourcetype="meraki:switch_port_status"
| stats count by portId status
| sort portId
```

---

## Part 7 — AI Apps Installed (Splunkbase)

Installed after applying the Developer License:

| App | Purpose |
|---|---|
| Splunk AI Toolkit (`Splunk_ML_Toolkit`) | `fit`, `apply`, `anomalydetection`, `predict`, IsolationForest |
| Python for Scientific Computing Linux x86_64 | numpy, scipy, scikit-learn, pandas dependency for AI Toolkit |
| Splunk MCP Server (`Splunk_MCP_Server`) | Exposes Splunk as MCP endpoint for LLM agent integration |
| AI Query Assistant (`AI_Query_Assistant_for_Splunk`) | Natural language → SPL conversion |

Install order matters: Python Scientific Computing must be installed before AI Toolkit.

---

## Part 8 — ICP Observability Agent (AI Chatbot)

### 8.1 Overview
A Python FastAPI application that:
- Polls Meraki APIs every 120 seconds (sensors, switch, cameras)
- Posts all data to Splunk HEC
- Detects anomalies against configured thresholds
- Generates AI narratives via OpenRouter
- Exposes a web chatbot UI on port 5000

### 8.2 Device Inventory
| Device | Serial | Data |
|---|---|---|
| ICP CRUX (MT10) | Q3CA-7SBV-ZCTA | Temp, humidity, battery |
| ICP SIDE DOOR (MT10) | Q3CC-WK69-K7TM | Door state, battery |
| ICP-SW01 (MS355) | Q2DY-GJ53-PCYT | 56 ports, traffic, PoE, WAP clients |
| Coffee/Microwave Cam (MV12W) | Q2GV-52D5-YTLK | People count (Full Frame) |
| ICP Bookshelf Cam (MV12W) | Q2GV-MGCS-QRM5 | People count (Full Frame) |
| DEMO CAM (MV12W) | Q2GV-XPS8-82TX | People count (Full Frame) |

### 8.3 MV Sense Configuration
MV Sense must be enabled on each camera for people analytics:
```bash
curl -s -X PUT \
  -H "X-Cisco-Meraki-API-Key: $MERAKI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"senseEnabled": true}' \
  "https://api.meraki.com/api/v1/devices/<CAMERA_SERIAL>/camera/sense"
```

> **Note:** After enabling MV Sense, allow 1-2 hours for the first analytics bucket to appear. Named area zones (coffee, microwave) use a different API endpoint — only Full Frame zone (`id: "0"`) is used.

### 8.4 Project Structure
```
/home/stanley/projects/icp-agent/
├── .venv/                          # Python virtual environment
├── icp_observability_agent.py      # Main agent (v2)
└── .env                            # Environment config
```

### 8.5 Virtual Environment Setup
```bash
cd /home/stanley/projects/icp-agent

# Create venv
python3 -m venv .venv

# Activate
source .venv/bin/activate

# Install dependencies
pip install requests fastapi uvicorn httpx python-dotenv
```

### 8.6 Environment Configuration (.env)
```ini
# Meraki
MERAKI_API_KEY=<your_meraki_api_key>

# Splunk HEC
SPLUNK_HEC_URL=https://127.0.0.1:8088/services/collector
SPLUNK_HEC_TOKEN=<your_hec_token>

# OpenRouter AI
OPENROUTER_API_KEY=sk-or-v1-...
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
OPENROUTER_REFERER=https://curtin.edu.au

# Models
MODEL_ANALYST=openai/gpt-oss-120b:free
MODEL_COMPOSER=meta-llama/llama-3.3-70b-instruct:free
MODEL_FALLBACK=openai/gpt-oss-20b:free

# Polling
POLL_INTERVAL=120
```

> **Critical:** Variable name is `SPLUNK_HEC_TOKEN` not `SPLUNK_HEC_KEY`.

> **OpenRouter rate limiting:** Free tier models are rate-limited. Add credits at `https://openrouter.ai/settings/credits` to avoid 429 errors during demos.

### 8.7 Anomaly Detection Thresholds
| Signal | Threshold | Severity |
|---|---|---|
| Temperature | > 24°C | MEDIUM / HIGH (>28°C) |
| Humidity | > 70% | MEDIUM |
| Door | Open | INFO |
| WAP clients | > 10 | INFO |
| Uplink traffic | > 5000 Kbps | MEDIUM |

### 8.8 Sourcetypes Written to Splunk
| Sourcetype | Data |
|---|---|
| `icp:sensor_latest` | MT10 temperature, humidity, door, battery |
| `icp:switch_port_status` | MS355 per-port traffic, PoE, clients |
| `icp:camera_analytics` | MV12W people count per zone |
| `icp:anomaly` | Detected anomaly events |

### 8.9 Run as systemd Service (Auto-start on Reboot)

> **Important:** The agent runs inside a `.venv`. The service must use the venv Python binary, not system Python.

```bash
sudo tee /etc/systemd/system/icp-agent.service << 'EOF'
[Unit]
Description=ICP Building Observability Agent
After=network.target splunkd.service

[Service]
Type=simple
User=stanley
WorkingDirectory=/home/stanley/projects/icp-agent
EnvironmentFile=/home/stanley/projects/icp-agent/.env
ExecStart=/home/stanley/projects/icp-agent/.venv/bin/python3 \
    /home/stanley/projects/icp-agent/icp_observability_agent.py
Restart=always
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable icp-agent
sudo systemctl start icp-agent
sudo systemctl status icp-agent
```

> **Key difference from switch poller:** Uses `.venv/bin/python3` (full path to venv Python) not `/usr/bin/python3`. The `EnvironmentFile` loads `.env` directly so `load_dotenv()` in the script is a safety fallback.

Monitor:
```bash
sudo journalctl -u icp-agent -f
```

Restart after code changes:
```bash
sudo systemctl restart icp-agent
```

### 8.10 Verify Agent Running
```bash
# Check service status
sudo systemctl status icp-agent

# Test API
curl -s http://127.0.0.1:5000/api/status | python3 -m json.tool

# Test AI chat
curl -s -X POST http://127.0.0.1:5000/api/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "What is the current building status?"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['response'])"
```

---

## Part 9 — Data Index Summary

All data lands in `index=main`.

| Sourcetype | Origin | Data | Interval |
|---|---|---|---|
| `meraki:sensorreadingshistory` | Meraki TA | MT10 temp, humidity, door, battery | 30 min |
| `meraki:cameras` | Meraki TA | MV12W camera events | 24 hrs |
| `meraki:switch_port_status` | Switch HEC poller | ICP-SW01 per-port data | 5 min |
| `icp:sensor_latest` | ICP Agent | Latest MT10 readings | 2 min |
| `icp:switch_port_status` | ICP Agent | Latest switch port data | 2 min |
| `icp:camera_analytics` | ICP Agent | MV12W people count | 2 min |
| `icp:anomaly` | ICP Agent | Detected anomalies | On detection |

---

## Part 10 — Services Auto-Start Summary

Three services run on Brannigan and auto-start on reboot:

| Service | Unit File | Start Command |
|---|---|---|
| Splunk Enterprise | `Splunkd` (auto-created) | `sudo systemctl start Splunkd` |
| Switch HEC Poller | `meraki-switch-poller.service` | `sudo systemctl start meraki-switch-poller` |
| ICP Observability Agent | `icp-agent.service` | `sudo systemctl start icp-agent` |

Check all at once:
```bash
sudo systemctl status Splunkd meraki-switch-poller icp-agent
```

Enable all on fresh install:
```bash
sudo systemctl enable Splunkd meraki-switch-poller icp-agent
```

---

## Part 11 — Useful Diagnostic Commands

### Splunk
```bash
# Restart Splunk
sudo /opt/splunk/bin/splunk restart --run-as-root

# Check config warnings
sudo /opt/splunk/bin/splunk btool check --debug --run-as-root 2>&1 | grep -i error

# Clear CSRF sessions (post-reboot login fix)
sudo /opt/splunk/bin/splunk stop --run-as-root
sudo rm -rf /opt/splunk/var/run/splunk/sessions/*
sudo rm -f /opt/splunk/var/run/splunk/csrf_token
sudo /opt/splunk/bin/splunk start --run-as-root

# Check TA input logs
sudo ls /opt/splunk/var/log/splunk/ | grep meraki
sudo tail -50 /opt/splunk/var/log/splunk/splunk_ta_cisco_meraki_<input_name>.log
```

### Splunk Search (SPL)
```spl
-- All sourcetypes and counts
index=main | stats count by sourcetype | sort - count

-- Sensor data
index=main sourcetype="meraki:sensorreadingshistory"
| stats count avg(value) min(value) max(value) by serial metric

-- Switch port data
index=main sourcetype="meraki:switch_port_status"
| stats count by portId status | sort portId

-- Agent data
index=main sourcetype="icp:*"
| stats count by sourcetype

-- Recent anomalies
index=main sourcetype="icp:anomaly"
| table _time type signal message severity
| sort - _time
```

### ICP Agent
```bash
# Service status
sudo systemctl status icp-agent

# Live logs
sudo journalctl -u icp-agent -f

# API status
curl -s http://127.0.0.1:5000/api/status | python3 -m json.tool

# Manual AI narrative
curl -s http://127.0.0.1:5000/api/narrative \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['narrative'])"
```

### Switch Poller
```bash
sudo systemctl status meraki-switch-poller
sudo journalctl -u meraki-switch-poller -f
```

---

## Part 12 — Known Issues & Resolutions

| Issue | Cause | Resolution |
|---|---|---|
| `Server Error` on Splunk login from remote browser | CSRF token stale after reboot | Clear sessions (Part 1.9) or use SSH tunnel |
| Corporate DNS can't resolve Tailscale hostname | Curtin DNS (`<your-corporate-dns>`) doesn't know `<your-tailscale-domain>` | Add hosts file entry or use IP directly |
| Double units in stat panels (`% %`) | Unit in both eval concat and panel `unit` option | Remove concat from eval, keep `unit` option |
| Zero search results with correct SPL | Sourcetype uses single colon not double | `meraki:sensorreadingshistory` not `meraki::` |
| `switch_ports_by_switch` returns 404 | Requires higher Meraki license tier | Use Python HEC poller instead |
| `cisco_meraki_switches` returns count=0 | TA inventory endpoint incompatible | Use `devices_availabilities` input instead |
| HEC `Connection refused` | HEC not enabled, or using HTTP not HTTPS | Enable via CLI; use `https://` with `-k` flag |
| `round()` eval error on dotted fields | Splunk interprets dot as nested accessor | Wrap in single quotes: `round('temperature.celsius',1)` |
| Splunk startup invalid key warnings | Keys deprecated in Splunk 10.2.3 | Remove from web.conf: `SplunkdConnectionTimeout`, `allowEmbedTokenAuth`, `hostnameAndPort` |
| OpenRouter 404 on chat | Model ID not available on free tier | Check available models; update `.env` MODEL_ vars |
| OpenRouter 429 rate limit | Free tier shared rate limit | Add credits at openrouter.ai/settings/credits |
| MV camera people count shows zeros | Analytics bucket incomplete (< 1 hour) | Use 1h lookback; filter for non-zero buckets |
| DEMO CAM shows `Initialising` | MV Sense enabled today — no history yet | Wait 1-2 hours for first analytics bucket |
| Agent fails to load `.env` when run as service | Working directory mismatch | Use explicit path: `load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))` |
| Wrong Python used in systemd service | Service points to system Python not venv | Use `.venv/bin/python3` as `ExecStart` binary |

---

*Document updated: June 2026 | ICP Building, Curtin University, Bentley WA*
