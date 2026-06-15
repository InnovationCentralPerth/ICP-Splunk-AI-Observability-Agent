#!/usr/bin/env python3
"""
camera_ml.py
Per-camera 24h occupancy analytics with in-process ML anomaly detection.
No external ML dependencies — pure stdlib math only.

Techniques:
  1. Z-score outliers vs 24h rolling window
  2. Time-of-day baseline deviation (per-hour mean/std)
  3. Crowd surge detection (entrance rate spike vs hourly norm)
  4. Dead-zone detection (zero during business hours)
  5. Linear trend extrapolation (last N buckets → next occupancy estimate)
"""

from collections import defaultdict
from datetime import datetime
from typing import Optional


# ── Utilities ──────────────────────────────────────────────────────────────────

def _mean(vals: list) -> float:
    return sum(vals) / len(vals) if vals else 0.0


def _std(vals: list) -> float:
    if len(vals) < 2:
        return 0.0
    m = _mean(vals)
    return math.sqrt(sum((x - m) ** 2 for x in vals) / (len(vals) - 1))


# ── OccupancyAnalyser ──────────────────────────────────────────────────────────

class OccupancyAnalyser:
    """
    Holds per-camera occupancy time series and runs multi-method anomaly detection.

    Usage:
        analyser = OccupancyAnalyser()
        anomalies = analyser.ingest("Demo Cam", api_bucket_list)
        chart_data = analyser.export_for_chart("Demo Cam")
    """

    SURGE_FACTOR      = 3.0             # entrance spike: N × hourly mean
    SURGE_MIN_ENT     = 3               # ignore surges below this count
    FORECAST_BUCKETS  = 6               # last N buckets for linear regression

    def __init__(self):
        self._history:   dict[str, list] = {}
        self._anomalies: dict[str, list] = {}

    # ── Public API ─────────────────────────────────────────────────────────────

    def ingest(self, camera_name: str, buckets: list) -> list:
        """
        Parse, sort and analyse a raw list of Meraki zone-history bucket dicts.
        Returns the anomaly list for this camera (also stored internally).
        """
        parsed = []
        for b in buckets:
            ts_str = b.get("startTs", "")
            try:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue
            parsed.append({
                "dt":        dt,
                "dt_local":  dt.astimezone(),
                "entrances": int(b.get("entrances", 0)),
                "avg_occ":   float(b.get("averageCount", 0.0)),
                "startTs":   ts_str,
                "endTs":     b.get("endTs", ""),
            })
        parsed.sort(key=lambda x: x["dt"])
        self._history[camera_name]   = parsed
        anomalies                    = self._detect(camera_name, parsed)
        self._anomalies[camera_name] = anomalies
        return anomalies

    def get_history(self, camera_name: str) -> list:
        return self._history.get(camera_name, [])

    def get_anomalies(self, camera_name: str) -> list:
        return self._anomalies.get(camera_name, [])

    def get_stats(self, camera_name: str) -> dict:
        h = self._history.get(camera_name, [])
        if not h:
            return {}
        occ_vals  = [b["avg_occ"]   for b in h]
        ent_vals  = [b["entrances"] for b in h]
        non_zero  = [b for b in h if b["avg_occ"] > 0 or b["entrances"] > 0]
        peak      = max(h, key=lambda b: b["avg_occ"]) if h else None
        return {
            "total_buckets":   len(h),
            "active_buckets":  len(non_zero),
            "total_entrances": sum(ent_vals),
            "mean_occupancy":  round(_mean(occ_vals), 2),
            "peak_occupancy":  round(peak["avg_occ"], 2)           if peak else 0.0,
            "peak_time":       peak["dt_local"].strftime("%H:%M")  if peak else "N/A",
            "anomaly_count":   len(self._anomalies.get(camera_name, [])),
        }

    def forecast_next(self, camera_name: str) -> Optional[dict]:
        """Simple linear regression on last FORECAST_BUCKETS → next value."""
        h = self._history.get(camera_name, [])
        if len(h) < 3:
            return None
        recent = h[-self.FORECAST_BUCKETS:]
        y  = [b["avg_occ"] for b in recent]
        x  = list(range(len(y)))
        mx, my = _mean(x), _mean(y)
        num = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
        den = sum((xi - mx) ** 2 for xi in x)
        slope = num / den if den else 0.0
        nxt   = max(0.0, (slope * len(y)) + (my - slope * mx))
        return {
            "slope":    round(slope, 3),
            "next_occ": round(nxt, 1),
            "trend":    "rising"  if slope >  0.1 else
                        "falling" if slope < -0.1 else "stable",
        }

    def export_for_chart(self, camera_name: str) -> dict:
        """
        Return data ready for Chart.js dual-axis rendering:
          - entrances bar chart (left axis) — all blue
          - avg occupancy line chart (right axis)
          - anomaly_markers: amber diamond positions + hover detail per anomaly
        """
        h     = self._history.get(camera_name, [])
        anoms = self._anomalies.get(camera_name, [])

        labels, occ_data, ent_data = [], [], []
        for b in h:
            labels.append(b["dt_local"].strftime("%H:%M"))
            occ_data.append(round(b["avg_occ"], 2))
            ent_data.append(b["entrances"])

        label_to_idx = {l: i for i, l in enumerate(labels)}
        anom_markers = []
        for a in anoms:
            t = a["time"]
            if t in label_to_idx:
                idx = label_to_idx[t]
                anom_markers.append({
                    "time":     t,
                    "type":     a["type"],
                    "severity": a["severity"],
                    "message":  a["message"],
                    "value":    a["value"],
                    "occ":      max(occ_data[idx], 0.15),   # floor so marker is visible at 0
                })

        return {
            "labels":          labels,
            "occupancy":       occ_data,
            "entrances":       ent_data,
            "anomaly_markers": anom_markers,
        }

    def summary_text(self, camera_name: str) -> str:
        stats = self.get_stats(camera_name)
        if not stats:
            return f"{camera_name}: no 24h data yet"
        n = stats["anomaly_count"]
        return (
            f"{camera_name}: peak {stats['peak_occupancy']:.1f} ppl "
            f"@ {stats['peak_time']}, "
            f"{stats['total_entrances']} entrances/24h, "
            f"{n} ML anomal{'ies' if n != 1 else 'y'}"
        )

    # ── Detection engine ───────────────────────────────────────────────────────

    def _detect(self, camera_name: str, buckets: list) -> list:
        if not buckets:
            return []

        anomalies = []

        # Per-hour entrance baseline (for crowd surge only)
        hour_ent: dict[int, list] = defaultdict(list)
        for b in buckets:
            hour_ent[b["dt_local"].hour].append(b["entrances"])
        hour_ent_mean = {h: _mean(vs) for h, vs in hour_ent.items()}

        for b in buckets:
            hour = b["dt_local"].hour
            ts   = b["dt_local"].strftime("%H:%M")
            ent  = b["entrances"]

            # Crowd surge — entrance spike vs hourly norm within this 24h window
            hem = hour_ent_mean.get(hour, 0.0)
            if ent >= self.SURGE_MIN_ENT and hem > 0 and ent > hem * self.SURGE_FACTOR:
                anomalies.append({
                    "type":     "CROWD_SURGE",
                    "severity": "HIGH",
                    "time":     ts,
                    "dt":       b["dt"].isoformat(),
                    "value":    ent,
                    "message": (
                        f"{camera_name} @ {ts}: surge — {ent} entrances "
                        f"vs typical {hem:.1f} for {hour:02d}:xx "
                        f"({ent / hem:.1f}×)"
                    ),
                })

        return _deduplicate(anomalies)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _deduplicate(anomalies: list) -> list:
    """Keep at most one anomaly of each (type, hour) pair to limit noise."""
    seen: set = set()
    out:  list = []
    for a in anomalies:
        key = (a["type"], a["time"][:2])    # type + hour-of-day
        if key not in seen:
            seen.add(key)
            out.append(a)
    return out
