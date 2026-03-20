from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

import requests

AIRLIE_LAT = -20.2676
AIRLIE_LON = 148.7169
BRISBANE_TZ = ZoneInfo("Australia/Brisbane")

WMO_CODES = {
    0: "Clear",
    1: "Mostly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Rime fog",
    51: "Light drizzle",
    53: "Drizzle",
    55: "Heavy drizzle",
    61: "Light rain",
    63: "Rain",
    65: "Heavy rain",
    71: "Light snow",
    80: "Rain showers",
    81: "Showers",
    82: "Heavy showers",
    95: "Thunderstorm",
}


def _weather_visuals(summary: str, wind_speed: float | None = None) -> Dict[str, str]:
    s = (summary or "").lower()
    if "thunder" in s or "storm" in s:
        return {"variant": "stormy", "icon": "⛈️"}
    if "rain" in s or "drizzle" in s or "showers" in s:
        return {"variant": "rainy", "icon": "🌧️"}
    if "fog" in s:
        return {"variant": "foggy", "icon": "🌫️"}
    if wind_speed is not None and wind_speed >= 28:
        return {"variant": "windy", "icon": "🌬️"}
    if "clear" in s or "sun" in s:
        return {"variant": "sunny", "icon": "☀️"}
    return {"variant": "cloudy", "icon": "☁️"}


def _brisbane_now() -> datetime:
    return datetime.now(BRISBANE_TZ)


def _hourly_points(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    hourly = data.get("hourly") or {}
    times = hourly.get("time") or []
    temps = hourly.get("temperature_2m") or []
    rain = hourly.get("precipitation_probability") or []
    codes = hourly.get("weather_code") or []
    cloud = hourly.get("cloud_cover") or []
    wind = hourly.get("wind_speed_10m") or []
    out: List[Dict[str, Any]] = []
    now = _brisbane_now()
    for idx, raw in enumerate(times):
        try:
            dt = datetime.fromisoformat(raw).replace(tzinfo=BRISBANE_TZ)
        except Exception:
            continue
        if dt < now:
            continue
        out.append({
            "time": dt.strftime("%I %p").lstrip("0"),
            "temperature": temps[idx] if idx < len(temps) else None,
            "rain_chance": rain[idx] if idx < len(rain) else None,
            "summary": WMO_CODES.get(codes[idx], "Conditions") if idx < len(codes) else "Conditions",
            "cloud_cover": cloud[idx] if idx < len(cloud) else None,
            "wind_speed": wind[idx] if idx < len(wind) else None,
        })
        if len(out) >= 6:
            break
    return out


def get_airlie_weather() -> Dict[str, Any]:
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={AIRLIE_LAT}&longitude={AIRLIE_LON}"
        "&current=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m,cloud_cover"
        "&hourly=temperature_2m,precipitation_probability,weather_code,cloud_cover,wind_speed_10m"
        "&daily=temperature_2m_max,temperature_2m_min,precipitation_probability_max,weather_code"
        "&timezone=Australia%2FBrisbane&forecast_days=2"
    )
    now = _brisbane_now()
    visuals = _weather_visuals("Weather unavailable")
    fallback = {
        "ok": False,
        "location": "Airlie Beach",
        "summary": "Weather unavailable",
        "temperature": None,
        "wind_speed": None,
        "humidity": None,
        "cloud_cover": None,
        "rain": None,
        "updated_at": now.strftime("%H:%M"),
        "updated_label": f"Brisbane time {now.strftime('%H:%M')}",
        "brisbane_time": now.strftime("%H:%M"),
        "brisbane_date": now.strftime("%a %d %b"),
        "today": {},
        "tomorrow": {},
        "hourly": [],
        "variant": visuals["variant"],
        "icon": visuals["icon"],
    }
    try:
        response = requests.get(url, timeout=8)
        response.raise_for_status()
        data = response.json()
        current = data.get("current") or {}
        daily = data.get("daily") or {}
        code = current.get("weather_code")
        summary = WMO_CODES.get(code, "Conditions updated")
        wind_speed = current.get("wind_speed_10m")
        visuals = _weather_visuals(summary, wind_speed)
        return {
            "ok": True,
            "location": "Airlie Beach",
            "summary": summary,
            "temperature": current.get("temperature_2m"),
            "feels_like": current.get("apparent_temperature"),
            "wind_speed": wind_speed,
            "humidity": current.get("relative_humidity_2m"),
            "cloud_cover": current.get("cloud_cover"),
            "rain": current.get("precipitation"),
            "updated_at": now.strftime("%H:%M"),
            "updated_label": f"Brisbane time {now.strftime('%H:%M')}",
            "brisbane_time": now.strftime("%H:%M"),
            "brisbane_date": now.strftime("%a %d %b"),
            "variant": visuals["variant"],
            "icon": visuals["icon"],
            "today": {
                "high": (daily.get("temperature_2m_max") or [None])[0],
                "low": (daily.get("temperature_2m_min") or [None])[0],
                "rain_chance": (daily.get("precipitation_probability_max") or [None])[0],
                "summary": WMO_CODES.get((daily.get("weather_code") or [None])[0], "")
            },
            "tomorrow": {
                "high": (daily.get("temperature_2m_max") or [None, None])[1],
                "low": (daily.get("temperature_2m_min") or [None, None])[1],
                "rain_chance": (daily.get("precipitation_probability_max") or [None, None])[1],
                "summary": WMO_CODES.get((daily.get("weather_code") or [None, None])[1], "")
            },
            "hourly": _hourly_points(data),
        }
    except Exception:
        return fallback
