"""Meteo della plancia: Open-Meteo, chiesto da Home Assistant (mai dal browser).

Comandi WebSocket:
  aurora_glass/weather/get       previsione della città scelta: adesso e tre giorni (tutti gli utenti)
  aurora_glass/weather/search    cerca una città per nome (solo amministratori)
  aurora_glass/weather/set_city  salva la città scelta (solo amministratori)

Tutte le richieste sono asincrone (sessione aiohttp di Home Assistant, timeout di 10 secondi) e non toccano
il ciclo degli eventi con chiamate bloccanti. La previsione resta in cache 20 minuti; l'ultima previsione
riuscita e la città scelta vivono in .storage, quindi sopravvivono a un riavvio. Se Open-Meteo non risponde
si mostra l'ultima previsione salvata, segnata come «vecchia»; se non c'è niente, un errore: nessun dato inventato.
Dati: Open-Meteo.com (https://open-meteo.com), licenza CC BY 4.0.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp
import voluptuous as vol

from homeassistant.components import websocket_api
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

STORE_VERSION = 1
STORE_KEY = f"{DOMAIN}.weather"
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
CACHE_SECONDS = 20 * 60
TIMEOUT = aiohttp.ClientTimeout(total=10)
SEARCH_LANGUAGES = ("ru", "en", "it")

# Astana, Kazakistan: la città di partenza finché non se ne sceglie un'altra
DEFAULT_CITY: dict[str, Any] = {
    "name": "Астана",
    "country": "Казахстан",
    "admin": "",
    "latitude": 51.1694,
    "longitude": 71.4491,
}


class WeatherError(Exception):
    """Open-Meteo non ha risposto o ha risposto in modo non valido."""


def _num(value: Any) -> float | None:
    """Un numero finito oppure None: i valori mancanti restano mancanti."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if value == value and abs(value) != float("inf") else None


def _round(value: Any, digits: int = 0) -> float | int | None:
    number = _num(value)
    if number is None:
        return None
    return round(number) if digits == 0 else round(number, digits)


def _at(values: Any, index: int) -> Any:
    return values[index] if isinstance(values, list) and index < len(values) else None


def parse_forecast(raw: dict[str, Any]) -> dict[str, Any]:
    """Dalla risposta di Open-Meteo alla forma che usa la plancia; solleva WeatherError se è inutilizzabile."""
    daily = raw.get("daily")
    if not isinstance(daily, dict) or not isinstance(daily.get("time"), list) or not daily["time"]:
        raise WeatherError("no daily data in the response")
    days = []
    for i, date in enumerate(daily["time"][:3]):
        days.append(
            {
                "date": str(date),
                "code": _round(_at(daily.get("weather_code"), i)),
                "max": _round(_at(daily.get("temperature_2m_max"), i)),
                "min": _round(_at(daily.get("temperature_2m_min"), i)),
                # null quando il modello non la fornisce: la plancia non mostra niente
                "pop": _round(_at(daily.get("precipitation_probability_max"), i)),
            }
        )
    cur = raw.get("current") if isinstance(raw.get("current"), dict) else {}
    current = {
        "temp": _round(cur.get("temperature_2m"), 1),
        "feels": _round(cur.get("apparent_temperature"), 1),
        "humidity": _round(cur.get("relative_humidity_2m")),
        "wind": _round(cur.get("wind_speed_10m"), 1),
        "code": _round(cur.get("weather_code")),
        "is_day": None if cur.get("is_day") is None else bool(cur.get("is_day")),
    }
    return {"current": current, "days": days}


class WeatherManager:
    """Città scelta, cache e ultima previsione riuscita."""

    def __init__(self, hass: HomeAssistant) -> None:
        self.hass = hass
        self.store: Store[dict[str, Any]] = Store(hass, STORE_VERSION, STORE_KEY)
        self.city: dict[str, Any] = dict(DEFAULT_CITY)
        self.forecast: dict[str, Any] | None = None  # {"city_key", "fetched", "data"}
        self._fetched_mono = 0.0
        self._lock = asyncio.Lock()

    async def async_load(self) -> None:
        data = await self.store.async_load() or {}
        city = data.get("city")
        if isinstance(city, dict) and _num(city.get("latitude")) is not None and _num(city.get("longitude")) is not None:
            self.city = city
        saved = data.get("forecast")
        if isinstance(saved, dict) and saved.get("city_key") == self._city_key() and isinstance(saved.get("data"), dict):
            self.forecast = saved

    def _city_key(self) -> str:
        return f"{self.city['latitude']:.3f},{self.city['longitude']:.3f}"

    def _payload(self) -> dict[str, Any]:
        return {"city": self.city, "forecast": self.forecast}

    @callback
    def _save_later(self) -> None:
        self.store.async_delay_save(self._payload, 2)

    async def async_set_city(self, city: dict[str, Any]) -> None:
        async with self._lock:
            self.city = city
            self.forecast = None
            self._fetched_mono = 0.0
        await self.store.async_save(self._payload())

    async def async_search(self, query: str, language: str) -> list[dict[str, Any]]:
        params = {"name": query, "count": 8, "language": language, "format": "json"}
        raw = await self._request(GEOCODING_URL, params)
        found = []
        for item in raw.get("results") or []:
            lat, lon = _num(item.get("latitude")), _num(item.get("longitude"))
            if not item.get("name") or lat is None or lon is None:
                continue
            found.append(
                {
                    "name": str(item["name"])[:80],
                    "country": str(item.get("country") or "")[:60],
                    "admin": str(item.get("admin1") or "")[:60],
                    "latitude": round(lat, 4),
                    "longitude": round(lon, 4),
                }
            )
        return found

    async def async_get(self, force: bool = False) -> dict[str, Any]:
        """Previsione pronta per la plancia; status: ok | stale | error."""
        async with self._lock:
            fresh = self.forecast is not None and self._fetched_mono and (
                asyncio.get_running_loop().time() - self._fetched_mono < CACHE_SECONDS
            )
            if not fresh or force:
                try:
                    data = parse_forecast(await self._request(FORECAST_URL, self._forecast_params()))
                except WeatherError as err:
                    _LOGGER.warning("Aurora Glass: meteo non disponibile (%s)", err)
                    if self.forecast is None:
                        return {"status": "error", "city": self.city, "error": str(err)}
                    return self._answer("stale", error=str(err))
                self.forecast = {"city_key": self._city_key(), "fetched": dt_util.utcnow().isoformat(), "data": data}
                self._fetched_mono = asyncio.get_running_loop().time()
                self._save_later()
                return self._answer("ok")
            return self._answer("ok")

    def _answer(self, status: str, error: str | None = None) -> dict[str, Any]:
        assert self.forecast is not None
        out = {"status": status, "city": self.city, "fetched": self.forecast["fetched"], **self.forecast["data"]}
        if error:
            out["error"] = error
        return out

    def _forecast_params(self) -> dict[str, Any]:
        return {
            "latitude": self.city["latitude"],
            "longitude": self.city["longitude"],
            "timezone": "auto",
            "forecast_days": 3,
            "current": "temperature_2m,apparent_temperature,relative_humidity_2m,weather_code,wind_speed_10m,is_day",
            "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max",
            "wind_speed_unit": "ms",
        }

    async def _request(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        session = async_get_clientsession(self.hass)
        try:
            async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                if resp.status != 200:
                    raise WeatherError(f"Open-Meteo HTTP {resp.status}")
                raw = await resp.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as err:
            raise WeatherError(type(err).__name__) from err
        if not isinstance(raw, dict) or raw.get("error"):
            raise WeatherError("invalid response")
        return raw


def _manager(hass: HomeAssistant) -> WeatherManager:
    return hass.data[DOMAIN]["weather"]


@websocket_api.websocket_command({vol.Required("type"): "aurora_glass/weather/get", vol.Optional("force", default=False): bool})
@websocket_api.async_response
async def ws_weather_get(hass: HomeAssistant, connection: websocket_api.ActiveConnection, msg: dict[str, Any]) -> None:
    # aggiornare di forza è riservato agli amministratori: gli altri usano la cache
    force = bool(msg["force"]) and connection.user.is_admin
    connection.send_result(msg["id"], await _manager(hass).async_get(force))


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "aurora_glass/weather/search",
        vol.Required("query"): vol.All(str, vol.Strip, vol.Length(min=2, max=60)),
        vol.Optional("language", default="ru"): vol.In(SEARCH_LANGUAGES),
    }
)
@websocket_api.async_response
async def ws_weather_search(hass: HomeAssistant, connection: websocket_api.ActiveConnection, msg: dict[str, Any]) -> None:
    try:
        results = await _manager(hass).async_search(msg["query"], msg["language"])
    except WeatherError as err:
        connection.send_error(msg["id"], "weather_unavailable", str(err))
        return
    connection.send_result(msg["id"], {"results": results})


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "aurora_glass/weather/set_city",
        vol.Required("name"): vol.All(str, vol.Strip, vol.Length(min=1, max=80)),
        vol.Optional("country", default=""): vol.All(str, vol.Length(max=60)),
        vol.Optional("admin", default=""): vol.All(str, vol.Length(max=60)),
        vol.Required("latitude"): vol.All(vol.Coerce(float), vol.Range(min=-90, max=90)),
        vol.Required("longitude"): vol.All(vol.Coerce(float), vol.Range(min=-180, max=180)),
    }
)
@websocket_api.async_response
async def ws_weather_set_city(hass: HomeAssistant, connection: websocket_api.ActiveConnection, msg: dict[str, Any]) -> None:
    manager = _manager(hass)
    await manager.async_set_city(
        {
            "name": msg["name"],
            "country": msg["country"],
            "admin": msg["admin"],
            "latitude": round(msg["latitude"], 4),
            "longitude": round(msg["longitude"], 4),
        }
    )
    connection.send_result(msg["id"], await manager.async_get())


async def async_setup_weather(hass: HomeAssistant) -> None:
    manager = WeatherManager(hass)
    await manager.async_load()
    hass.data[DOMAIN]["weather"] = manager
    for command in (ws_weather_get, ws_weather_search, ws_weather_set_city):
        websocket_api.async_register_command(hass, command)
