"""
================================================================================
RUBAIH v2 — CoinDCX INR-M Futures Cycle Bot with OpenRouter AI
================================================================================
⚠️  EDUCATIONAL / RESEARCH PURPOSES ONLY.
    Live CoinDCX trading only — no testnet mode.
    Default strategy: futures_cycle multi-pair scanner (flat → scan → buy → sell).
================================================================================
"""

import os
import json
import asyncio
import hashlib
import hmac
import math
import secrets
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Deque, Dict, List, Optional, Tuple

from dotenv import load_dotenv

load_dotenv()

import aiohttp
import asyncpg
import redis.asyncio as redis
import socketio
import yaml
from scipy.stats import norm

from openrouter_ai import OpenRouterAI, AIDecision, ai_configured
from cmd_bus import verify_command, filter_settings

# ==============================================================================
# CONFIG
# ==============================================================================
def _env_secret(name: str) -> str:
    """Load env value, strip whitespace and surrounding quotes from .env typos."""
    v = (os.getenv(name) or "").strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
        v = v[1:-1].strip()
    return v


with open("config.yaml", "r") as f:
    CFG = yaml.safe_load(f)

REST_URL = CFG["exchange"]["rest_url"].rstrip("/")
PUBLIC_URL = CFG["exchange"].get("public_url", "https://public.coindcx.com").rstrip("/")
WS_URL = CFG["exchange"]["ws_url"]
MARGIN_CCY = CFG["exchange"].get("margin_currency", "USDT")
API_KEY = _env_secret("COINDCX_API_KEY")
API_SECRET = _env_secret("COINDCX_API_SECRET")
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").strip().lower() in ("1", "true", "yes")
CMD_SECRET = (os.getenv("RUBAIH_API_TOKEN") or "").strip()
# Manual free-futures INR when CoinDCX wallet endpoints 404 (set to YOUR Futures wallet ₹)
try:
    FREE_CAPITAL_INR_ENV = float(os.getenv("RUBAIH_FREE_CAPITAL_INR") or os.getenv("FREE_CAPITAL_INR") or 0)
except (TypeError, ValueError):
    FREE_CAPITAL_INR_ENV = 0.0

# ==============================================================================
# MODELS
# ==============================================================================
class OptionType(Enum):
    CALL = "call"
    PUT = "put"

class Side(Enum):
    BUY = "buy"
    SELL = "sell"

@dataclass
class CoinDCXProduct:
    pair: str
    symbol: str
    underlying: str
    strike: float = 0.0
    expiry_ts: float = 0.0
    opt_type: Optional[OptionType] = None
    is_perp: bool = True
    contract_value: float = 1.0
    quantity_increment: float = 0.001
    min_quantity: float = 0.001
    max_leverage: float = 10.0
    price_increment: float = 0.01

@dataclass
class GreeksSnapshot:
    timestamp: float
    delta: float
    gamma: float
    vega: float
    theta: float
    vanna: float = 0.0
    volga: float = 0.0

@dataclass
class HedgeSignal:
    timestamp: float
    target_delta: float
    current_delta: float
    hedge_size: float
    urgency: str
    reason: str
    ai_augmented: bool = False
    pair: Optional[str] = None

@dataclass
class Position:
    symbol: str
    product_id: str
    side: str
    size: float
    entry_price: float
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0

# ==============================================================================
# COINDCX CLIENT
# ==============================================================================
# Cloudflare on api.coindcx.com blocks bare Python UA (error 1010). Look like a normal client.
_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://coindcx.com",
    "Referer": "https://coindcx.com/",
}


class CoinDCXAuth:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret.encode() if api_secret else b""

    def sign(self, body: dict):
        payload = json.dumps(body, separators=(",", ":"))
        signature = hmac.new(self.api_secret, payload.encode(), hashlib.sha256).hexdigest()
        headers = {
            **_HTTP_HEADERS,
            "Content-Type": "application/json",
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": signature,
        }
        return headers, payload

class CoinDCXClient:
    def __init__(self, auth: CoinDCXAuth):
        self.auth = auth
        self.session: Optional[aiohttp.ClientSession] = None
        self.margin = MARGIN_CCY
        self._auth_errors = 0
        self._auth_ok = False
        # CoinDCX docs disagree: code samples use ms, some tables say seconds
        self._ts_mode = "ms"  # "ms" | "s"

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=_HTTP_HEADERS)
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    def _timestamp(self) -> int:
        if self._ts_mode == "s":
            return int(time.time())
        return int(round(time.time() * 1000))

    async def _public_get(self, url: str, params: Optional[Dict] = None) -> dict:
        async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            text = await resp.text()
            if resp.status == 403 and "1010" in text:
                print(f"[API ERROR] Cloudflare blocked GET {url} (1010) — VPS IP or UA banned")
                return {}
            try:
                return json.loads(text) if text else {}
            except Exception:
                return {}

    async def _signed_request(self, method: str, path: str, body: Optional[Dict] = None):
        """Signed CoinDCX call. Some futures wallet endpoints are GET-with-body (per docs)."""
        extra = {k: v for k, v in dict(body or {}).items() if k != "timestamp"}
        body = {"timestamp": self._timestamp(), **extra}
        headers, payload = self.auth.sign(body)
        url = f"{REST_URL}{path}"
        method = (method or "POST").upper()
        timeout = aiohttp.ClientTimeout(total=15)
        if method == "GET":
            # Docs use GET with signed JSON body for wallets / cross_margin_details
            req = self.session.get(url, data=payload, headers=headers, timeout=timeout)
        else:
            req = self.session.post(url, data=payload, headers=headers, timeout=timeout)
        async with req as resp:
            text = await resp.text()
            try:
                data = json.loads(text) if text else {}
            except Exception:
                data = {"raw": text[:200]}
            if resp.status == 403 and ("1010" in text or "cloudflare" in text.lower()):
                self._auth_errors += 1
                if self._auth_errors == 1 or self._auth_errors % 30 == 0:
                    print(
                        f"[API ERROR] {path} Cloudflare 403/1010 x{self._auth_errors} — "
                        "VPS IP may be banned by CoinDCX/Cloudflare"
                    )
                return data
            if resp.status == 401:
                self._auth_ok = False
                self._auth_errors += 1
                if self._auth_errors == 1 or self._auth_errors % 60 == 0:
                    print(
                        f"[API ERROR] {path} (401 Invalid credentials) x{self._auth_errors} — "
                        "CoinDCX rejected key/secret. Confirm email activation, "
                        "IP whitelist matches this VPS, recreate engine after .env edit"
                    )
            elif resp.status >= 400:
                # Throttle noisy 404s on optional capital endpoints
                key = f"{method}:{path}:{resp.status}"
                counts = getattr(self, "_api_err_counts", {})
                counts[key] = counts.get(key, 0) + 1
                self._api_err_counts = counts
                if counts[key] <= 2 or counts[key] % 30 == 0:
                    print(f"[API ERROR] {method} {path} ({resp.status}): {data}")
            else:
                self._auth_errors = 0
                self._auth_ok = True
            return data

    async def _signed_post(self, path: str, body: Optional[Dict] = None):
        return await self._signed_request("POST", path, body)

    async def _signed_get(self, path: str, body: Optional[Dict] = None):
        return await self._signed_request("GET", path, body)

    async def verify_credentials(self) -> bool:
        """
        Probe auth. Prefer futures endpoints (what CoinDCX enables after approval email),
        then fall back to /users/info. Tries timestamp ms then seconds.
        """
        for mode in ("ms", "s"):
            self._ts_mode = mode

            # 1) Futures positions — success is often an empty list [] when flat
            pos = await self._signed_post(
                "/exchange/v1/derivatives/futures/positions",
                {
                    "page": "1",
                    "size": "10",
                    "margin_currency_short_name": [self.margin],
                },
            )
            if isinstance(pos, list):
                self._auth_ok = True
                print(
                    f"[AUTH] CoinDCX futures OK (timestamp={mode}, margin={self.margin}, "
                    f"positions={len(pos)})"
                )
                return True
            if isinstance(pos, dict) and pos.get("status") != "error" and pos.get("code") not in (401, "401"):
                if "data" in pos or "result" in pos:
                    self._auth_ok = True
                    print(f"[AUTH] CoinDCX futures OK (timestamp={mode}): keys={list(pos.keys())[:6]}")
                    return True
            print(f"[AUTH] futures/positions failed timestamp={mode}: {pos}")

            # 2) Spot-style users/info
            info = await self._signed_post("/exchange/v1/users/info", {})
            if isinstance(info, dict) and info.get("status") != "error" and info.get("code") not in (401, "401"):
                if info.get("coindcx_id") or info.get("email") or info.get("id") or "first_name" in info:
                    self._auth_ok = True
                    print(f"[AUTH] CoinDCX users/info OK (timestamp={mode})")
                    # Re-check futures now that auth is confirmed
                    pos2 = await self._signed_post(
                        "/exchange/v1/derivatives/futures/positions",
                        {
                            "page": "1",
                            "size": "10",
                            "margin_currency_short_name": [self.margin],
                        },
                    )
                    if isinstance(pos2, list):
                        print(f"[AUTH] futures/positions OK after users/info (n={len(pos2)})")
                    else:
                        print(f"[AUTH] WARN futures/positions still odd: {pos2}")
                    return True
            print(f"[AUTH] users/info failed timestamp={mode}: {info}")

        self._auth_ok = False
        self._ts_mode = "ms"
        print(
            "[AUTH] FAILED — CoinDCX Invalid credentials on futures endpoints.\n"
            "  Docs: https://docs.coindcx.com/#futures-end-points\n"
            "  After CoinDCX approval email: recreate key if needed, confirm IP whitelist,\n"
            "  then: docker compose up -d --force-recreate rubaih_engine"
        )
        return False

    async def get_active_instruments(self) -> List[str]:
        url = f"{REST_URL}/exchange/v1/derivatives/futures/data/active_instruments"
        data = await self._public_get(url, {"margin_currency_short_name[]": self.margin})
        if isinstance(data, list):
            return data
        return data.get("result", data.get("data", [])) if isinstance(data, dict) else []

    async def get_instrument(self, pair: str) -> Dict:
        url = f"{REST_URL}/exchange/v1/derivatives/futures/data/instrument"
        return await self._public_get(url, {"pair": pair, "margin_currency_short_name": self.margin})

    async def get_orderbook(self, pair: str) -> Dict:
        # Futures book (spot /market_data/orderbook is price→qty dicts too, but wrong market)
        url = f"{PUBLIC_URL}/market_data/v3/orderbook/{pair}-futures/20"
        data = await self._public_get(url)
        if isinstance(data, dict) and (data.get("bids") or data.get("asks")):
            return data
        # Fallback spot book for mid price if futures endpoint unavailable
        return await self._public_get(f"{PUBLIC_URL}/market_data/orderbook", {"pair": pair})

    async def get_positions(self, pairs: Optional[str] = None) -> List[Dict]:
        if not self._auth_ok:
            return []
        body = {
            "page": "1",
            "size": "100",
            "margin_currency_short_name": [self.margin],
        }
        if pairs:
            body["pairs"] = pairs
        data = await self._signed_post("/exchange/v1/derivatives/futures/positions", body)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            if data.get("code") in (401, "401") or data.get("status") == "error":
                return []
            return data.get("data", data.get("result", [])) or []
        return []

    async def place_order(
        self,
        pair: str,
        side: Side,
        size: float,
        order_type: str = "market",
        leverage: Optional[int] = None,
        take_profit_price: Optional[float] = None,
        stop_loss_price: Optional[float] = None,
        client_order_id: Optional[str] = None,
    ) -> Any:
        """
        Place futures market/limit order.
        NOTE: Never attach take_profit_price / stop_loss_price — CoinDCX INR-M returns
        422 "Please enter correct values for TP / SL". Bot manages TP/SL after fill.
        (Args kept for API compatibility but intentionally ignored.)

        Success payload is typically a *list* of order objects (id, status,
        remaining_quantity, avg_price, …) — not a bare dict.
        """
        order: Dict[str, Any] = {
            "side": side.value,
            "pair": pair,
            "order_type": "market_order" if order_type == "market" else "limit_order",
            "total_quantity": float(size),
            "notification": "no_notification",
            "hidden": False,
            "post_only": False,
            "margin_currency_short_name": self.margin,
        }
        if leverage is not None:
            order["leverage"] = int(leverage)
        if client_order_id:
            # Spot docs require this; futures accepts when present (idempotency key).
            order["client_order_id"] = str(client_order_id)
        # Intentionally do NOT send take_profit_price / stop_loss_price (INR-M 422).
        if order_type != "market":
            order["time_in_force"] = "good_till_cancel"

        return await self._signed_post(
            "/exchange/v1/derivatives/futures/orders/create",
            {"order": order},
        )

    async def list_futures_orders(
        self,
        status: str = "open,filled,cancelled,partially_filled,partially_cancelled",
        side: Optional[str] = None,
        page: int = 1,
        size: int = 50,
    ) -> List[Dict]:
        """List futures orders (used to poll fill state by order id)."""
        if not self._auth_ok:
            return []
        body: Dict[str, Any] = {
            "status": status,
            "page": str(page),
            "size": str(size),
            "margin_currency_short_name": [self.margin],
        }
        if side:
            body["side"] = side
        data = await self._signed_post("/exchange/v1/derivatives/futures/orders", body)
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            rows = data.get("data") or data.get("result") or data.get("orders") or []
            if isinstance(rows, list):
                return [x for x in rows if isinstance(x, dict)]
        return []

    async def get_order_by_id(self, order_id: str) -> Optional[Dict]:
        """Find one futures order by id across recent status buckets."""
        if not order_id:
            return None
        oid = str(order_id)
        rows = await self.list_futures_orders(
            status="open,filled,cancelled,partially_filled,partially_cancelled,initial",
            size=50,
        )
        for o in rows:
            if str(o.get("id") or "") == oid:
                return o
            if str(o.get("client_order_id") or "") == oid:
                return o
        return None

    @staticmethod
    def normalize_order_payload(result: Any) -> Dict:
        """Create may return a list[order] or wrapped dict / error dict."""
        if isinstance(result, list):
            for item in result:
                if isinstance(item, dict):
                    return item
            return {}
        if not isinstance(result, dict):
            return {}
        if result.get("status") == "error" or result.get("error"):
            return result
        for key in ("order", "data", "result"):
            nested = result.get(key)
            if isinstance(nested, dict):
                return nested
            if isinstance(nested, list) and nested and isinstance(nested[0], dict):
                return nested[0]
        orders = result.get("orders")
        if isinstance(orders, list) and orders and isinstance(orders[0], dict):
            return orders[0]
        return result

    @staticmethod
    def filled_qty_from_order(order: Dict, requested: float = 0.0) -> float:
        """Exchange-true filled size from total/remaining/cancelled."""
        if not isinstance(order, dict):
            return 0.0
        try:
            total = float(order.get("total_quantity") or requested or 0)
        except (TypeError, ValueError):
            total = float(requested or 0)
        try:
            rem = float(order.get("remaining_quantity") or 0)
        except (TypeError, ValueError):
            rem = 0.0
        try:
            canc = float(order.get("cancelled_quantity") or 0)
        except (TypeError, ValueError):
            canc = 0.0
        filled = total - rem - canc
        if filled < 0 and total > 0:
            filled = max(0.0, total - rem)
        st = str(order.get("status") or "").lower()
        if st == "filled" and filled <= 0 and total > 0:
            filled = total
        return max(0.0, filled)

    async def resolve_order_fill(
        self,
        order_id: str,
        requested_qty: float,
        timeout_sec: float = 12.0,
        poll_sec: float = 0.4,
        initial: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        Poll until order reaches a terminal-ish state; return filled qty + avg price.
        Never invent a full fill — returns filled_qty=0 on failure/timeout with no evidence.
        """
        terminal = {
            "filled",
            "cancelled",
            "rejected",
            "partially_cancelled",
        }
        last = dict(initial or {})
        deadline = time.time() + max(1.0, timeout_sec)
        while True:
            order: Optional[Dict] = None
            if order_id:
                found = await self.get_order_by_id(order_id)
                if found:
                    order = found
                    last = found
            elif last:
                order = last
            if order:
                st = str(order.get("status") or "").lower()
                filled = self.filled_qty_from_order(order, requested_qty)
                try:
                    avg = float(order.get("avg_price") or 0)
                except (TypeError, ValueError):
                    avg = 0.0
                fx = 0.0
                try:
                    fx = float(order.get("settlement_currency_conversion_price") or 0)
                except (TypeError, ValueError):
                    fx = 0.0
                if st == "filled" or (
                    filled + 1e-12 >= float(requested_qty or 0) > 0
                    and st not in ("open", "initial", "untriggered", "pending")
                ):
                    return {
                        "ok": filled > 0,
                        "filled_qty": filled if filled > 0 else float(requested_qty or 0),
                        "avg_price": avg,
                        "status": st,
                        "fx": fx,
                        "order": order,
                        "terminal": True,
                    }
                if st in terminal:
                    return {
                        "ok": filled > 0,
                        "filled_qty": filled,
                        "avg_price": avg,
                        "status": st,
                        "fx": fx,
                        "order": order,
                        "terminal": True,
                    }
                # partially_filled / open / initial — keep polling
                if filled > 0 and time.time() >= deadline:
                    return {
                        "ok": True,
                        "filled_qty": filled,
                        "avg_price": avg,
                        "status": st or "partial_timeout",
                        "fx": fx,
                        "order": order,
                        "terminal": False,
                    }
            if time.time() >= deadline:
                filled = self.filled_qty_from_order(last, requested_qty) if last else 0.0
                avg = 0.0
                fx = 0.0
                if last:
                    try:
                        avg = float(last.get("avg_price") or 0)
                    except (TypeError, ValueError):
                        avg = 0.0
                    try:
                        fx = float(last.get("settlement_currency_conversion_price") or 0)
                    except (TypeError, ValueError):
                        fx = 0.0
                return {
                    "ok": filled > 0,
                    "filled_qty": filled,
                    "avg_price": avg,
                    "status": str((last or {}).get("status") or "timeout"),
                    "fx": fx,
                    "order": last or {},
                    "terminal": False,
                }
            await asyncio.sleep(poll_sec)

    async def cancel_all_orders(self) -> Dict:
        return await self._signed_post(
            "/exchange/v1/derivatives/futures/positions/cancel_all_open_orders",
            {"margin_currency_short_name": [self.margin]},
        )

    async def get_cross_margin_details(self) -> Dict:
        """Live free futures margin (available_balance_cross)."""
        if not self._auth_ok:
            return {}
        # Docs disagree POST vs GET — try both; include margin currency for INR-M
        bodies = [
            {},
            {"margin_currency_short_name": self.margin},
            {"margin_currency_short_name": [self.margin]},
        ]
        for body in bodies:
            for method in ("GET", "POST"):
                data = await self._signed_request(
                    method,
                    "/exchange/v1/derivatives/futures/positions/cross_margin_details",
                    body,
                )
                if isinstance(data, dict) and data.get("available_balance_cross") is not None:
                    return data
                if isinstance(data, dict) and data.get("code") not in (404, "404", 400, "400"):
                    if any(k in data for k in ("available_balance_cross", "total_wallet_balance", "withdrawable_balance")):
                        return data
        return {}

    async def get_futures_wallets(self) -> List[Dict]:
        """Futures wallet balances."""
        if not self._auth_ok:
            return []
        for method in ("GET", "POST"):
            data = await self._signed_request(
                method, "/exchange/v1/derivatives/futures/wallets", {}
            )
            if isinstance(data, list) and data:
                return data
            if isinstance(data, dict):
                rows = data.get("data", data.get("result", []))
                if isinstance(rows, list) and rows:
                    return rows
        return []

    async def get_user_balances(self) -> List[Dict]:
        """Spot/user balances (fallback). Prefer futures wallet when available."""
        if not self._auth_ok:
            return []
        data = await self._signed_post("/exchange/v1/users/balances", {})
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("data", data.get("result", [])) or []
        return []

# ==============================================================================
# PRICING ENGINE
# ==============================================================================
class PricingEngine:
    def __init__(self, risk_free_rate: float = 0.05):
        self.r = risk_free_rate
        self._iv_cache: Dict[str, float] = {}
        self._surface_params: Dict[str, Dict] = {}

    def update_iv(self, symbol: str, iv: float):
        self._iv_cache[symbol] = iv

    def update_surface(self, underlying: str, atm_vol: float, skew: float, convexity: float):
        self._surface_params[underlying] = {"atm_vol": atm_vol, "skew": skew, "convexity": convexity}

    def _tte(self, expiry_ts: float) -> float:
        tte = (expiry_ts - time.time()) / (365.25 * 24 * 3600)
        return max(tte, 1.0 / 365.25 / 24)

    def _local_iv(self, underlying: str, strike: float, spot: float, tte: float) -> float:
        params = self._surface_params.get(underlying, {"atm_vol": 0.55, "skew": -0.15, "convexity": 0.08})
        moneyness = math.log(strike / spot) / math.sqrt(tte) if tte > 0 else 0.0
        iv = params["atm_vol"] + params["skew"] * moneyness + params["convexity"] * (moneyness ** 2)
        return max(iv, 0.01)

    def greeks(self, opt_type: OptionType, S: float, K: float, T: float, sigma: float) -> Dict[str, float]:
        if T <= 0:
            intrinsic = max(S - K, 0) if opt_type == OptionType.CALL else max(K - S, 0)
            delta = 1.0 if opt_type == OptionType.CALL and S > K else -1.0 if opt_type == OptionType.PUT and S < K else 0.0
            return {"delta": delta, "gamma": 0.0, "vega": 0.0, "theta": 0.0, "vanna": 0.0, "volga": 0.0, "price": intrinsic}

        sqrt_T = math.sqrt(T)
        d1 = (math.log(S / K) + (self.r + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
        d2 = d1 - sigma * sqrt_T
        nd1 = norm.pdf(d1)
        sign = 1.0 if opt_type == OptionType.CALL else -1.0
        delta = sign * norm.cdf(sign * d1)

        params = self._surface_params.get("BTC", {"skew": 0.0})
        skew_corr = params.get("skew", 0.0) * nd1 * sqrt_T * 0.5
        smile_delta = delta + skew_corr

        gamma = nd1 / (S * sigma * sqrt_T)
        vega = S * nd1 * sqrt_T / 100
        theta = (-(S * nd1 * sigma) / (2 * sqrt_T) - self.r * K * math.exp(-self.r * T) * norm.cdf(sign * d2) * sign) / 365.25
        vanna = -nd1 * d2 / sigma
        volga = S * nd1 * sqrt_T * d1 * d2 / sigma

        return {
            "delta": smile_delta, "gamma": gamma, "vega": vega,
            "theta": theta, "vanna": vanna, "volga": volga,
            "price": self._price(opt_type, S, K, T, sigma)
        }

    def _price(self, opt_type, S, K, T, sigma):
        if T <= 0:
            return max(S - K, 0) if opt_type == OptionType.CALL else max(K - S, 0)
        d1 = (math.log(S / K) + (self.r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        if opt_type == OptionType.CALL:
            return S * norm.cdf(d1) - K * math.exp(-self.r * T) * norm.cdf(d2)
        return K * math.exp(-self.r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

# ==============================================================================
# PORTFOLIO RISK ENGINE
# ==============================================================================
class PortfolioRiskEngine:
    def __init__(self, pricing: PricingEngine):
        self.pricing = pricing
        self.positions: Dict[str, Position] = {}
        self.spot_prices: Dict[str, float] = {}
        self.products: Dict[str, CoinDCXProduct] = {}

    def update_spot(self, underlying: str, price: float):
        self.spot_prices[underlying] = price

    def update_product(self, product: CoinDCXProduct):
        self.products[product.symbol] = product

    def update_positions(self, positions: List[Position]):
        self.positions = {p.symbol: p for p in positions}

    def compute_greeks(self) -> GreeksSnapshot:
        agg = {"delta": 0.0, "gamma": 0.0, "vega": 0.0, "theta": 0.0, "vanna": 0.0, "volga": 0.0}
        for pos in self.positions.values():
            prod = self.products.get(pos.symbol)
            if not prod:
                continue
            spot = self.spot_prices.get(prod.underlying, 0.0)
            if spot <= 0 and not prod.is_perp:
                continue
            if prod.is_perp:
                direction = 1.0 if pos.side == "buy" else -1.0
                agg["delta"] += pos.size * prod.contract_value * direction
            else:
                T = self.pricing._tte(prod.expiry_ts)
                sigma = self.pricing._local_iv(prod.underlying, prod.strike, spot, T)
                g = self.pricing.greeks(prod.opt_type, spot, prod.strike, T, sigma)
                direction = 1.0 if pos.side == "buy" else -1.0
                mult = prod.contract_value
                for key in agg:
                    agg[key] += g[key] * pos.size * mult * direction
        return GreeksSnapshot(timestamp=time.time(), **agg)

# ==============================================================================
# HEDGING STRATEGIST
# ==============================================================================
class HedgingStrategist:
    def __init__(self):
        cfg = CFG["trading"]
        self.delta_threshold = cfg["delta_threshold"]
        self.min_interval = cfg["min_hedge_interval_sec"]
        self.cost_mult = cfg["cost_buffer_multiplier"]
        self.taker_fee = CFG["exchange"]["taker_fee"]
        self.slippage_bps = 2.0
        self._last_hedge = 0.0
        self._last_delta = 0.0

    def evaluate(self, greeks: GreeksSnapshot, spot: float) -> Optional[HedgeSignal]:
        now = time.time()
        delta = greeks.delta
        if abs(delta) < self.delta_threshold:
            return None
        if now - self._last_hedge < self.min_interval:
            return None

        hedge_size = -delta
        notional = abs(hedge_size) * spot
        est_cost = notional * (self.taker_fee + self.slippage_bps / 10000)
        expected_move = spot * 0.015
        gamma_save = 0.5 * abs(greeks.gamma) * (expected_move ** 2)
        delta_drift = abs(delta - self._last_delta)
        if delta_drift < self.delta_threshold * 0.3:
            return None

        if gamma_save < est_cost * self.cost_mult and abs(greeks.gamma) > 1e-12:
            return HedgeSignal(now, 0.0, delta, 0.0, "none",
                f"cost_reject: save=${gamma_save:.2f} < cost=${est_cost:.2f}", False)

        urgency = "immediate" if abs(delta) > self.delta_threshold * 3 else "passive"
        self._last_hedge = now
        self._last_delta = delta
        return HedgeSignal(now, 0.0, delta, hedge_size, urgency,
            f"delta={delta:.4f}, cost=${est_cost:.2f}, save=${gamma_save:.2f}", False)


# ==============================================================================
# FUTURES CYCLE STRATEGIST (flat → scan → buy → sell)
# ==============================================================================
class FuturesCycleStrategist:
    """
    Momentum scanner + capital-fraction sizing + R-multiple exits.

    Size = 25–30% of *live free* futures margin (not fixed ₹).
    At buy: lock TP/SL prices. Trail in R (1R = stop distance).
    """

    def __init__(self):
        tcfg = CFG["trading"]
        scfg = tcfg.get("strategy") or {}
        self.entry_cooldown = float(scfg.get("entry_cooldown_sec", 150))
        self.lookback = int(scfg.get("momentum_lookback", 24))
        self.entry_move_pct = float(scfg.get("entry_move_pct", 0.0025))
        # TP and SL are fixed coin-price movements; ROE is derived for display.
        self.take_profit_price_pct = float(
            scfg.get("take_profit_price_pct", scfg.get("take_profit_pct", 0.014))
        )
        self.take_profit_roe = self.take_profit_price_pct * float(
            tcfg.get("leverage", 10)
        )  # derived for display
        self.stop_loss_price_pct = float(
            scfg.get("stop_loss_price_pct", scfg.get("stop_loss_pct", 0.007))
        )
        self.stop_loss_roe = self.stop_loss_price_pct * float(
            tcfg.get("leverage", 10)
        )
        self.take_profit_pct = self.take_profit_price_pct
        self.stop_loss_pct = self.stop_loss_price_pct
        # 0 = no time-based exit; otherwise flatten after max hold.
        self.max_hold_sec = float(scfg.get("max_hold_sec", 14400))
        self.allow_short = bool(scfg.get("allow_short", False))
        self.capital_inr = float(tcfg.get("capital_inr", 5000))  # fallback
        self.free_capital_inr = self.capital_inr  # refreshed from exchange
        self.margin_use_frac = float(tcfg.get("margin_use_frac", 0.25))
        self.margin_use_max_frac = float(tcfg.get("margin_use_max_frac", 0.30))
        self.leverage = int(tcfg.get("leverage", 10))
        self.usdt_inr = float(tcfg.get("usdt_inr", 87))
        self.taker_fee = float(CFG.get("exchange", {}).get("taker_fee", 0.00075))
        self.min_interval = float(tcfg.get("min_hedge_interval_sec", 45))
        self.max_loss_frac = float(scfg.get("max_loss_frac", 0.08))
        self.trail_arm_r = float(scfg.get("trail_arm_r", 0.35))
        self.trail_giveback_r = float(scfg.get("trail_giveback_r", 0.30))
        self.trail_giveback_of_peak = float(scfg.get("trail_giveback_of_peak", 0.25))
        self._last_signal = 0.0
        self._entry_ts = 0.0
        self._peak_pnl_inr = 0.0
        self._peak_price = 0.0
        self._entry_price = 0.0
        self._tp_price = 0.0
        self._sl_price = 0.0
        self._r_price = 0.0
        self._r_inr = 0.0
        self._trade_leverage = float(self.leverage)
        self._margin_used = 0.0
        self._plan_pair: Optional[str] = None
        self._plan_size = 0.0
        self._prices: Dict[str, Deque[float]] = {}
        self._last_scan_log = 0.0
        self._last_entry_pair: Optional[str] = None
        self._last_scan_msg: Optional[str] = None
        self._last_hold_log = 0.0

    def set_free_capital(self, free_inr: float):
        if free_inr and free_inr > 0:
            self.free_capital_inr = float(free_inr)

    def trade_margin_budget(self) -> float:
        free = max(self.free_capital_inr, 0.0) or max(self.capital_inr, 0.0)
        lo = max(0.05, min(self.margin_use_frac, self.margin_use_max_frac))
        hi = max(lo, min(0.95, self.margin_use_max_frac))
        use = min(max(self.margin_use_frac, lo), hi)
        return free * use

    def _buf(self, pair: str) -> Deque[float]:
        if pair not in self._prices:
            self._prices[pair] = deque(maxlen=max(self.lookback + 5, 40))
        return self._prices[pair]

    def push_price(self, mid: float, pair: Optional[str] = None):
        if mid and mid > 0:
            key = pair or CFG["trading"]["perp_symbol"]
            self._buf(key).append(float(mid))

    def affordable_qty(
        self,
        spot: float,
        min_qty: float,
        leverage: Optional[float] = None,
        step: Optional[float] = None,
    ) -> float:
        """Size off free capital within margin_use_max_frac. Never all-in for min lot."""
        if spot <= 0 or self.usdt_inr <= 0:
            return 0.0
        lev = float(leverage if leverage is not None else self.leverage)
        if lev <= 0:
            return 0.0
        free = max(self.free_capital_inr, self.capital_inr, 0.0)
        if free <= 0:
            return 0.0
        max_margin = min(free, free * self.margin_use_max_frac)
        budget = self.trade_margin_budget()
        qty = (budget * lev) / (spot * self.usdt_inr) if budget > 0 else 0.0
        min_q = max(float(min_qty or 0.0), 0.0)
        min_margin = (min_q * spot * self.usdt_inr) / lev if min_q > 0 else 0.0

        def _can_take_min() -> bool:
            # Min lot allowed only if it fits inside the hard margin cap (not 100% free).
            return min_q > 0 and min_margin <= max_margin * 1.001

        # Below exchange min lot: take min only if within max_frac×free
        if min_q > 0 and qty + 1e-12 < min_q:
            if _can_take_min():
                qty = min_q
            else:
                return 0.0
        else:
            margin = (qty * spot * self.usdt_inr) / lev
            if margin > max_margin * 1.01:
                qty = (max_margin * lev) / (spot * self.usdt_inr)
            if min_q > 0 and qty + 1e-12 < min_q:
                if _can_take_min():
                    qty = min_q
                else:
                    return 0.0

        # Floor to lot step without collapsing a valid min lot
        st = float(step or 0.0)
        if st > 0 and qty > 0:
            floored = math.floor(qty / st + 1e-12) * st
            decimals = max(0, min(8, int(round(-math.log10(st))) if st < 1 else 0))
            floored = round(floored, decimals)
            if floored + 1e-12 < min_q:
                if _can_take_min():
                    qty = min_q
                else:
                    return 0.0
            else:
                qty = floored
        return float(qty)

    def margin_for(self, spot: float, qty: float, leverage: Optional[float] = None) -> float:
        lev = float(leverage if leverage is not None else self.leverage)
        if spot <= 0 or qty <= 0 or lev <= 0:
            return 0.0
        return (qty * spot * self.usdt_inr) / lev

    def _move_pct(self, pair: str, spot: float) -> Optional[float]:
        hist = list(self._buf(pair))
        if len(hist) < max(5, self.lookback // 2):
            return None
        look = hist[-self.lookback:] if len(hist) >= self.lookback else hist
        base = look[0]
        if base <= 0:
            return None
        return (spot - base) / base

    def pnl_inr(self, entry: float, spot: float, size: float) -> float:
        return (spot - entry) * size * self.usdt_inr

    def price_pct_from_roe(self, roe: float, leverage: Optional[float] = None) -> float:
        """CoinDCX ROE% ≈ price_move% × leverage → price_move = ROE / lev."""
        lev = float(leverage if leverage is not None else self.leverage)
        lev = max(lev, 1.0)
        return max(0.0005, float(roe) / lev)

    def arm_trade(self, pair: str, entry: float, size: float, leverage: Optional[float] = None):
        if entry <= 0 or size <= 0:
            return
        lev = float(leverage if leverage is not None else self.leverage)
        lev = max(lev, 1.0)
        self._trade_leverage = lev
        # Fixed price TP/SL across pairs; displayed ROE varies by leverage.
        self.take_profit_pct = self.take_profit_price_pct
        self.take_profit_roe = self.take_profit_pct * lev
        self.stop_loss_pct = self.stop_loss_price_pct
        self.stop_loss_roe = self.stop_loss_pct * lev
        self._plan_pair = pair
        self._plan_size = size
        self._entry_price = entry
        self._entry_ts = time.time()
        self._tp_price = entry * (1.0 + self.take_profit_pct)
        self._sl_price = entry * (1.0 - self.stop_loss_pct)
        self._r_price = entry * self.stop_loss_pct
        self._r_inr = abs(self.pnl_inr(entry, entry - self._r_price, size))
        self._margin_used = (size * entry * self.usdt_inr) / lev
        self._peak_price = entry
        self._peak_pnl_inr = 0.0
        self._last_entry_pair = pair
        free = max(self.free_capital_inr, self.capital_inr, 1.0)
        print(
            f"[PLAN] {pair} entry={entry:.4f} size={size} margin~₹{self._margin_used:.0f} "
            f"({100 * self._margin_used / free:.0f}% of free ₹{free:.0f}) @{lev:.0f}x | "
            f"TP price=+{self.take_profit_pct:.2%} (ROE≈+{self.take_profit_roe:.0%}) → {self._tp_price:.4f} | "
            f"SL price=-{self.stop_loss_pct:.2%} (ROE≈-{self.stop_loss_roe:.0%}) → {self._sl_price:.4f} | "
            f"1R=₹{self._r_inr:.0f} trail={self.trail_arm_r}R→{self.trail_giveback_r}R "
            f"max_loss={self.max_loss_frac:.0%} margin"
        )

    def refresh_exit_levels(self):
        """Recompute TP/SL/R from current pcts without resetting peak/entry."""
        entry = float(self._entry_price or 0)
        size = float(self._plan_size or 0)
        if entry <= 0 or size <= 0 or not self._plan_pair:
            return
        lev = float(self._trade_leverage or self.leverage or 10)
        lev = max(lev, 1.0)
        self.take_profit_pct = self.take_profit_price_pct
        self.stop_loss_pct = self.stop_loss_price_pct
        self.take_profit_roe = self.take_profit_pct * lev
        self.stop_loss_roe = self.stop_loss_pct * lev
        self._tp_price = entry * (1.0 + self.take_profit_pct)
        self._sl_price = entry * (1.0 - self.stop_loss_pct)
        self._r_price = entry * self.stop_loss_pct
        self._r_inr = abs(self.pnl_inr(entry, entry - self._r_price, size))
        self._margin_used = (size * entry * self.usdt_inr) / lev
        print(
            f"[PLAN] refreshed exits {self._plan_pair} entry={entry:.4f} "
            f"TP=+{self.take_profit_pct:.2%}→{self._tp_price:.4f} "
            f"SL=-{self.stop_loss_pct:.2%}→{self._sl_price:.4f} "
            f"1R=₹{self._r_inr:.0f} trail={self.trail_arm_r}R"
        )

    def clear_trade(self):
        self._plan_pair = None
        self._plan_size = 0.0
        self._entry_price = 0.0
        self._tp_price = 0.0
        self._sl_price = 0.0
        self._r_price = 0.0
        self._r_inr = 0.0
        self._margin_used = 0.0
        self._peak_price = 0.0
        self._peak_pnl_inr = 0.0
        self._entry_ts = 0.0

    def trade_plan_dict(self) -> Dict:
        return {
            "pair": self._plan_pair,
            "entry": self._entry_price,
            "tp": self._tp_price,
            "sl": self._sl_price,
            "r_price": self._r_price,
            "r_inr": self._r_inr,
            "margin_used": self._margin_used,
            "peak_pnl_inr": self._peak_pnl_inr,
            "peak_price": self._peak_price,
            "size": self._plan_size,
            "entry_ts": self._entry_ts,
        }

    def restore_trade_plan(self, data: Dict):
        if not data:
            return
        self._plan_pair = data.get("pair")
        self._entry_price = float(data.get("entry") or 0)
        self._tp_price = float(data.get("tp") or 0)
        self._sl_price = float(data.get("sl") or 0)
        self._r_price = float(data.get("r_price") or 0)
        self._r_inr = float(data.get("r_inr") or 0)
        self._margin_used = float(data.get("margin_used") or 0)
        self._peak_pnl_inr = float(data.get("peak_pnl_inr") or 0)
        self._peak_price = float(data.get("peak_price") or self._entry_price or 0)
        self._plan_size = float(data.get("size") or 0)
        self._entry_ts = float(data.get("entry_ts") or 0)

    def pick_entry(
        self,
        pair_mids: Dict[str, float],
        min_qty_by_pair: Dict[str, float],
        leverage_by_pair: Optional[Dict[str, float]] = None,
        step_by_pair: Optional[Dict[str, float]] = None,
    ) -> Optional[HedgeSignal]:
        now = time.time()
        if now - self._last_signal < max(self.min_interval, self.entry_cooldown):
            return None
        ranked = []
        skipped_size = skipped_move = warming = 0
        budget = self.trade_margin_budget()
        free = max(self.free_capital_inr, self.capital_inr, 0.0)
        lev_map = leverage_by_pair or {}
        step_map = step_by_pair or {}
        for pair, spot in pair_mids.items():
            if spot <= 0:
                continue
            move = self._move_pct(pair, spot)
            if move is None:
                warming += 1
                continue
            if move < self.entry_move_pct:
                skipped_move += 1
                continue
            min_q = float(min_qty_by_pair.get(pair, 0.001) or 0.001)
            lev = float(lev_map.get(pair, self.leverage) or self.leverage)
            step = float(step_map.get(pair, 0.0) or 0.0)
            qty = self.affordable_qty(spot, min_q, leverage=lev, step=step or None)
            if qty <= 0:
                skipped_size += 1
                continue
            margin_inr = self.margin_for(spot, qty, lev)
            # Prefer strong move; slight bias to pairs that fit cleanly in budget (small capital)
            fit = 1.0 if margin_inr <= budget * 1.05 else 0.92
            score = move * fit
            if pair == self._last_entry_pair:
                score *= 0.85
            # Until equity is larger, de-prefer BTC min-lot vs ETH/SOL/BNB
            if free < 15000 and "BTC" in pair.upper():
                score *= 0.88
            ranked.append((score, move, pair, spot, qty, margin_inr, lev))

        if now - self._last_scan_log > 30:
            self._last_scan_log = now
            top = ", ".join(
                f"{p}:{m:.2%}~₹{mar:.0f}@{int(lv)}x"
                for _s, m, p, _sp, _q, mar, lv in sorted(ranked, reverse=True)[:5]
            ) or "none"
            self._last_scan_msg = (
                f"[SCAN] free=₹{free:.0f} budget=₹{budget:.0f} "
                f"({self.margin_use_frac:.0%}–{self.margin_use_max_frac:.0%}) "
                f"cand={len(pair_mids)} qual={len(ranked)} warm={warming} "
                f"flat={skipped_move} skip_size={skipped_size} top=[{top}]"
            )
            print(self._last_scan_msg)

        if not ranked:
            return None
        ranked.sort(reverse=True)
        _score, move, pair, spot, qty, margin_inr, lev = ranked[0]
        notional_inr = qty * spot * self.usdt_inr
        fee_inr = notional_inr * self.taker_fee * 2
        tp_pct = self.take_profit_price_pct
        tp_inr = notional_inr * tp_pct
        if tp_inr < fee_inr * 1.15:
            # Try next affordable candidate instead of burning the whole scan
            for cand in ranked[1:]:
                _s, move, pair, spot, qty, margin_inr, lev = cand
                notional_inr = qty * spot * self.usdt_inr
                fee_inr = notional_inr * self.taker_fee * 2
                tp_pct = self.take_profit_price_pct
                tp_inr = notional_inr * tp_pct
                if tp_inr >= fee_inr * 1.15:
                    break
            else:
                print(f"[SCAN] skip all: TP thin vs fees at free=₹{free:.0f}")
                return None
        self._last_signal = now
        return HedgeSignal(
            now, qty, 0.0, qty, "passive",
            f"ENTRY_LONG: {pair} move={move:.2%} size={qty:.6f} "
            f"margin~₹{margin_inr:.0f}/{free:.0f} free @{int(lev)}x "
            f"TP_PRICE=+{self.take_profit_price_pct:.2%} "
            f"(ROE≈+{self.take_profit_price_pct * lev:.0%}) SL_ROE=-{self.stop_loss_roe:.0%}",
            False, pair=pair,
        )

    def evaluate_exit(self, spot: float, position: Optional[Position], pair: str) -> Optional[HedgeSignal]:
        now = time.time()
        if spot <= 0 or not position or position.size <= 0:
            return None
        side = (position.side or "buy").lower()
        entry = position.entry_price or self._entry_price or 0.0
        size = position.size
        if side != "buy" or entry <= 0:
            if side == "sell":
                self._last_signal = now
                return HedgeSignal(
                    now, 0.0, -size, size, "immediate",
                    f"EXIT_FLATTEN_SHORT: {pair} size={size}", False, pair=pair,
                )
            return None

        if self._tp_price <= 0 or self._sl_price <= 0 or self._plan_pair != pair:
            self.arm_trade(pair, entry, size, leverage=self._trade_leverage or self.leverage)
        if self._entry_ts <= 0:
            self._entry_ts = now
        if self._r_inr <= 0 or self._r_price <= 0:
            self._r_price = entry * self.stop_loss_pct
            self._r_inr = abs(self.pnl_inr(entry, entry - self._r_price, size))
        if self._margin_used <= 0:
            self._margin_used = (size * entry * self.usdt_inr) / max(self.leverage, 1)

        pnl = self.pnl_inr(entry, spot, size)
        if position.unrealized_pnl:
            exch = float(position.unrealized_pnl)
            # unrealized_pnl is INR (sync/publish normalize) — use exchange mark when present
            if abs(exch) >= 1:
                pnl = exch

        self._peak_pnl_inr = max(self._peak_pnl_inr, pnl)
        self._peak_price = max(self._peak_price, spot)
        giveback = self._peak_pnl_inr - pnl
        price_drop = self._peak_price - spot

        max_loss = self._margin_used * self.max_loss_frac
        arm_inr = self._r_inr * self.trail_arm_r
        round_trip_fee = entry * size * self.usdt_inr * self.taker_fee * 2.0
        trail_armed = self._peak_pnl_inr >= arm_inr
        giveback_need = max(
            self._r_inr * self.trail_giveback_r,
            self._peak_pnl_inr * self.trail_giveback_of_peak if self._peak_pnl_inr > 0 else 0.0,
        )
        price_giveback_need = self._r_price * self.trail_giveback_r

        if now - self._last_hold_log > 8:
            self._last_hold_log = now
            print(
                f"[HOLD] {pair} spot={spot:.4f} pnl=₹{pnl:.0f} peak=₹{self._peak_pnl_inr:.0f} "
                f"giveback=₹{giveback:.0f}/{giveback_need:.0f} "
                f"TP={self._tp_price:.4f} SL={self._sl_price:.4f} "
                f"maxloss=₹{max_loss:.0f} 1R=₹{self._r_inr:.0f} "
                f"fees~₹{round_trip_fee:.0f} trail={'ON' if trail_armed else 'off'}"
            )

        def _exit(reason: str) -> HedgeSignal:
            # Keep plan until fill confirms — failed exits must not wipe TP/SL/trail peak
            self._last_signal = now
            return HedgeSignal(now, 0.0, size, -size, "immediate", reason, False, pair=pair)

        if spot >= self._tp_price:
            return _exit(f"EXIT_TP: {pair} spot>={self._tp_price:.4f} pnl=₹{pnl:.0f}")
        if spot <= self._sl_price:
            return _exit(f"EXIT_SL: {pair} spot<={self._sl_price:.4f} pnl=₹{pnl:.0f}")
        if pnl <= -abs(max_loss):
            return _exit(f"EXIT_MAXLOSS: {pair} pnl=₹{pnl:.0f} limit=-₹{max_loss:.0f}")
        if (
            self._peak_pnl_inr >= arm_inr
            and pnl >= round_trip_fee
            and (giveback >= giveback_need or price_drop >= price_giveback_need)
        ):
            return _exit(
                f"EXIT_TRAIL: {pair} peak=₹{self._peak_pnl_inr:.0f} now=₹{pnl:.0f} "
                f"giveback=₹{giveback:.0f} need=₹{giveback_need:.0f}"
            )
        pnl_pct = (spot - entry) / entry
        if pnl_pct >= self.take_profit_pct:
            return _exit(f"EXIT_TP_PCT: {pair} +{pnl_pct:.2%} pnl=₹{pnl:.0f}")
        if pnl_pct <= -self.stop_loss_pct:
            return _exit(f"EXIT_SL_PCT: {pair} {pnl_pct:.2%} pnl=₹{pnl:.0f}")
        if self.max_hold_sec > 0 and self._entry_ts > 0 and (now - self._entry_ts) >= self.max_hold_sec:
            held = now - self._entry_ts
            return _exit(
                f"EXIT_TIME: {pair} held={held:.0f}s>=max={self.max_hold_sec:.0f}s pnl=₹{pnl:.0f}"
            )
        return None

    def evaluate(self, spot: float, position: Optional[Position], min_qty: float, pair: Optional[str] = None) -> Optional[HedgeSignal]:
        pair = pair or CFG["trading"]["perp_symbol"]
        if position and position.size > 0:
            return self.evaluate_exit(spot, position, pair)
        self.clear_trade()
        return self.pick_entry({pair: spot}, {pair: min_qty})


class RiskManager:
    def __init__(self):
        cfg = CFG["trading"]
        self.max_delta = cfg["max_delta"]
        self.max_vega = cfg["max_vega"]
        self.max_dd = cfg["max_drawdown_pct"]
        self.capital_base = float(cfg.get("capital_inr", 1000) or 1000)
        self._hwm = 0.0
        self._kill = False
        self._order_ts: List[float] = []

    def check(
        self,
        greeks: GreeksSnapshot,
        pnl: float,
        capital_base: Optional[float] = None,
    ) -> Optional[str]:
        if self._kill:
            return "KILL_SWITCH_ACTIVE"
        self._hwm = max(self._hwm, pnl)
        dd = self._hwm - pnl
        # Drawdown is a fraction of account capital, not a fraction of a tiny
        # unrealized-PnL high-water mark. The old denominator made a move from
        # +₹1 to -₹8 look like an 900% drawdown and forced an emergency exit.
        base = max(float(capital_base or self.capital_base or 0), 1.0)
        dd_pct = dd / base
        if dd_pct > self.max_dd:
            self._kill = True
            return (
                f"MAX_DRAWDOWN: ₹{dd:.0f}/{base:.0f}={dd_pct:.1%} "
                f"(limit {self.max_dd:.1%})"
            )
        if abs(greeks.delta) > self.max_delta:
            self._kill = True
            return f"MAX_DELTA: {greeks.delta:.4f}"
        if abs(greeks.vega) > self.max_vega:
            self._kill = True
            return f"MAX_VEGA: {greeks.vega:.2f}"
        return None

    def rate_limit_ok(self) -> bool:
        now = time.time()
        self._order_ts = [t for t in self._order_ts if now - t < 1.0]
        if len(self._order_ts) >= CFG["risk"]["rate_limit_orders_per_sec"]:
            return False
        self._order_ts.append(now)
        return True

    @property
    def alive(self) -> bool:
        return not self._kill

    def trigger_kill(self, reason: str):
        self._kill = True
        print(f"[RISK] KILL SWITCH: {reason}")

# ==============================================================================
# DATA STORE
# ==============================================================================
class DataStore:
    def __init__(self):
        self.pg: Optional[asyncpg.Pool] = None
        self.rd: Optional[redis.Redis] = None

    async def connect(self):
        db_password = os.getenv("DB_PASSWORD", "")
        if not db_password:
            raise RuntimeError("DB_PASSWORD environment variable is required")
        self.pg = await asyncpg.create_pool(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME", "rubaih"),
            user=os.getenv("DB_USER", "rubaih"),
            password=db_password,
        )
        self.rd = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            decode_responses=True,
            lib_name=None,
            lib_version=None,
            socket_connect_timeout=5,
            socket_timeout=5,
            socket_keepalive=True,
            health_check_interval=30,
            retry_on_timeout=True,
        )
        await self.rd.ping()
        await self._init_tables()

    async def _init_tables(self):
        await self.pg.execute("""
            CREATE TABLE IF NOT EXISTS greeks_snapshots (
                id SERIAL PRIMARY KEY, timestamp TIMESTAMPTZ DEFAULT NOW(),
                delta NUMERIC, gamma NUMERIC, vega NUMERIC, theta NUMERIC, spot_price NUMERIC
            );
            CREATE TABLE IF NOT EXISTS hedge_trades (
                id SERIAL PRIMARY KEY, timestamp TIMESTAMPTZ DEFAULT NOW(),
                symbol TEXT, side TEXT, size NUMERIC, price NUMERIC, reason TEXT, ai_augmented BOOLEAN DEFAULT FALSE
            );
            CREATE TABLE IF NOT EXISTS risk_events (
                id SERIAL PRIMARY KEY, timestamp TIMESTAMPTZ DEFAULT NOW(),
                event_type TEXT, details TEXT
            );
            CREATE TABLE IF NOT EXISTS ai_decisions (
                id SERIAL PRIMARY KEY, timestamp TIMESTAMPTZ DEFAULT NOW(),
                model TEXT, action TEXT, confidence NUMERIC, reasoning TEXT,
                risk_assessment TEXT, portfolio_delta NUMERIC
            );
        """)

    async def save_greeks(
        self,
        g: GreeksSnapshot,
        spot: float,
        session_pnl: float = 0.0,
        active_pair: Optional[str] = None,
        position_size: float = 0.0,
        position_side: str = "flat",
    ):
        await self.pg.execute(
            "INSERT INTO greeks_snapshots (delta, gamma, vega, theta, spot_price) VALUES ($1,$2,$3,$4,$5)",
            g.delta, g.gamma, g.vega, g.theta, spot
        )
        await self.rd.set("rubaih:session_pnl", str(session_pnl))
        pair = active_pair or CFG["trading"]["perp_symbol"]
        await self.rd.set("rubaih:active_pair", pair)
        await self.rd.set("rubaih:position_size", str(position_size))
        await self.rd.set("rubaih:position_side", position_side)
        await self.rd.publish("rubaih:greeks", json.dumps({
            "timestamp": g.timestamp, "delta": g.delta, "gamma": g.gamma,
            "vega": g.vega, "theta": g.theta, "spot": spot, "session_pnl": session_pnl,
            "active_pair": pair,
            "position_size": position_size,
            "position_side": position_side,
        }))

    async def set_engine_status(self, status: str):
        await self.rd.set("rubaih:engine_status", status)
        await self.rd.publish("rubaih:status", json.dumps({"status": status, "ts": time.time()}))

    async def push_log(self, line: str):
        """Append a live log line for the mobile Logs tab."""
        payload = {"ts": time.time(), "line": line}
        try:
            await self.rd.lpush("rubaih:logs", json.dumps(payload))
            await self.rd.ltrim("rubaih:logs", 0, 199)
            await self.rd.publish("rubaih:log", json.dumps(payload))
        except Exception:
            pass

    async def publish_scan(self, rows: List[Dict]):
        """Publish scanner table (pair, mid, move%) for Coins tab."""
        payload = {"ts": time.time(), "pairs": rows}
        try:
            await self.rd.set("rubaih:scan", json.dumps(payload))
            await self.rd.publish("rubaih:scan", json.dumps(payload))
        except Exception:
            pass

    async def save_capital_ledger(self, free_inr: float, source: str, locked_margin: float = 0.0):
        """Persist free futures capital so it survives restarts and updates after each trade."""
        try:
            payload = {
                "free_inr": float(free_inr),
                "locked_margin": float(locked_margin),
                "source": source,
                "ts": time.time(),
            }
            await self.rd.set("rubaih:capital_ledger", json.dumps(payload))
            await self.rd.hset(
                "rubaih:settings",
                mapping={
                    "free_capital_inr": f"{free_inr:.2f}",
                    "trade_budget_inr": f"{max(0.0, free_inr) * float(CFG['trading'].get('margin_use_frac', 0.25)):.2f}",
                    "capital_source": source,
                    "locked_margin_inr": f"{locked_margin:.2f}",
                },
            )
        except Exception:
            pass

    async def load_capital_ledger(self) -> Dict:
        try:
            raw = await self.rd.get("rubaih:capital_ledger")
            if raw:
                return json.loads(raw)
            # legacy: settings hash only
            s = await self.rd.hgetall("rubaih:settings") or {}
            free = float(s.get("free_capital_inr") or 0)
            if free > 0:
                return {
                    "free_inr": free,
                    "locked_margin": float(s.get("locked_margin_inr") or 0),
                    "source": s.get("capital_source") or "redis_settings",
                    "ts": 0,
                }
        except Exception:
            pass
        return {}

    async def save_trade_plan(self, plan: Dict):
        try:
            if plan and plan.get("pair"):
                await self.rd.set("rubaih:trade_plan", json.dumps(plan))
            else:
                await self.rd.delete("rubaih:trade_plan")
        except Exception:
            pass

    async def load_trade_plan(self) -> Dict:
        try:
            raw = await self.rd.get("rubaih:trade_plan")
            return json.loads(raw) if raw else {}
        except Exception:
            return {}

    async def save_hedge(self, signal: HedgeSignal, price: float, size: Optional[float] = None):
        side = "buy" if signal.hedge_size > 0 else "sell"
        qty = abs(size if size is not None else signal.hedge_size)
        await self.pg.execute(
            "INSERT INTO hedge_trades (symbol, side, size, price, reason, ai_augmented) VALUES ($1,$2,$3,$4,$5,$6)",
            signal.pair or CFG["trading"]["perp_symbol"], side, qty, price, signal.reason, signal.ai_augmented
        )

    async def save_risk_event(self, event_type: str, details: str):
        await self.pg.execute(
            "INSERT INTO risk_events (event_type, details) VALUES ($1, $2)", event_type, details
        )
        await self.rd.publish("rubaih:risk", json.dumps({"type": event_type, "details": details}))

    async def save_ai_decision(self, decision: AIDecision, portfolio_delta: float):
        await self.pg.execute(
            "INSERT INTO ai_decisions (model, action, confidence, reasoning, risk_assessment, portfolio_delta) VALUES ($1,$2,$3,$4,$5,$6)",
            decision.model_used, decision.action, decision.confidence, decision.reasoning, decision.risk_assessment, portfolio_delta
        )
        await self.rd.publish("rubaih:ai", json.dumps({
            "model": decision.model_used, "action": decision.action,
            "confidence": decision.confidence, "reasoning": decision.reasoning,
            "risk": decision.risk_assessment, "delta": portfolio_delta
        }))

    async def get_recent_hedges(self, limit: int = 10) -> List[Dict]:
        rows = await self.pg.fetch(
            "SELECT * FROM hedge_trades ORDER BY timestamp DESC LIMIT $1", limit
        )
        return [{"timestamp": str(r["timestamp"]), "side": r["side"], "size": float(r["size"]), "reason": r["reason"]} for r in rows]

# ==============================================================================
# RUBAIH BOT ORCHESTRATOR
# ==============================================================================
class RubaihBot:
    def __init__(self):
        self.auth = CoinDCXAuth(API_KEY, API_SECRET)
        self.pricing = PricingEngine()
        self.portfolio = PortfolioRiskEngine(self.pricing)
        self.strategist = HedgingStrategist()
        self.cycle = FuturesCycleStrategist()
        self.risk = RiskManager()
        self.store = DataStore()
        self.ai = OpenRouterAI()
        self.client: Optional[CoinDCXClient] = None
        self.products: Dict[str, CoinDCXProduct] = {}
        self._running = False
        self._ai_enabled = ai_configured()
        self._hedge_history: List[Dict] = []
        self._leverage = int(CFG["trading"].get("leverage", 10))
        self._live = LIVE_TRADING
        self._mode = str(CFG["trading"].get("mode", "futures_cycle")).strip().lower()
        self._dry_pos: Optional[Position] = None
        self._scan_enabled = bool(CFG["trading"].get("scan_enabled", True))
        self._scan_pairs: List[str] = list(CFG["trading"].get("scan_pairs") or [CFG["trading"]["perp_symbol"]])
        self._scan_max = int(CFG["trading"].get("scan_max_pairs", 20))
        self._scan_interval = float(CFG["trading"].get("scan_interval_sec", 5))
        self._active_pair = CFG["trading"]["perp_symbol"]
        self._pair_mids: Dict[str, float] = {}
        self._ws_pair = self._active_pair
        self._last_fill_ts = 0.0
        self._last_flatten_ts = 0.0
        self._capital_live_ok = False
        self._margin_locked = 0.0
        self._capital_source = "unset"
        self._ledger_seeded = False
        # Serializes all mutations of _dry_pos + free/locked capital across
        # main_loop, sync_positions, fills, and ledger adjustments.
        self._state_lock = asyncio.Lock()
        # Live fill accepted but exchange inventory not yet confirmed.
        self._pending_settle = False
        # Consecutive exchange-flat readings required before ghost-close credit.
        self._flat_confirm_count = 0
        self._flat_confirms_needed = 2
        # Do not treat exchange as flat (or credit capital) inside this window
        # after a local fill — CoinDCX position feeds lag under load.
        self._settle_grace_sec = 30.0
        # Flatten order failed or unconfirmed — position remains active risk.
        self._active_risk = False

    def _leverage_for(self, pair: Optional[str] = None) -> int:
        """Config leverage capped by instrument max (e.g. SOL often 5x)."""
        cfg_lev = max(1, int(self._leverage or 1))
        prod = self.products.get(pair or "") if pair else None
        if prod and getattr(prod, "max_leverage", 0):
            return max(1, min(cfg_lev, int(float(prod.max_leverage))))
        return cfg_lev

    def _pnl_inr(self, entry: float, spot: float, size: float, side: str = "buy") -> float:
        if entry <= 0 or spot <= 0 or size <= 0:
            return 0.0
        direction = 1.0 if (side or "buy").lower() in ("buy", "long") else -1.0
        return (spot - entry) * size * direction * float(self.cycle.usdt_inr)

    async def _log(self, line: str):
        print(line)
        try:
            await self.store.push_log(line)
        except Exception:
            pass

    def _scan_rows(self) -> List[Dict]:
        rows = []
        for pair in self._scan_pairs:
            mid = self._pair_mids.get(pair, 0.0)
            move = self.cycle._move_pct(pair, mid) if mid > 0 else None
            base = pair.replace("B-", "").replace("I-", "").split("_")[0]
            rows.append({
                "pair": pair,
                "base": base,
                "mid": mid,
                "move_pct": None if move is None else round(move * 100, 3),
                "active": pair == self._active_pair,
            })
        rows.sort(key=lambda r: (r["move_pct"] is None, -(r["move_pct"] or -999)))
        return rows

    def _round_qty(self, size: float, product: CoinDCXProduct) -> float:
        step = product.quantity_increment or 0.001
        if step <= 0:
            return size
        rounded = math.floor(size / step) * step
        # avoid float dust
        decimals = max(0, min(8, int(round(-math.log10(step))) if step < 1 else 0))
        return round(rounded, decimals)

    def _round_price(self, price: float, product: Optional[CoinDCXProduct]) -> float:
        """Round to instrument price_increment (CoinDCX: price must be divisible by tick)."""
        if price <= 0:
            return 0.0
        step = float(getattr(product, "price_increment", 0) or 0.01)
        if step <= 0:
            return round(price, 2)
        ticks = round(price / step)
        out = ticks * step
        decimals = max(0, min(8, int(round(-math.log10(step))) if step < 1 else 0))
        return round(out, decimals)

    def _exchange_tpsl_prices(self, spot: float, product: CoinDCXProduct) -> Tuple[float, float]:
        tp = self._round_price(spot * (1.0 + float(self.cycle.take_profit_pct)), product)
        sl = self._round_price(spot * (1.0 - float(self.cycle.stop_loss_pct)), product)
        # Ensure strict long ordering after rounding
        if tp <= spot:
            tp = self._round_price(spot + max(getattr(product, "price_increment", 0.01), spot * 0.001), product)
        if sl >= spot:
            sl = self._round_price(spot - max(getattr(product, "price_increment", 0.01), spot * 0.001), product)
        return tp, sl

    async def _seed_settings(self):
        cfg = CFG["trading"]
        scfg = cfg.get("strategy") or {}
        defaults = {
            "mode": self._mode,
            "delta_threshold": str(cfg["delta_threshold"]),
            "max_delta": str(cfg["max_delta"]),
            "max_vega": str(cfg["max_vega"]),
            "max_drawdown_pct": str(cfg["max_drawdown_pct"]),
            "capital_inr": str(cfg.get("capital_inr", 5000)),
            "margin_use_frac": str(cfg.get("margin_use_frac", 0.25)),
            "margin_use_max_frac": str(cfg.get("margin_use_max_frac", 0.30)),
            "take_profit_price_pct": str(
                scfg.get("take_profit_price_pct", scfg.get("take_profit_pct", 0.014))
            ),
            "stop_loss_price_pct": str(
                scfg.get("stop_loss_price_pct", scfg.get("stop_loss_pct", 0.007))
            ),
            "take_profit_pct": str(scfg.get("take_profit_pct", 0.014)),
            "stop_loss_pct": str(scfg.get("stop_loss_pct", 0.007)),
            "max_loss_frac": str(scfg.get("max_loss_frac", 0.08)),
            "trail_arm_r": str(scfg.get("trail_arm_r", 0.35)),
            "trail_giveback_r": str(scfg.get("trail_giveback_r", 0.30)),
            "leverage": str(cfg.get("leverage", 10)),
            "live_trading": str(self._live).lower(),
            "exchange": "coindcx",
            "margin_currency": MARGIN_CCY,
            "perp_symbol": cfg["perp_symbol"],
            "active_pair": self._active_pair,
            "scan_enabled": str(self._scan_enabled).lower(),
            "scan_pairs": ",".join(self._scan_pairs),
        }
        force_keys = (
            "mode", "capital_inr", "margin_use_frac", "margin_use_max_frac", "leverage",
            "take_profit_price_pct", "stop_loss_price_pct",
            "take_profit_pct", "stop_loss_pct",
            "perp_symbol", "margin_currency", "live_trading", "max_delta",
            "max_drawdown_pct",
        )
        existing = await self.store.rd.hgetall("rubaih:settings")
        merged = dict(existing or {})
        # Drop legacy fixed-₹ keys so the app never shows stale caps
        for stale in (
            "target_margin_inr", "max_margin_inr", "profit_trail_giveback_inr",
            "profit_trail_arm_inr", "max_loss_inr",
        ):
            merged.pop(stale, None)
            try:
                await self.store.rd.hdel("rubaih:settings", stale)
            except Exception:
                pass
        merged.update(defaults)
        await self.store.rd.hset("rubaih:settings", mapping={k: merged[k] for k in defaults})
        await self._apply_settings(merged)
        for k in force_keys:
            if k == "leverage":
                self._leverage = int(float(defaults["leverage"]))
                self.cycle.leverage = self._leverage
            elif k == "capital_inr":
                self.cycle.capital_inr = float(defaults["capital_inr"])
                self.cycle.set_free_capital(self.cycle.capital_inr)
            elif k == "margin_use_frac":
                self.cycle.margin_use_frac = float(defaults["margin_use_frac"])
            elif k == "margin_use_max_frac":
                self.cycle.margin_use_max_frac = float(defaults["margin_use_max_frac"])
            elif k == "take_profit_price_pct":
                self.cycle.take_profit_price_pct = float(
                    defaults["take_profit_price_pct"]
                )
            elif k == "stop_loss_price_pct":
                self.cycle.stop_loss_price_pct = float(
                    defaults["stop_loss_price_pct"]
                )
            elif k == "take_profit_pct":
                self.cycle.take_profit_pct = float(defaults["take_profit_pct"])
            elif k == "stop_loss_pct":
                self.cycle.stop_loss_pct = float(defaults["stop_loss_pct"])
            elif k == "mode":
                self._mode = str(defaults["mode"]).strip().lower()
            elif k == "max_delta":
                self.risk.max_delta = float(defaults["max_delta"])
            elif k == "max_drawdown_pct":
                self.risk.max_dd = float(defaults["max_drawdown_pct"])
        self.cycle.usdt_inr = float(cfg.get("usdt_inr", 87))
        self.cycle.take_profit_price_pct = float(
            scfg.get("take_profit_price_pct", scfg.get("take_profit_pct", 0.014))
        )
        self.cycle.stop_loss_price_pct = float(
            scfg.get("stop_loss_price_pct", scfg.get("stop_loss_pct", 0.007))
        )
        self.cycle.take_profit_pct = self.cycle.take_profit_price_pct
        self.cycle.stop_loss_pct = self.cycle.stop_loss_price_pct
        self.cycle.max_loss_frac = float(scfg.get("max_loss_frac", 0.08))
        self.cycle.trail_arm_r = float(scfg.get("trail_arm_r", 0.35))
        self.cycle.trail_giveback_r = float(scfg.get("trail_giveback_r", 0.30))
        self.cycle.trail_giveback_of_peak = float(scfg.get("trail_giveback_of_peak", 0.25))
        self.cycle.max_hold_sec = float(scfg.get("max_hold_sec", 14400))
        self.cycle.entry_cooldown = float(scfg.get("entry_cooldown_sec", 150))
        self.cycle.entry_move_pct = float(scfg.get("entry_move_pct", 0.0025))
        # App shows CoinDCX-style ROE (what you see as 10/20/30 on exchange)
        lev = max(1, int(self._leverage or 10))
        tp_px = self.cycle.take_profit_price_pct
        tp_roe = tp_px * lev
        sl_px = self.cycle.stop_loss_price_pct
        sl_roe = sl_px * lev
        try:
            await self.store.rd.hset(
                "rubaih:settings",
                mapping={
                    "take_profit_price_pct": str(tp_px),
                    "take_profit_roe": str(tp_roe),
                    "stop_loss_price_pct": str(sl_px),
                    "stop_loss_roe": str(sl_roe),
                    "take_profit_pct": str(tp_px),
                    "stop_loss_pct": str(sl_px),
                    "tp_display": f"Price +{tp_px*100:.2f}% (ROE≈+{tp_roe*100:.0f}% @{lev}x)",
                    "sl_display": f"Price −{sl_px*100:.2f}% (ROE≈−{sl_roe*100:.0f}% @{lev}x)",
                    "max_loss_frac": str(self.cycle.max_loss_frac),
                    "trail_arm_r": str(self.cycle.trail_arm_r),
                    "trail_giveback_r": str(self.cycle.trail_giveback_r),
                },
            )
        except Exception:
            pass
        if self.cycle._entry_price > 0 and self.cycle._plan_pair:
            self.cycle.refresh_exit_levels()
            try:
                await self.store.save_trade_plan(self.cycle.trade_plan_dict())
            except Exception:
                pass
        print(
            f"[SETTINGS] Forced from config: mode={self._mode} capital≈₹{self.cycle.capital_inr} "
            f"use={self.cycle.margin_use_frac:.0%}–{self.cycle.margin_use_max_frac:.0%} of free "
            f"TP_PRICE=+{tp_px:.2%} (ROE≈+{tp_roe:.0%} @{lev}x) "
            f"SL_PRICE=-{sl_px:.2%} (ROE≈-{sl_roe:.0%} @{lev}x) "
            f"trail={self.cycle.trail_arm_r}R→{self.cycle.trail_giveback_r}R "
            f"max_loss={self.cycle.max_loss_frac:.0%} margin lev={self._leverage}x"
        )

    async def _set_active_pair(self, pair: str):
        if not pair:
            return
        self._active_pair = pair
        try:
            # Keep config default in perp_symbol; active_pair is the live focus
            await self.store.rd.hset("rubaih:settings", mapping={"active_pair": pair})
            await self.store.rd.set("rubaih:active_pair", pair)
        except Exception:
            pass

    async def _apply_settings(self, data: Dict):
        if "mode" in data and data["mode"]:
            self._mode = str(data["mode"]).strip().lower()
        if "delta_threshold" in data:
            self.strategist.delta_threshold = float(data["delta_threshold"])
        if "max_delta" in data:
            self.risk.max_delta = float(data["max_delta"])
        if "max_vega" in data:
            self.risk.max_vega = float(data["max_vega"])
        if "max_drawdown_pct" in data:
            self.risk.max_dd = float(data["max_drawdown_pct"])
        # capital_inr intentionally NOT applied from command bus — prevents free-capital poison
        if "margin_use_frac" in data:
            self.cycle.margin_use_frac = float(data["margin_use_frac"])
        if "margin_use_max_frac" in data:
            self.cycle.margin_use_max_frac = float(data["margin_use_max_frac"])
        if "take_profit_price_pct" in data:
            self.cycle.take_profit_price_pct = float(data["take_profit_price_pct"])
            self.cycle.take_profit_pct = self.cycle.take_profit_price_pct
        elif "take_profit_pct" in data:
            # Backward-compatible API key; it means coin price movement.
            self.cycle.take_profit_price_pct = float(data["take_profit_pct"])
            self.cycle.take_profit_pct = self.cycle.take_profit_price_pct
        if "stop_loss_price_pct" in data:
            self.cycle.stop_loss_price_pct = float(data["stop_loss_price_pct"])
            self.cycle.stop_loss_pct = self.cycle.stop_loss_price_pct
        elif "stop_loss_pct" in data:
            self.cycle.stop_loss_price_pct = float(data["stop_loss_pct"])
            self.cycle.stop_loss_pct = self.cycle.stop_loss_price_pct
        if "max_loss_frac" in data:
            self.cycle.max_loss_frac = float(data["max_loss_frac"])
        if "trail_arm_r" in data:
            self.cycle.trail_arm_r = float(data["trail_arm_r"])
        if "trail_giveback_r" in data:
            self.cycle.trail_giveback_r = float(data["trail_giveback_r"])
        if "leverage" in data:
            self._leverage = int(float(data["leverage"]))
            self.cycle.leverage = self._leverage
        if any(
            k in data
            for k in (
                "take_profit_price_pct",
                "take_profit_pct",
                "stop_loss_price_pct",
                "stop_loss_pct",
                "trail_arm_r",
                "trail_giveback_r",
                "max_loss_frac",
            )
        ):
            self.cycle.refresh_exit_levels()
        print(
            f"[SETTINGS] mode={self._mode} threshold={self.strategist.delta_threshold} "
            f"max_delta={self.risk.max_delta} max_vega={self.risk.max_vega} "
            f"max_dd={self.risk.max_dd} capital≈₹{self.cycle.capital_inr} "
            f"use={self.cycle.margin_use_frac:.0%}–{self.cycle.margin_use_max_frac:.0%} "
            f"lev={self._leverage}x"
        )

    @staticmethod
    def _parse_free_margin_inr(payload, prefer_ccy: str = "INR") -> float:
        """Extract available INR futures margin from CoinDCX wallet / cross-margin payloads."""
        prefer = (prefer_ccy or "INR").upper()
        keys = (
            "available_balance_cross",
            "available_balance_isolated",
            "withdrawable_balance",
            "available_balance",
            "available_margin",
            "free_balance",
            "available_wallet_balance",
            "balance",
            "wallet_balance",
        )

        def row_ccy(d: Dict) -> str:
            return str(
                d.get("currency")
                or d.get("currency_short_name")
                or d.get("margin_currency")
                or d.get("margin_currency_short_name")
                or ""
            ).upper()

        def from_dict(d: Dict) -> float:
            if not isinstance(d, dict):
                return 0.0
            for k in keys:
                if k not in d or d[k] is None:
                    continue
                try:
                    v = float(d[k])
                except (TypeError, ValueError):
                    continue
                if k == "balance":
                    try:
                        locked = float(d.get("locked_balance") or 0)
                        v = max(0.0, v - locked)
                    except (TypeError, ValueError):
                        pass
                if v > 0:
                    return v
            return 0.0

        def best_from_list(items: list) -> float:
            preferred = 0.0
            any_pos = 0.0
            for item in items:
                if not isinstance(item, dict):
                    continue
                v = from_dict(item)
                if v <= 0:
                    continue
                ccy = row_ccy(item)
                if prefer in ccy or ccy == prefer:
                    preferred = max(preferred, v)
                elif not ccy:
                    preferred = max(preferred, v)
                else:
                    any_pos = max(any_pos, v)
            return preferred or any_pos

        if isinstance(payload, dict):
            # Top-level cross_margin_details (often no currency field)
            direct = from_dict(payload)
            if direct > 0:
                return direct
            for nest in ("data", "result", "cross_margin", "wallet", "wallets"):
                nested = payload.get(nest)
                if isinstance(nested, dict):
                    v = from_dict(nested)
                    if v > 0:
                        return v
                if isinstance(nested, list):
                    v = best_from_list(nested)
                    if v > 0:
                        return v
        if isinstance(payload, list):
            return best_from_list(payload)
        return 0.0

    async def _publish_capital_locked(self, free: float, source: str):
        """Mutate free capital. Caller MUST hold self._state_lock."""
        self.cycle.set_free_capital(free)
        self._capital_live_ok = free > 0
        self._capital_source = source
        try:
            await self.store.save_capital_ledger(free, source, self._margin_locked)
        except Exception:
            pass

    async def _publish_capital(self, free: float, source: str):
        async with self._state_lock:
            await self._publish_capital_locked(free, source)

    async def _ledger_adjust_locked(self, delta: float, reason: str):
        """Update free capital in-memory + await Redis persist. Lock MUST be held."""
        new_free = max(0.0, float(self.cycle.free_capital_inr) + float(delta))
        self.cycle.set_free_capital(new_free)
        self._capital_live_ok = True
        self._capital_source = "ledger"
        print(
            f"[CAPITAL] {reason}: Δ₹{delta:+.0f} → free=₹{new_free:.0f} "
            f"budget=₹{self.cycle.trade_margin_budget():.0f} locked=₹{self._margin_locked:.0f}"
        )
        try:
            await self.store.save_capital_ledger(new_free, "ledger", self._margin_locked)
        except Exception as e:
            print(f"[CAPITAL] ledger persist failed: {e}")

    async def _refresh_free_capital(self, force: bool = False):
        """
        Always keep free capital current:
        1) CoinDCX wallet APIs when they work (authoritative)
        2) Else auto ledger (updated after every fill) — no VPS edits per trade
        3) Seed once from env/config only if ledger empty

        Exchange I/O runs outside the state lock; publishes run under it.
        While a fill is pending settle, only exchange-sourced balances may overwrite.
        """
        now = time.time()
        last = getattr(self, "_last_capital_refresh", 0.0)
        if not force and now - last < 20:
            return
        self._last_capital_refresh = now
        free = 0.0
        source = ""
        try:
            if self.client and self.client._auth_ok:
                details = await self.client.get_cross_margin_details()
                free = self._parse_free_margin_inr(details, MARGIN_CCY)
                if free > 0:
                    source = "exchange:cross_margin"
                if free <= 0:
                    wallets = await self.client.get_futures_wallets()
                    free = self._parse_free_margin_inr(wallets, MARGIN_CCY)
                    if free > 0:
                        source = "exchange:futures_wallets"
                # Spot INR is NOT futures free — only warn, never size from it for LIVE
                if free <= 0 and not self._live:
                    bals = await self.client.get_user_balances()
                    free = self._parse_free_margin_inr(bals, MARGIN_CCY)
                    if free > 0:
                        source = "spot_balances(dry-run)"
        except Exception as e:
            print(f"[CAPITAL] exchange refresh failed: {e}")

        if free > 0:
            async with self._state_lock:
                # Tight heuristic only: tiny USDT-scale residuals on INR wallets.
                # Prefer settlement_currency_conversion_price from fills for FX.
                if (
                    source.startswith("exchange:")
                    and free < 50
                    and MARGIN_CCY.upper() == "INR"
                    and free * float(self.cycle.usdt_inr) > 500
                ):
                    converted = free * float(self.cycle.usdt_inr)
                    print(
                        f"[CAPITAL] exchange {free:.4f} looks USDT-scale → ₹{converted:.0f} "
                        f"@ usdt_inr={self.cycle.usdt_inr:.2f}"
                    )
                    free = converted
                await self._publish_capital_locked(free, source)
            if force or now - getattr(self, "_last_capital_log", 0.0) > 60:
                self._last_capital_log = now
                print(
                    f"[CAPITAL] free=₹{free:.0f} budget=₹{self.cycle.trade_margin_budget():.0f} "
                    f"via {source}"
                )
            return

        # --- No exchange balance: keep / restore auto ledger ---
        async with self._state_lock:
            # Do not invent capital from ledger while inventory is still settling
            if self._pending_settle or self._active_risk:
                print(
                    "[CAPITAL] skip ledger restore — pending settle / active risk "
                    "(waiting for exchange confirmation)"
                )
                return
            if self.cycle.free_capital_inr > 0 and (
                self._capital_live_ok or self._capital_source == "ledger"
            ):
                await self._publish_capital_locked(self.cycle.free_capital_inr, "ledger")
                if force or now - getattr(self, "_last_capital_log", 0.0) > 90:
                    self._last_capital_log = now
                    print(
                        f"[CAPITAL] ledger free=₹{self.cycle.free_capital_inr:.0f} "
                        f"(auto-updates after each trade; wallet API still 404)"
                    )
                return

        ledger = await self.store.load_capital_ledger()
        ledger_free = float(ledger.get("free_inr") or 0)
        if ledger_free > 0:
            async with self._state_lock:
                if self._pending_settle or self._active_risk:
                    return
                self._margin_locked = float(ledger.get("locked_margin") or 0)
                await self._publish_capital_locked(ledger_free, "ledger:redis")
            print(
                f"[CAPITAL] restored ledger free=₹{ledger_free:.0f} "
                f"locked=₹{float(ledger.get('locked_margin') or 0):.0f}"
            )
            return

        # Seed once only (env preferred, then config). Never re-seed over ledger.
        if not self._ledger_seeded:
            seed = FREE_CAPITAL_INR_ENV if FREE_CAPITAL_INR_ENV > 0 else 0.0
            if seed <= 0 and not self._live:
                seed = float(self.cycle.capital_inr or 0)
            if seed > 0:
                async with self._state_lock:
                    if self._pending_settle or self._active_risk:
                        return
                    self._ledger_seeded = True
                    await self._publish_capital_locked(
                        seed,
                        "seed:env" if FREE_CAPITAL_INR_ENV > 0 else "seed:config",
                    )
                print(
                    f"[CAPITAL] seeded free=₹{seed:.0f} once — will auto-track after trades. "
                    f"Set RUBAIH_FREE_CAPITAL_INR only for first seed if wallet API 404s."
                )
                return

        if self._live:
            async with self._state_lock:
                self._capital_live_ok = False
            if now - getattr(self, "_last_capital_fail_log", 0.0) > 60:
                self._last_capital_fail_log = now
                print(
                    "[CAPITAL] LIVE: no free capital yet. One-time: set "
                    "RUBAIH_FREE_CAPITAL_INR=<Futures INR> in .env, restart once — "
                    "after that the bot auto-updates free capital every trade."
                )
        else:
            await self._publish_capital(float(self.cycle.capital_inr or 1000), "seed:config")

    async def note_insufficient_funds(self, attempted_margin: float = 0.0):
        """After CoinDCX Insufficient funds — shrink ledger immediately."""
        async with self._state_lock:
            cur = max(self.cycle.free_capital_inr, 0.0)
            if attempted_margin > 0:
                new_free = max(50.0, attempted_margin * 0.65 / max(self.cycle.margin_use_frac, 0.1))
                new_free = min(new_free, cur * 0.65) if cur > 0 else new_free
            else:
                new_free = max(50.0, cur * 0.55) if cur > 0 else 200.0
            self._margin_locked = 0.0
            await self._publish_capital_locked(new_free, "ledger:insuff_cut")
            self.cycle._last_signal = time.time()
            print(
                f"[CAPITAL] Insufficient funds → ledger cut to ₹{new_free:.0f} "
                f"(budget ₹{self.cycle.trade_margin_budget():.0f})"
            )

    async def command_listener(self):
        """Honor signed kill-switch / settings from authenticated API. Reconnects if Redis drops."""
        while self._running:
            pubsub = None
            try:
                if self.store.rd is None:
                    await self.store.connect()
                pubsub = self.store.rd.pubsub()
                await pubsub.subscribe("rubaih:command")
                print("[CMD] Listening on rubaih:command (HMAC required)")
                while self._running:
                    try:
                        message = await pubsub.get_message(
                            ignore_subscribe_messages=True, timeout=1.0
                        )
                    except (redis.ConnectionError, redis.TimeoutError, OSError) as e:
                        print(f"[CMD] Redis pubsub error: {e}. Reconnecting in 3s...")
                        break
                    if not message or not message.get("data"):
                        await asyncio.sleep(0.05)
                        continue
                    try:
                        payload = json.loads(message["data"])
                    except Exception:
                        continue
                    if not isinstance(payload, dict):
                        continue
                    if not CMD_SECRET or not verify_command(CMD_SECRET, payload):
                        print(
                            "[CMD] REJECTED unsigned/invalid command — "
                            "ignoring (require HMAC from API)"
                        )
                        continue
                    cmd = payload.get("command")
                    if cmd == "KILL_SWITCH":
                        reason = f"API_KILL_SWITCH from {payload.get('source', 'unknown')}"
                        print(f"[CMD] {reason}")
                        self.risk.trigger_kill(reason)
                        await self.store.save_risk_event("KILL_SWITCH", reason)
                        await self.store.set_engine_status("kill_switch")
                        await self._emergency_unwind()
                        return
                    if cmd == "UPDATE_SETTINGS":
                        data = filter_settings(payload.get("data") or {})
                        if not data:
                            print("[CMD] UPDATE_SETTINGS empty after allowlist filter")
                            continue
                        await self._apply_settings(data)
                        await self.store.rd.hset(
                            "rubaih:settings", mapping={k: str(v) for k, v in data.items()}
                        )
                        await self.store.save_risk_event("SETTINGS_UPDATE", json.dumps(data))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[CMD] Listener error: {e}. Retry in 3s...")
            finally:
                if pubsub is not None:
                    try:
                        await pubsub.unsubscribe()
                        await pubsub.close()
                    except Exception:
                        pass
            if self._running:
                await asyncio.sleep(3)

    async def _supervised(self, name: str, coro_factory):
        """Restart a background loop on unexpected errors (don't kill the whole bot)."""
        while self._running:
            try:
                await coro_factory()
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[SUPERVISE] {name} crashed: {e}. Restarting in 5s...")
                await asyncio.sleep(5)

    async def bootstrap(self):
        print("[RUBAIH] Bootstrapping CoinDCX...")
        if not API_KEY or not API_SECRET:
            raise RuntimeError("COINDCX_API_KEY and COINDCX_API_SECRET are required")
        masked = f"{API_KEY[:4]}…{API_KEY[-4:]}" if len(API_KEY) >= 8 else "(short)"
        print(f"[RUBAIH] CoinDCX key: {masked} (len={len(API_KEY)}, secret_len={len(API_SECRET)})")
        if not os.getenv("DB_PASSWORD"):
            print("[WARN] DB_PASSWORD is empty — ensure compose/.env is configured")

        await self.store.connect()
        await self._seed_settings()
        await self.store.set_engine_status("starting")
        self.client = CoinDCXClient(self.auth)
        await self.client.__aenter__()
        await self.client.verify_credentials()
        if self._live and not self.client._auth_ok:
            print("[WARN] LIVE_TRADING=true but CoinDCX auth FAILED — live orders will be blocked")
            print("[WARN] Set LIVE_TRADING=false until [AUTH] CoinDCX OK appears")

        instruments = await self.client.get_active_instruments()
        if not isinstance(instruments, list):
            instruments = []
        active_set = {p for p in instruments if isinstance(p, str)}

        majors = [p for p in self._scan_pairs if p in active_set or not active_set]
        if not majors:
            majors = [CFG["trading"]["perp_symbol"]]

        fill_extras = bool(CFG["trading"].get("scan_fill_extras", False))
        extras = []
        if fill_extras:
            for pair in instruments:
                if not isinstance(pair, str) or pair in majors:
                    continue
                up = pair.upper()
                if "SLX" in up:
                    continue
                extras.append(pair)
        to_load = majors + extras
        to_load = to_load[: max(self._scan_max, len(majors))]
        print(
            f"[RUBAIH] Pair universe: {len(majors)} configured"
            + (f" + {len(extras)} extras" if extras else " (majors only — no meme fill)")
        )
        for pair in to_load:
            try:
                details = await self.client.get_instrument(pair)
                inst = details.get("instrument", details) if isinstance(details, dict) else {}
                # Base from pair id (B-ETH_USDT → ETH). Do NOT use position/margin
                # currency (often INR/USDT) — that made every coin look like BTC/INR.
                base = pair.replace("B-", "").replace("I-", "").split("_")[0].upper()
                raw_u = (
                    inst.get("underlying_currency_short_name")
                    or inst.get("underlying")
                    or base
                )
                underlying = str(raw_u).upper().strip()
                if underlying in ("INR", "USDT", "USD", "USD-M", "INR-M", ""):
                    underlying = base
                max_lev = float(
                    inst.get("max_leverage_long")
                    or inst.get("max_leverage")
                    or CFG["trading"].get("leverage", 10)
                    or 10
                )
                prod = CoinDCXProduct(
                    pair=pair,
                    symbol=pair,
                    underlying=underlying,
                    is_perp=True,
                    contract_value=float(inst.get("unit_contract_value", 1.0) or 1.0),
                    quantity_increment=float(inst.get("quantity_increment", 0.001) or 0.001),
                    min_quantity=float(inst.get("min_quantity", 0.001) or 0.001),
                    max_leverage=max(1.0, max_lev),
                    price_increment=float(
                        inst.get("price_increment")
                        or inst.get("quote_increment")
                        or 0.01
                    ),
                )
                self.products[pair] = prod
                self.portfolio.update_product(prod)
            except Exception as e:
                print(f"[RUBAIH] Failed to load instrument {pair}: {e}")

        # Final scan list = loaded products (majors first)
        loaded = [p for p in to_load if p in self.products]
        if not loaded:
            target = CFG["trading"]["perp_symbol"]
            prod = CoinDCXProduct(pair=target, symbol=target, underlying=CFG["trading"]["underlying"])
            self.products[target] = prod
            self.portfolio.update_product(prod)
            loaded = [target]
        self._scan_pairs = loaded
        self._active_pair = loaded[0]
        await self._set_active_pair(self._active_pair)

        self.pricing.update_surface("BTC", 0.55, -0.15, 0.08)
        self.pricing.update_surface("ETH", 0.60, -0.18, 0.10)
        self.pricing.update_surface("SOL", 0.70, -0.12, 0.10)
        self.pricing.update_surface("BNB", 0.65, -0.12, 0.10)
        print(f"[RUBAIH] Loaded {len(self.products)} CoinDCX instruments")
        print(f"[RUBAIH] Scan pairs ({len(self._scan_pairs)}): {', '.join(self._scan_pairs[:8])}{'…' if len(self._scan_pairs) > 8 else ''}")
        print(f"[RUBAIH] Mode: {self._mode} | scan={'ON' if self._scan_enabled else 'OFF'}")
        print(f"[RUBAIH] Active pair: {self._active_pair}")
        print(f"[RUBAIH] AI augmentation: {'ENABLED' if self._ai_enabled else 'DISABLED'}")
        print(f"[RUBAIH] LIVE_TRADING: {'ON — real orders' if self._live else 'OFF — dry-run only'}")
        print(
            f"[RUBAIH] Capital fallback: ₹{CFG['trading'].get('capital_inr', '?')} | "
            f"margin/trade: {CFG['trading'].get('margin_use_frac', 0.25):.0%}"
            f"–{CFG['trading'].get('margin_use_max_frac', 0.30):.0%} of free @ {self._leverage}x"
        )
        print(f"[RUBAIH] Margin currency: {MARGIN_CCY}")
        await self.store.rd.hset(
            "rubaih:settings",
            mapping={"scan_pairs": ",".join(self._scan_pairs), "active_pair": self._active_pair},
        )
        await self._refresh_free_capital(force=True)
        await self.store.set_engine_status("running" if self._live else "dry_run")
        # Restore open-trade TP/SL plan after restart
        plan = await self.store.load_trade_plan()
        if plan:
            self.cycle.restore_trade_plan(plan)
            print(f"[RUBAIH] Restored trade plan: {plan}")

    async def ws_listener(self):
        """CoinDCX public socket.io orderbook for the active pair (reconnects on pair switch)."""
        self._ob_parse_errors = 0

        while self._running:
            pair = self._active_pair or CFG["trading"]["perp_symbol"]
            self._ws_pair = pair
            channel = f"{pair}@orderbook@20-futures"
            sio = socketio.AsyncClient(logger=False, engineio_logger=False)
            try:
                @sio.event
                async def connect():
                    print(f"[WS] Connected — joining {channel}")
                    await sio.emit("join", {"channelName": channel})

                @sio.on("depth-snapshot")
                async def on_snapshot(data):
                    await self._on_orderbook(pair, data)

                @sio.on("depth-update")
                async def on_update(data):
                    await self._on_orderbook(pair, data)

                await sio.connect(WS_URL, transports=["websocket"])
                while self._running and sio.connected:
                    if self._active_pair and self._active_pair != pair:
                        print(f"[WS] Active pair changed {pair} → {self._active_pair}; reconnecting")
                        break
                    await asyncio.sleep(1)
            except Exception as e:
                print(f"[WS] Error: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)
            finally:
                try:
                    await sio.disconnect()
                except Exception:
                    pass
            await asyncio.sleep(0.5)

    @staticmethod
    def _best_prices(bids, asks) -> Tuple[float, float]:
        """CoinDCX books are usually {price_str: qty_str} maps; also support [[p,q],...]."""

        def from_map(levels, want_max: bool) -> float:
            prices = []
            for p in levels.keys():
                try:
                    prices.append(float(p))
                except (TypeError, ValueError):
                    continue
            if not prices:
                return 0.0
            return max(prices) if want_max else min(prices)

        def from_list(levels, want_max: bool) -> float:
            prices = []
            for level in levels:
                try:
                    if isinstance(level, (list, tuple)):
                        prices.append(float(level[0]))
                    elif isinstance(level, dict):
                        prices.append(float(level.get("price") or level.get("p") or 0))
                    else:
                        prices.append(float(level))
                except (TypeError, ValueError, IndexError, KeyError):
                    continue
            if not prices:
                return 0.0
            return max(prices) if want_max else min(prices)

        def best(levels, want_max: bool) -> float:
            if not levels:
                return 0.0
            if isinstance(levels, dict):
                return from_map(levels, want_max)
            if isinstance(levels, list):
                return from_list(levels, want_max)
            return 0.0

        return best(bids, True), best(asks, False)

    async def _on_orderbook(self, pair: str, data):
        try:
            payload = data
            if isinstance(data, dict) and "data" in data:
                payload = data["data"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            if not isinstance(payload, dict):
                return

            bids = payload.get("bids") or payload.get("b") or {}
            asks = payload.get("asks") or payload.get("a") or {}
            bid, ask = self._best_prices(bids, asks)
            if bid > 0 and ask > 0:
                mid = (bid + ask) / 2
                self._pair_mids[pair] = mid
                prod = self.products.get(pair)
                if prod:
                    self.portfolio.update_spot(prod.underlying, mid)
                self.cycle.push_price(mid, pair)
                self._ob_parse_errors = 0
        except Exception as e:
            self._ob_parse_errors = getattr(self, "_ob_parse_errors", 0) + 1
            if self._ob_parse_errors <= 3 or self._ob_parse_errors % 100 == 0:
                print(f"[WS] Orderbook parse error ({type(e).__name__}): {e!r}")

    async def price_poller(self):
        """REST orderbook scan across candidate pairs + active pair refresh."""
        while self._running:
            try:
                pairs = list(self._scan_pairs) if self._scan_enabled else [self._active_pair]
                # Always include active
                if self._active_pair and self._active_pair not in pairs:
                    pairs = [self._active_pair] + pairs
                for pair in pairs:
                    if not self._running:
                        break
                    try:
                        book = await self.client.get_orderbook(pair)
                        await self._on_orderbook(pair, book)
                    except Exception as e:
                        if pair == self._active_pair:
                            print(f"[PRICE] {pair}: {e}")
                    await asyncio.sleep(0.15)
            except Exception as e:
                print(f"[PRICE] Error: {e}")
            await asyncio.sleep(self._scan_interval)

    def _mid_for(self, pair: Optional[str] = None) -> float:
        pair = pair or self._active_pair
        if pair and pair in self._pair_mids and self._pair_mids[pair] > 0:
            return self._pair_mids[pair]
        if pair and pair in self.products:
            u = self.products[pair].underlying
            spot = self.portfolio.spot_prices.get(u, 0.0)
            if spot > 0:
                return spot
        return self.portfolio.spot_prices.get(CFG["trading"]["underlying"], 0.0)

    def _find_open_position(self) -> Tuple[Optional[Position], Optional[str]]:
        """Return (position, pair) for the single open scan-pair position, if any."""
        if self._dry_pos and self._dry_pos.size > 0:
            return self._dry_pos, self._dry_pos.symbol
        allow = set(self._scan_pairs) | {self._active_pair}
        for pair in list(allow):
            pos = self.portfolio.positions.get(pair)
            if pos and pos.size > 0:
                return pos, pair
        for pair, pos in self.portfolio.positions.items():
            if pos and pos.size > 0 and pair in allow:
                return pos, pair
        return None, None

    def _position_meta(self) -> Tuple[float, str, Optional[str]]:
        """Return (size, side, pair) for dashboard — cycle local pos preferred."""
        pos, pair = self._find_open_position()
        if pos and pos.size > 0:
            side = (pos.side or "buy").lower()
            if side in ("buy", "long"):
                side = "long"
            elif side in ("sell", "short"):
                side = "short"
            else:
                side = "flat"
            return float(pos.size), side, pair or pos.symbol
        return 0.0, "flat", self._active_pair

    async def _publish_snapshot(self):
        """Push greeks + position so dashboard updates right after fills."""
        greeks = self.portfolio.compute_greeks()
        size, side, pair = self._position_meta()
        # Futures: delta == signed position size (gamma/vega/theta stay 0)
        if self._mode == "futures_cycle" and size > 0:
            signed = size if side == "long" else -size
            greeks = GreeksSnapshot(
                timestamp=time.time(),
                delta=signed,
                gamma=0.0,
                vega=0.0,
                theta=0.0,
            )
        spot = self._mid_for(pair or self._active_pair)
        if spot <= 0 and self._pair_mids:
            spot = next(iter(self._pair_mids.values()), 0.0)
        session_pnl = sum(p.unrealized_pnl for p in self.portfolio.positions.values())
        if self._dry_pos and spot > 0 and self._dry_pos.entry_price > 0:
            self._dry_pos.unrealized_pnl = self._pnl_inr(
                self._dry_pos.entry_price, spot, self._dry_pos.size, self._dry_pos.side or "buy",
            )
            session_pnl = self._dry_pos.unrealized_pnl
        await self.store.save_greeks(
            greeks,
            spot,
            session_pnl,
            active_pair=pair or self._active_pair,
            position_size=size,
            position_side=side,
        )

    async def _exchange_active_qty(self, pair: str) -> Optional[float]:
        """Return abs(active_pos) for pair, 0 if flat, None if positions API failed."""
        if not self.client or not self.client._auth_ok:
            return None
        try:
            raw = await self.client.get_positions() or []
        except Exception as e:
            print(f"[SYNC] positions fetch failed: {e}")
            return None
        saw = False
        for rp in raw:
            if rp.get("pair", "") != pair:
                continue
            saw = True
            return abs(float(rp.get("active_pos", 0) or 0))
        # Pair absent from feed — treat as flat only if API returned a list
        return 0.0 if isinstance(raw, list) else None

    async def _confirm_exchange_flat(
        self, pair: str, reads: int = 3, delay_sec: float = 1.2
    ) -> bool:
        """Require consecutive flat readings before trusting exchange as settled flat."""
        if not pair:
            return False
        ok = 0
        for i in range(max(1, reads)):
            qty = await self._exchange_active_qty(pair)
            if qty is None:
                print(f"[SYNC] flat-confirm {i + 1}/{reads}: positions API error — not flat")
                return False
            if qty > 1e-12:
                print(f"[SYNC] flat-confirm {i + 1}/{reads}: still open qty={qty}")
                return False
            ok += 1
            if i < reads - 1:
                await asyncio.sleep(delay_sec)
        print(f"[SYNC] flat-confirm OK for {pair} ({ok}/{reads})")
        return True

    async def sync_positions(self):
        """Sync positions for scan allowlist only — never ingest SLX / other books."""
        while self._running:
            try:
                allow = set(self._scan_pairs) | {self._active_pair}

                # Snapshot local SoT under lock (mutations happen later under lock)
                async with self._state_lock:
                    local = self._dry_pos
                    local_size = float(local.size) if local and local.size > 0 else 0.0
                    local_symbol = local.symbol if local and local_size > 0 else None

                # futures_cycle: local cycle position is source of truth for dashboard
                if self._mode == "futures_cycle" and local_symbol and local_size > 0:
                    spot = self._mid_for(local_symbol)
                    async with self._state_lock:
                        if self._dry_pos and self._dry_pos.symbol == local_symbol:
                            if spot > 0 and self._dry_pos.entry_price > 0:
                                self._dry_pos.unrealized_pnl = self._pnl_inr(
                                    self._dry_pos.entry_price,
                                    spot,
                                    self._dry_pos.size,
                                    self._dry_pos.side or "buy",
                                )
                            self.portfolio.update_positions([self._dry_pos])
                    await self._set_active_pair(local_symbol)
                    # Live: overlay exchange; ghost-close only after structural settle
                    if self._live and self.client and self.client._auth_ok:
                        try:
                            raw = await self.client.get_positions() or []
                            open_pairs = set()
                            matched_flat = False
                            matched_open = False
                            mark = 0.0
                            avg = 0.0
                            active = 0.0
                            for rp in raw:
                                pair = rp.get("pair", "")
                                act = float(rp.get("active_pos", 0) or 0)
                                if abs(act) > 0 and pair:
                                    open_pairs.add(pair)
                                if pair != local_symbol:
                                    continue
                                mark = float(rp.get("mark_price", 0) or 0)
                                avg = float(rp.get("avg_price", 0) or 0)
                                active = act
                                if abs(act) > 0:
                                    matched_open = True
                                else:
                                    matched_flat = True

                            async with self._state_lock:
                                if not self._dry_pos or self._dry_pos.symbol != local_symbol:
                                    pass
                                elif matched_open:
                                    self._dry_pos.size = abs(active)
                                    self._dry_pos.side = "buy" if active > 0 else "sell"
                                    if avg > 0:
                                        self._dry_pos.entry_price = avg
                                    if mark > 0 and avg > 0:
                                        self._dry_pos.unrealized_pnl = self._pnl_inr(
                                            avg, mark, abs(active), self._dry_pos.side,
                                        )
                                    self.portfolio.update_positions([self._dry_pos])
                                    # Exchange inventory matches — settle complete
                                    self._pending_settle = False
                                    self._flat_confirm_count = 0
                                    if self._active_risk and abs(active) > 0:
                                        # Still open after failed flatten — keep flag
                                        pass
                                else:
                                    ghost_candidate = matched_flat or (
                                        local_symbol not in open_pairs
                                    )
                                    age = time.time() - self._last_fill_ts
                                    if not ghost_candidate:
                                        self._flat_confirm_count = 0
                                    elif self._pending_settle:
                                        self._flat_confirm_count = 0
                                        print(
                                            f"[SYNC] defer ghost-close {local_symbol} — "
                                            f"fill pending exchange settle"
                                        )
                                    elif age < self._settle_grace_sec:
                                        self._flat_confirm_count = 0
                                        print(
                                            f"[SYNC] defer ghost-close {local_symbol} — "
                                            f"settle grace {age:.0f}/{self._settle_grace_sec:.0f}s"
                                        )
                                    else:
                                        self._flat_confirm_count += 1
                                        need = self._flat_confirms_needed
                                        if self._flat_confirm_count < need:
                                            print(
                                                f"[SYNC] flat reading "
                                                f"{self._flat_confirm_count}/{need} for "
                                                f"{local_symbol} — no capital credit yet"
                                            )
                                        else:
                                            # Structurally settled flat — safe to clear + credit
                                            closed_pair = self._dry_pos.symbol
                                            last_pnl = float(
                                                self._dry_pos.unrealized_pnl or 0
                                            )
                                            release = float(self._margin_locked or 0)
                                            print(
                                                f"[SYNC] Exchange flat confirmed for "
                                                f"{closed_pair} — external close; "
                                                f"clearing SoT (pnl≈₹{last_pnl:.0f} "
                                                f"release≈₹{release:.0f})"
                                            )
                                            self._dry_pos = None
                                            self.cycle.clear_trade()
                                            self._last_flatten_ts = time.time()
                                            self._margin_locked = 0.0
                                            self._pending_settle = False
                                            self._flat_confirm_count = 0
                                            self._active_risk = False
                                            # Credit only still-locked margin (never
                                            # double-pay if an exit already unlocked).
                                            credit = release + (
                                                last_pnl if release > 0 else 0.0
                                            )
                                            if abs(credit) > 1e-9:
                                                await self._ledger_adjust_locked(
                                                    credit,
                                                    f"EXTERNAL_CLOSE {closed_pair} "
                                                    f"pnl≈₹{last_pnl:.0f}",
                                                )
                                            await self.store.save_trade_plan({})
                                            self.portfolio.update_positions([])
                        except Exception as e:
                            print(f"[SYNC] live overlay: {e}")
                    # Faster poll while holding so CoinDCX manual closes are noticed sooner
                    await asyncio.sleep(5)
                    continue

                raw = await self.client.get_positions() if self.client else []
                positions = []
                for rp in (raw or []):
                    pair = rp.get("pair", "")
                    if pair and pair not in allow:
                        continue
                    active = float(rp.get("active_pos", 0) or 0)
                    if active == 0:
                        continue
                    if not pair:
                        pair = self._active_pair
                    if pair not in allow:
                        continue
                    side = "buy" if active > 0 else "sell"
                    mark = float(rp.get("mark_price", 0) or 0)
                    avg = float(rp.get("avg_price", 0) or 0)
                    if mark > 0:
                        self._pair_mids[pair] = mark
                        if pair in self.products:
                            self.portfolio.update_spot(self.products[pair].underlying, mark)
                        self.cycle.push_price(mark, pair)
                    upnl = 0.0
                    if mark > 0 and avg > 0:
                        upnl = self._pnl_inr(avg, mark, abs(active), side)
                    positions.append(Position(
                        symbol=pair,
                        product_id=str(rp.get("id", pair)),
                        side=side,
                        size=abs(active),
                        entry_price=avg,
                        unrealized_pnl=upnl,
                    ))
                if len(positions) > 1:
                    positions = positions[:1]
                # Avoid resurrecting a just-flattened position from exchange lag
                async with self._state_lock:
                    flatten_recent = time.time() - self._last_flatten_ts < 20
                    flat_local = not (self._dry_pos and self._dry_pos.size > 0)
                if positions and flatten_recent:
                    positions = []
                if positions:
                    await self._set_active_pair(positions[0].symbol)
                    async with self._state_lock:
                        if self._mode == "futures_cycle":
                            self._dry_pos = positions[0]
                            self._pending_settle = False
                            self._flat_confirm_count = 0
                            if (
                                self.cycle._tp_price <= 0
                                or self.cycle._plan_pair != positions[0].symbol
                            ):
                                self.cycle.arm_trade(
                                    positions[0].symbol,
                                    positions[0].entry_price,
                                    positions[0].size,
                                    leverage=self._leverage_for(positions[0].symbol),
                                )
                                await self.store.save_trade_plan(
                                    self.cycle.trade_plan_dict()
                                )
                                await self._log(
                                    f"[SYNC] Armed TP/SL for open {positions[0].symbol} "
                                    f"entry={positions[0].entry_price}"
                                )
                            # If we previously unlocked margin but exchange still holds,
                            # re-debit free so capital cannot be double-spent.
                            if self._margin_locked <= 0 and positions[0].size > 0:
                                relock = float(self.cycle._margin_used or 0)
                                if relock <= 0:
                                    lev = max(1, self._leverage_for(positions[0].symbol))
                                    relock = (
                                        positions[0].size
                                        * max(positions[0].entry_price, 0)
                                        * float(self.cycle.usdt_inr)
                                    ) / lev
                                if relock > 0:
                                    self._margin_locked = relock
                                    await self._ledger_adjust_locked(
                                        -relock,
                                        f"RESYNC re-lock {positions[0].symbol}",
                                    )
                        self.portfolio.update_positions(positions)
                else:
                    async with self._state_lock:
                        if self._mode == "futures_cycle" and flat_local and not self._dry_pos:
                            self.cycle.clear_trade()
                            await self.store.save_trade_plan({})
                        self.portfolio.update_positions(positions)
            except Exception as e:
                print(f"[SYNC] Error: {e}")
            await asyncio.sleep(10)

    async def ai_analysis_loop(self):
        """Periodic AI analysis — quiet; quant/cycle remains authority."""
        if not self._ai_enabled:
            return
        while self._running and self.risk.alive:
            try:
                greeks = self.portfolio.compute_greeks()
                spot = self.portfolio.spot_prices.get(CFG["trading"]["underlying"], 0.0)
                if spot <= 0:
                    await asyncio.sleep(10)
                    continue

                self._hedge_history = await self.store.get_recent_hedges(5)

                context = {
                    "portfolio_greeks": {
                        "delta": greeks.delta,
                        "gamma": greeks.gamma,
                        "vega": greeks.vega,
                        "theta": greeks.theta
                    },
                    "spot_price": spot,
                    "recent_hedges": self._hedge_history,
                    "iv_change_1h": 0.0,
                    "funding_rate": 0.0001,
                    "time_since_last_hedge": time.time() - self.strategist._last_hedge,
                    "quant_signal": "HOLD" if abs(greeks.delta) < self.strategist.delta_threshold else "HEDGE"
                }

                decision = await self.ai.analyze_market(context)
                if decision:
                    await self.store.save_ai_decision(decision, greeks.delta)
                    # analyze_market already prints once — avoid duplicate line

                    if decision.action == "EMERGENCY" and decision.confidence > 0.95:
                        self.risk.trigger_kill(f"AI_EMERGENCY: {decision.reasoning}")
                        await self.store.save_risk_event("AI_EMERGENCY", decision.reasoning)
                        await self.store.set_engine_status("kill_switch")
                        await self._emergency_unwind()
                        break
            except Exception as e:
                print(f"[AI LOOP] Error: {e}")
            await asyncio.sleep(180)  # 3 min — free tiers rate-limit hard

    async def main_loop(self):
        while self._running and self.risk.alive:
            try:
                spot = self._mid_for(self._active_pair)
                if spot <= 0 and not self._pair_mids:
                    await asyncio.sleep(1)
                    continue

                await self._publish_snapshot()
                size, side, _pair = self._position_meta()
                greeks = self.portfolio.compute_greeks()
                if self._mode == "futures_cycle" and size > 0:
                    signed = size if side == "long" else -size
                    greeks = GreeksSnapshot(
                        timestamp=time.time(), delta=signed, gamma=0.0, vega=0.0, theta=0.0,
                    )
                session_pnl = sum(p.unrealized_pnl for p in self.portfolio.positions.values())
                if self._dry_pos:
                    session_pnl = self._dry_pos.unrealized_pnl

                capital_base = max(
                    float(self.cycle.free_capital_inr or 0)
                    + float(self._margin_locked or 0),
                    float(self.cycle.capital_inr or 0),
                    1.0,
                )
                violation = self.risk.check(greeks, session_pnl, capital_base)
                if violation:
                    await self._log(f"[RISK] VIOLATION: {violation}")
                    await self.store.save_risk_event("VIOLATION", violation)
                    await self.store.set_engine_status("halted")
                    await self._emergency_unwind()
                    break

                if self._mode == "futures_cycle":
                    await self._refresh_free_capital()
                    try:
                        await self.store.publish_scan(self._scan_rows())
                    except Exception:
                        pass
                    if self.cycle._last_scan_msg:
                        await self._log(self.cycle._last_scan_msg)
                        self.cycle._last_scan_msg = None
                    pos, pos_pair = self._find_open_position()
                    signal = None
                    if pos and pos_pair:
                        await self._set_active_pair(pos_pair)
                        exit_spot = self._mid_for(pos_pair)
                        if exit_spot > 0:
                            signal = self.cycle.evaluate_exit(exit_spot, pos, pos_pair)
                            # Persist peak while holding
                            try:
                                await self.store.save_trade_plan(self.cycle.trade_plan_dict())
                            except Exception:
                                pass
                    else:
                        # Flat: keep restored plan only if we somehow still have peaks — normally clear
                        async with self._state_lock:
                            if self.cycle._plan_pair and not self._dry_pos:
                                self.cycle.clear_trade()
                        if self._live and not getattr(self, "_capital_live_ok", False):
                            signal = None
                        elif self._active_risk:
                            signal = None  # entries blocked while flatten_failed risk open
                        else:
                            mids = {
                                p: self._pair_mids[p]
                                for p in self._scan_pairs
                                if p in self._pair_mids and self._pair_mids[p] > 0
                            }
                            if not self._scan_enabled:
                                ap = self._active_pair
                                if ap in self._pair_mids:
                                    mids = {ap: self._pair_mids[ap]}
                            min_qty_by_pair = {
                                p: (self.products[p].min_quantity if p in self.products else 0.001)
                                for p in mids
                            }
                            lev_by_pair = {p: float(self._leverage_for(p)) for p in mids}
                            step_by_pair = {
                                p: (self.products[p].quantity_increment if p in self.products else 0.001)
                                for p in mids
                            }
                            signal = self.cycle.pick_entry(
                                mids, min_qty_by_pair,
                                leverage_by_pair=lev_by_pair,
                                step_by_pair=step_by_pair,
                            )
                    if signal and signal.hedge_size != 0:
                        if signal.pair:
                            await self._set_active_pair(signal.pair)
                        await self._log(f"[CYCLE] {signal.reason}")
                        # Exits must never be rate-limited or soft-blocked
                        is_exit = signal.hedge_size < 0
                        ok = await self._execute_hedge(signal, force=is_exit)
                        await self._publish_snapshot()
                        if ok:
                            if is_exit:
                                await self.store.save_trade_plan({})
                            else:
                                await self.store.save_trade_plan(self.cycle.trade_plan_dict())
                            await self._refresh_free_capital(force=True)
                else:
                    signal = self.strategist.evaluate(greeks, spot if spot > 0 else next(iter(self._pair_mids.values()), 0.0))
                    if signal and signal.hedge_size != 0:
                        await self._log(f"[HEDGE] {signal.reason}")
                        await self._execute_hedge(signal)
                        await self._publish_snapshot()
            except Exception as e:
                await self._log(f"[LOOP] Error: {e}")
            await asyncio.sleep(1)

    async def _execute_hedge(self, signal: HedgeSignal, force: bool = False) -> bool:
        if not force and not self.risk.alive:
            print("[HEDGE] Blocked — kill switch active")
            return False
        if not self.risk.rate_limit_ok() and not force:
            print("[HEDGE] Rate limited")
            return False
        perp_symbol = signal.pair or self._active_pair or CFG["trading"]["perp_symbol"]
        perp_prod = self.products.get(perp_symbol)
        if not perp_prod:
            print(f"[HEDGE] Perp {perp_symbol} not found")
            return False
        if not signal.pair:
            signal.pair = perp_symbol

        side = Side.BUY if signal.hedge_size > 0 else Side.SELL
        size = abs(signal.hedge_size)
        cfg = CFG["trading"]
        spot = self._mid_for(perp_symbol)
        if spot <= 0:
            spot = self.portfolio.spot_prices.get(perp_prod.underlying, 0.0)
        lev = self._leverage_for(perp_symbol)
        # Never send exchange TP/SL — CoinDCX INR-M 422s. Bot locks TP/SL at fill.

        def _is_reject(result) -> bool:
            if isinstance(result, list):
                return False  # create success is typically a list of orders
            if not isinstance(result, dict):
                return True
            code = result.get("code")
            return (
                result.get("status") == "error"
                or code in (400, 401, 403, 422, 500, "400", "401", "403", "422", "500")
                or bool(result.get("error"))
            )

        async def _claim_coid(coid: str) -> bool:
            """Redis NX claim so the same client_order_id is never double-submitted."""
            if not self.store.rd or not coid:
                return True
            try:
                ok = await self.store.rd.set(
                    f"rubaih:coid:{coid}", "pending", nx=True, ex=3600
                )
                return bool(ok)
            except Exception:
                return True

        async def _finish_coid(coid: str, meta: Dict):
            if not self.store.rd or not coid:
                return
            try:
                await self.store.rd.set(
                    f"rubaih:coid:{coid}", json.dumps(meta), ex=86400
                )
            except Exception:
                pass

        def _maybe_update_fx(fx: float):
            if fx > 0 and abs(fx - float(self.cycle.usdt_inr)) / max(fx, 1e-9) > 0.002:
                print(
                    f"[FX] usdt_inr {self.cycle.usdt_inr:.2f} → {fx:.2f} "
                    f"(exchange settlement_currency_conversion_price)"
                )
                self.cycle.usdt_inr = float(fx)

        async def _place(qty: float) -> bool:
            if qty <= 0 or qty < perp_prod.min_quantity:
                await self._log(f"[HEDGE] Size {qty} below min {perp_prod.min_quantity}")
                return False
            if self._live and self.client and not self.client._auth_ok:
                if not getattr(self, "_live_block_warned", False):
                    self._live_block_warned = True
                    await self._log(
                        "[LIVE BLOCKED] CoinDCX auth failed — no fills until AUTH OK. "
                        "Fix API keys or set LIVE_TRADING=false for dry-run."
                    )
                await self._log(f"[LIVE BLOCKED] Would {side.value} {qty} {perp_symbol} @ ~{spot}")
                return False
            if not self._live or not self.client:
                await self._log(
                    f"[DRY-RUN] Would {side.value} {qty} {perp_symbol} @ ~{spot} lev={lev}x "
                    f"botTP={self.cycle.take_profit_pct:.2%} botSL={self.cycle.stop_loss_pct:.2%} "
                    f"(no exchange TP/SL fields)"
                )
                await self.store.save_hedge(signal, spot, size=qty)
                async with self._state_lock:
                    await self._apply_fill_locked(
                        perp_symbol, side, qty, spot, pending_settle=False
                    )
                return True

            coid = f"rb-{int(time.time() * 1000)}-{secrets.token_hex(4)}"
            if not await _claim_coid(coid):
                await self._log(f"[HEDGE] Idempotency claim failed for {coid} — skip")
                return False

            try:
                result = await self.client.place_order(
                    perp_prod.pair,
                    side,
                    qty,
                    "market",
                    lev,
                    client_order_id=coid,
                )
            except Exception as e:
                await self._log(
                    f"[HEDGE] place_order exception ({coid}): {e} — "
                    f"reconciling via order list / positions (no blind retry)"
                )
                await _finish_coid(coid, {"status": "ambiguous", "error": str(e)})
                async with self._state_lock:
                    self._pending_settle = True
                    self._last_fill_ts = time.time()
                return False

            await self._log(
                f"[HEDGE] LIVE create @{lev}x coid={coid}: {result}"
            )

            if _is_reject(result):
                await self._log(f"[HEDGE] Order rejected — not updating position: {result}")
                await _finish_coid(coid, {"status": "rejected", "raw": result})
                msg = ""
                if isinstance(result, dict):
                    msg = str(result.get("message") or result.get("error") or "").lower()
                if "insufficient" in msg:
                    try:
                        m = (qty * spot * self.cycle.usdt_inr) / max(lev, 1)
                    except Exception:
                        m = self.cycle.trade_margin_budget()
                    await self.note_insufficient_funds(m)
                return False

            initial = CoinDCXClient.normalize_order_payload(result)
            order_id = str(initial.get("id") or "")
            # Create often returns status=initial, avg_price=0 — must poll for truth
            resolved = await self.client.resolve_order_fill(
                order_id=order_id,
                requested_qty=qty,
                timeout_sec=12.0,
                poll_sec=0.4,
                initial=initial if initial else None,
            )
            filled_qty = float(resolved.get("filled_qty") or 0)
            fill_px = float(resolved.get("avg_price") or 0)
            if fill_px <= 0:
                fill_px = spot
            fx = float(resolved.get("fx") or 0)
            _maybe_update_fx(fx)
            # Prefer conversion price already on create payload
            if fx <= 0 and initial:
                try:
                    _maybe_update_fx(
                        float(initial.get("settlement_currency_conversion_price") or 0)
                    )
                except (TypeError, ValueError):
                    pass

            filled_qty = self._round_qty(filled_qty, perp_prod) if filled_qty > 0 else 0.0
            await _finish_coid(
                coid,
                {
                    "status": resolved.get("status"),
                    "order_id": order_id,
                    "filled_qty": filled_qty,
                    "avg_price": fill_px,
                    "requested": qty,
                },
            )

            if filled_qty <= 0:
                await self._log(
                    f"[HEDGE] No confirmed fill for {coid} order={order_id} "
                    f"status={resolved.get('status')} — local SoT unchanged"
                )
                async with self._state_lock:
                    self._pending_settle = True
                    self._last_fill_ts = time.time()
                return False

            if filled_qty + 1e-12 < qty:
                await self._log(
                    f"[HEDGE] PARTIAL fill {filled_qty}/{qty} {perp_symbol} "
                    f"@ {fill_px:.4f} status={resolved.get('status')}"
                )
            elif abs(fill_px - spot) / max(spot, 1e-9) > 0.0005:
                await self._log(
                    f"[HEDGE] Fill price {fill_px:.4f} (mid was {spot:.4f}) — "
                    f"arming TP/SL on confirmed fill"
                )

            await self.store.save_hedge(signal, fill_px, size=filled_qty)
            async with self._state_lock:
                await self._apply_fill_locked(
                    perp_symbol, side, filled_qty, fill_px, pending_settle=True
                )
            return True

        if size > cfg["max_order_size_btc"]:
            slices = max(1, int(math.ceil(size / cfg["max_order_size_btc"])))
            slices = min(slices, cfg.get("twap_slices", 3) if not force else max(slices, 1))
            slice_size = self._round_qty(size / slices, perp_prod)
            print(f"[HEDGE] TWAP: {slices} slices of {slice_size} on {perp_symbol}{' (force)' if force else ''}")
            any_ok = False
            for i in range(slices):
                if await _place(slice_size):
                    any_ok = True
                if i < slices - 1:
                    await asyncio.sleep(cfg["twap_interval_sec"] if not force else max(2, cfg["twap_interval_sec"] // 2))
            return any_ok
        qty = self._round_qty(size, perp_prod)
        return await _place(qty)

    async def _apply_fill_locked(
        self,
        symbol: str,
        side: Side,
        qty: float,
        price: float,
        pending_settle: bool = False,
    ):
        """Update local cycle position + ledger. Caller MUST hold self._state_lock."""
        if self._mode != "futures_cycle" or qty <= 0:
            return
        if symbol not in self.products:
            base = symbol.replace("B-", "").replace("I-", "").split("_")[0]
            prod = CoinDCXProduct(pair=symbol, symbol=symbol, underlying=base, is_perp=True)
            self.products[symbol] = prod
            self.portfolio.update_product(prod)
        if price > 0 and symbol in self.products:
            self.portfolio.update_spot(self.products[symbol].underlying, price)
            self._pair_mids[symbol] = price

        self._last_fill_ts = time.time()
        self._pending_settle = bool(pending_settle)
        self._flat_confirm_count = 0
        lev = max(1, self._leverage_for(symbol))
        notional = qty * price * float(self.cycle.usdt_inr)
        fee = notional * float(self.cycle.taker_fee)
        pos = self._dry_pos
        if side == Side.BUY:
            if pos and pos.side == "buy" and pos.symbol == symbol:
                new_size = pos.size + qty
                pos.entry_price = ((pos.entry_price * pos.size) + price * qty) / new_size
                pos.size = new_size
            else:
                self._dry_pos = Position(
                    symbol=symbol, product_id=f"dry-{symbol}", side="buy",
                    size=qty, entry_price=price, unrealized_pnl=0.0,
                )
            if self._dry_pos:
                prev_locked = float(self._margin_locked or 0)
                self.cycle.arm_trade(
                    symbol, self._dry_pos.entry_price, self._dry_pos.size, leverage=lev
                )
                new_locked = float(self.cycle._margin_used or 0)
                self._margin_locked = new_locked
                await self._ledger_adjust_locked(
                    -(max(0.0, new_locked - prev_locked) + fee),
                    f"ENTRY {symbol} margin→₹{new_locked:.0f}",
                )
        else:  # SELL
            if pos and pos.side == "buy" and pos.symbol == symbol:
                entry = pos.entry_price
                closed = min(qty, pos.size)
                pnl = self._pnl_inr(entry, price, closed, "buy")
                remain = pos.size - qty
                if remain <= 1e-12:
                    release = float(self._margin_locked or self.cycle._margin_used or 0)
                    self._dry_pos = None
                    self.cycle.clear_trade()
                    self._last_flatten_ts = time.time()
                    self._margin_locked = 0.0
                    # Live exits stay pending_settle until exchange confirms flat;
                    # capital credit already applied here — ghost-close must not double-credit.
                    if pending_settle:
                        self._pending_settle = True
                    else:
                        self._pending_settle = False
                        self._active_risk = False
                    await self._ledger_adjust_locked(
                        release + pnl - fee,
                        f"EXIT {symbol} pnl=₹{pnl:.0f} released=₹{release:.0f}",
                    )
                else:
                    frac = closed / max(pos.size, 1e-12)
                    release = float(self._margin_locked or 0) * frac
                    self._margin_locked = max(0.0, float(self._margin_locked or 0) - release)
                    pos.size = remain
                    await self._ledger_adjust_locked(
                        release + pnl - fee,
                        f"PARTIAL_EXIT {symbol} pnl=₹{pnl:.0f}",
                    )
            else:
                self._dry_pos = None
                self.cycle.clear_trade()
                self._last_flatten_ts = time.time()
                self._margin_locked = 0.0
                self._pending_settle = bool(pending_settle)
        if self._dry_pos:
            self.portfolio.update_positions([self._dry_pos])
        else:
            self.portfolio.update_positions([])
            self.cycle.clear_trade()

    async def _emergency_unwind(self):
        """Flatten open risk. Local SoT cleared only after exchange confirms flat."""
        print("[EMERGENCY] Flattening...")
        try:
            if self._live and self.client:
                await self.client.cancel_all_orders()
            else:
                print("[DRY-RUN] Would cancel all open orders")
        except Exception as e:
            print(f"[EMERGENCY] cancel_all failed: {e}")

        async with self._state_lock:
            pos, pos_pair = self._find_open_position()
            snap = None
            if pos and pos_pair and pos.size > 0:
                snap = Position(
                    symbol=pos.symbol,
                    product_id=pos.product_id,
                    side=pos.side,
                    size=pos.size,
                    entry_price=pos.entry_price,
                    unrealized_pnl=float(pos.unrealized_pnl or 0),
                )

        order_ok = False
        if snap and pos_pair:
            flatten = -snap.size if (snap.side or "buy").lower() == "buy" else snap.size
            signal = HedgeSignal(
                time.time(), 0.0, snap.size, flatten,
                "emergency", "EMERGENCY_UNWIND", False,
                pair=pos_pair,
            )
            order_ok = bool(await self._execute_hedge(signal, force=True))
        else:
            greeks = self.portfolio.compute_greeks()
            if abs(greeks.delta) > 0.001:
                signal = HedgeSignal(
                    time.time(), 0.0, greeks.delta, -greeks.delta,
                    "emergency", "EMERGENCY_UNWIND", False,
                    pair=self._active_pair,
                )
                order_ok = bool(await self._execute_hedge(signal, force=True))
                pos_pair = self._active_pair
            else:
                # Already flat locally
                async with self._state_lock:
                    self._dry_pos = None
                    self._pending_settle = False
                    self._active_risk = False
                    self._last_flatten_ts = time.time()
                self._running = False
                await self.store.set_engine_status("stopped")
                return

        if not self._live:
            async with self._state_lock:
                self._dry_pos = None
                self.cycle.clear_trade()
                self._margin_locked = 0.0
                self._pending_settle = False
                self._active_risk = False
                self._last_flatten_ts = time.time()
                self.portfolio.update_positions([])
            self._running = False
            await self.store.set_engine_status("stopped")
            return

        # LIVE: only clear SoT after consecutive flat confirms on exchange
        flat = False
        if order_ok and pos_pair:
            flat = await self._confirm_exchange_flat(pos_pair, reads=3, delay_sec=1.2)
        elif not order_ok:
            print("[EMERGENCY] Flatten order rejected/failed — not clearing local SoT")

        async with self._state_lock:
            if flat:
                self._dry_pos = None
                self.cycle.clear_trade()
                self._margin_locked = 0.0
                self._pending_settle = False
                self._active_risk = False
                self._flat_confirm_count = 0
                self._last_flatten_ts = time.time()
                self.portfolio.update_positions([])
                print("[EMERGENCY] Exchange flat confirmed — local SoT cleared")
                stop = True
                status = "stopped"
            else:
                # Restore / keep position as ACTIVE RISK — never orphan exchange exposure
                if snap and (not self._dry_pos or self._dry_pos.size <= 0):
                    self._dry_pos = snap
                    self.cycle.arm_trade(
                        snap.symbol,
                        snap.entry_price,
                        snap.size,
                        leverage=self._leverage_for(snap.symbol),
                    )
                    self.portfolio.update_positions([self._dry_pos])
                self._pending_settle = False
                self._active_risk = True
                self._flat_confirm_count = 0
                print(
                    "[EMERGENCY] Flatten NOT confirmed — position flagged ACTIVE RISK; "
                    "engine stays up for sync overlay (no silent orphan)"
                )
                stop = False
                status = "flatten_failed"

        await self.store.set_engine_status(status)
        if stop:
            self._running = False
        else:
            # Keep process alive so sync_positions continues to track exchange risk
            self._running = True
            try:
                await self.store.save_trade_plan(self.cycle.trade_plan_dict())
            except Exception:
                pass
            await self._log(
                "[EMERGENCY] ACTIVE RISK — manual flatten or retry kill required; "
                "entries blocked by kill switch"
            )

    async def run(self):
        self._running = True
        await self.bootstrap()
        print("\n" + "=" * 60)
        print("  RUBAIH v2")
        print("  Exchange: CoinDCX")
        print(f"  Strategy: {self._mode}")
        print(f"  Orders: {'LIVE' if self._live else 'DRY-RUN (set LIVE_TRADING=true)'}")
        print(f"  Scan: {'ON' if self._scan_enabled else 'OFF'} ({len(self._scan_pairs)} pairs)")
        print(f"  Active pair: {self._active_pair}")
        print(
            f"  Size: {CFG['trading'].get('margin_use_frac', 0.25):.0%}"
            f"–{CFG['trading'].get('margin_use_max_frac', 0.30):.0%} of free margin @ {self._leverage}x"
        )
        print(f"  Free ≈ ₹{self.cycle.free_capital_inr:.0f} (budget ₹{self.cycle.trade_margin_budget():.0f})")
        print(f"  AI: {'ENABLED' if self._ai_enabled else 'DISABLED'}")
        print(
            f"  Control bus: {'HMAC ON' if len(CMD_SECRET) >= 16 else 'HMAC OFF (set RUBAIH_API_TOKEN)'}"
        )
        print("=" * 60 + "\n")

        tasks = [
            self._supervised("command_listener", self.command_listener),
            self._supervised("ws_listener", self.ws_listener),
            self._supervised("price_poller", self.price_poller),
            self._supervised("sync_positions", self.sync_positions),
            self._supervised("main_loop", self.main_loop),
        ]
        if self._ai_enabled:
            tasks.append(self._supervised("ai_analysis_loop", self.ai_analysis_loop))

        await asyncio.gather(*tasks)

    async def shutdown(self):
        self._running = False
        try:
            await self.store.set_engine_status("stopped")
        except Exception:
            pass
        await self.ai.close()
        if self.client:
            await self.client.__aexit__(None, None, None)
        print("[RUBAIH] Shutdown complete")

if __name__ == "__main__":
    bot = RubaihBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n[RUBAIH] Interrupted")
        try:
            asyncio.run(bot.shutdown())
        except Exception:
            pass
    except Exception as e:
        print(f"[RUBAIH] Fatal: {e}")
        raise
