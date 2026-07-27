"""
================================================================================
RUBAIH v2 — CoinDCX INR-M Futures Cycle Bot with OpenRouter AI
================================================================================
⚠️  EDUCATIONAL / RESEARCH PURPOSES ONLY.
    Live CoinDCX trading only — no testnet mode.
    Default strategy: futures_cycle (flat → buy → sell on B-BTC_USDT).
================================================================================
"""

import os
import json
import asyncio
import hashlib
import hmac
import math
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Deque, Dict, List, Optional, Tuple

from dotenv import load_dotenv

load_dotenv()

import aiohttp
import asyncpg
import redis.asyncio as redis
import socketio
import yaml
from scipy.stats import norm

from openrouter_ai import OpenRouterAI, AIDecision

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

    async def _signed_post(self, path: str, body: Optional[Dict] = None) -> dict:
        extra = {k: v for k, v in dict(body or {}).items() if k != "timestamp"}
        body = {"timestamp": self._timestamp(), **extra}
        headers, payload = self.auth.sign(body)
        url = f"{REST_URL}{path}"
        async with self.session.post(
            url, data=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
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
                return data if isinstance(data, dict) else {}
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
                print(f"[API ERROR] {path} ({resp.status}): {data}")
            else:
                self._auth_errors = 0
                self._auth_ok = True
            return data if isinstance(data, dict) else {}

    async def verify_credentials(self) -> bool:
        """Probe /users/info with ms then seconds timestamp. Sets _auth_ok / _ts_mode."""
        for mode in ("ms", "s"):
            self._ts_mode = mode
            data = await self._signed_post("/exchange/v1/users/info", {})
            # success: dict with coindcx_id / email / first_name etc, not error status
            if isinstance(data, dict) and data.get("status") != "error" and data.get("code") not in (401, "401"):
                if data.get("coindcx_id") or data.get("email") or data.get("id") or "first_name" in data:
                    self._auth_ok = True
                    print(f"[AUTH] CoinDCX OK (timestamp={mode}): id={data.get('coindcx_id') or data.get('id') or 'ok'}")
                    return True
                # Some responses are nested
                if data and "message" not in data:
                    self._auth_ok = True
                    print(f"[AUTH] CoinDCX OK (timestamp={mode}): keys={list(data.keys())[:5]}")
                    return True
            print(f"[AUTH] users/info failed with timestamp={mode}: {data}")
        self._auth_ok = False
        self._ts_mode = "ms"
        print(
            "[AUTH] FAILED — CoinDCX Invalid credentials.\n"
            "  1) New key must be email-confirmed\n"
            "  2) Whitelist this VPS public IP exactly\n"
            "  3) Paste secret shown only once at create (no spaces)\n"
            "  4) docker compose up -d --force-recreate rubaih_engine\n"
            "  Dry-run cycle can still run; LIVE orders blocked until auth works."
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
    ) -> Dict:
        order = {
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
        # CoinDCX: do not include time_in_force for market orders
        if order_type != "market":
            order["time_in_force"] = "good_till_cancel"

        return await self._signed_post(
            "/exchange/v1/derivatives/futures/orders/create",
            {"order": order},
        )

    async def cancel_all_orders(self) -> Dict:
        return await self._signed_post(
            "/exchange/v1/derivatives/futures/positions/cancel_all_open_orders",
            {"margin_currency_short_name": [self.margin]},
        )

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
# FUTURES CYCLE STRATEGIST (flat → buy → sell)
# ==============================================================================
class FuturesCycleStrategist:
    """Long-only micro scalper: enter on short momentum, exit TP/SL/timeout."""

    def __init__(self):
        tcfg = CFG["trading"]
        scfg = tcfg.get("strategy") or {}
        self.entry_cooldown = float(scfg.get("entry_cooldown_sec", 120))
        self.lookback = int(scfg.get("momentum_lookback", 30))
        self.entry_move_pct = float(scfg.get("entry_move_pct", 0.0015))
        self.take_profit_pct = float(scfg.get("take_profit_pct", 0.004))
        self.stop_loss_pct = float(scfg.get("stop_loss_pct", 0.003))
        self.max_hold_sec = float(scfg.get("max_hold_sec", 1800))
        self.allow_short = bool(scfg.get("allow_short", False))
        self.target_size = float(scfg.get("position_size_btc", tcfg.get("max_order_size_btc", 0.001)))
        self.capital_inr = float(tcfg.get("capital_inr", 1000))
        self.leverage = int(tcfg.get("leverage", 10))
        self.usdt_inr = float(tcfg.get("usdt_inr", 87))
        self.margin_buffer = float(tcfg.get("margin_buffer", 0.7))
        self.min_interval = float(tcfg.get("min_hedge_interval_sec", 60))
        self._last_signal = 0.0
        self._entry_ts = 0.0
        self._prices: Deque[float] = deque(maxlen=max(self.lookback + 5, 40))

    def push_price(self, mid: float):
        if mid and mid > 0:
            self._prices.append(float(mid))

    def affordable_qty(self, spot: float, min_qty: float) -> float:
        """Max BTC size from INR capital / leverage / FX, floored to target."""
        if spot <= 0 or self.usdt_inr <= 0 or self.leverage <= 0:
            return 0.0
        usable_inr = self.capital_inr * self.margin_buffer
        # margin_inr ≈ size * spot_usdt * usdt_inr / leverage
        max_size = (usable_inr * self.leverage) / (spot * self.usdt_inr)
        qty = min(self.target_size, max_size)
        if qty + 1e-12 < min_qty:
            return 0.0
        return qty

    def evaluate(
        self,
        spot: float,
        position: Optional[Position],
        min_qty: float,
    ) -> Optional[HedgeSignal]:
        now = time.time()
        if spot <= 0:
            return None
        if now - self._last_signal < self.min_interval:
            return None

        # ----- manage open long -----
        if position and position.size > 0:
            side = (position.side or "buy").lower()
            entry = position.entry_price or 0.0
            size = position.size
            if side == "buy" and entry > 0:
                if self._entry_ts <= 0:
                    self._entry_ts = now
                pnl_pct = (spot - entry) / entry
                held = now - self._entry_ts
                if pnl_pct >= self.take_profit_pct:
                    self._last_signal = now
                    return HedgeSignal(
                        now, 0.0, size, -size, "immediate",
                        f"EXIT_TP: +{pnl_pct:.2%} size={size}", False,
                    )
                if pnl_pct <= -self.stop_loss_pct:
                    self._last_signal = now
                    return HedgeSignal(
                        now, 0.0, size, -size, "immediate",
                        f"EXIT_SL: {pnl_pct:.2%} size={size}", False,
                    )
                if held >= self.max_hold_sec:
                    self._last_signal = now
                    return HedgeSignal(
                        now, 0.0, size, -size, "passive",
                        f"EXIT_TIMEOUT: held={held:.0f}s size={size}", False,
                    )
                return None
            # Unexpected short while allow_short=false → flatten
            if side == "sell":
                self._last_signal = now
                return HedgeSignal(
                    now, 0.0, -size, size, "immediate",
                    f"EXIT_FLATTEN_SHORT: size={size}", False,
                )
            return None

        # ----- flat: look for long entry -----
        self._entry_ts = 0.0
        if now - self._last_signal < self.entry_cooldown:
            return None
        if len(self._prices) < max(5, self.lookback // 2):
            return None

        hist = list(self._prices)
        look = hist[-self.lookback:] if len(hist) >= self.lookback else hist
        base = look[0]
        if base <= 0:
            return None
        move = (spot - base) / base
        if move < self.entry_move_pct:
            return None

        qty = self.affordable_qty(spot, min_qty)
        if qty <= 0:
            if now - self._last_signal > 60:
                print(
                    f"[CYCLE] Skip entry — cannot afford min {min_qty} BTC "
                    f"with ₹{self.capital_inr} @ {self.leverage}x (spot={spot:.1f})"
                )
                self._last_signal = now
            return None

        self._last_signal = now
        self._entry_ts = now
        return HedgeSignal(
            now, qty, 0.0, qty, "passive",
            f"ENTRY_LONG: move={move:.2%} size={qty}", False,
        )


# ==============================================================================
# RISK MANAGER
# ==============================================================================
class RiskManager:
    def __init__(self):
        cfg = CFG["trading"]
        self.max_delta = cfg["max_delta"]
        self.max_vega = cfg["max_vega"]
        self.max_dd = cfg["max_drawdown_pct"]
        self._hwm = 0.0
        self._kill = False
        self._order_ts: List[float] = []

    def check(self, greeks: GreeksSnapshot, pnl: float) -> Optional[str]:
        if self._kill:
            return "KILL_SWITCH_ACTIVE"
        self._hwm = max(self._hwm, pnl)
        dd = self._hwm - pnl
        if self._hwm > 0 and dd / max(self._hwm, 1) > self.max_dd:
            self._kill = True
            return f"MAX_DRAWDOWN: {dd/self._hwm:.1%}"
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

    async def save_greeks(self, g: GreeksSnapshot, spot: float, session_pnl: float = 0.0):
        await self.pg.execute(
            "INSERT INTO greeks_snapshots (delta, gamma, vega, theta, spot_price) VALUES ($1,$2,$3,$4,$5)",
            g.delta, g.gamma, g.vega, g.theta, spot
        )
        await self.rd.set("rubaih:session_pnl", str(session_pnl))
        await self.rd.publish("rubaih:greeks", json.dumps({
            "timestamp": g.timestamp, "delta": g.delta, "gamma": g.gamma,
            "vega": g.vega, "theta": g.theta, "spot": spot, "session_pnl": session_pnl
        }))

    async def set_engine_status(self, status: str):
        await self.rd.set("rubaih:engine_status", status)
        await self.rd.publish("rubaih:status", json.dumps({"status": status, "ts": time.time()}))

    async def save_hedge(self, signal: HedgeSignal, price: float):
        side = "buy" if signal.hedge_size > 0 else "sell"
        await self.pg.execute(
            "INSERT INTO hedge_trades (symbol, side, size, price, reason, ai_augmented) VALUES ($1,$2,$3,$4,$5,$6)",
            CFG["trading"]["perp_symbol"], side, abs(signal.hedge_size), price, signal.reason, signal.ai_augmented
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
        self._ai_enabled = bool(os.getenv("OPENROUTER_API_KEY", "").strip())
        self._hedge_history: List[Dict] = []
        self._leverage = int(CFG["trading"].get("leverage", 15))
        self._live = LIVE_TRADING
        self._mode = str(CFG["trading"].get("mode", "futures_cycle")).strip().lower()
        self._dry_pos: Optional[Position] = None  # simulated fill when LIVE_TRADING=false

    def _round_qty(self, size: float, product: CoinDCXProduct) -> float:
        step = product.quantity_increment or 0.001
        if step <= 0:
            return size
        rounded = math.floor(size / step) * step
        # avoid float dust
        decimals = max(0, min(8, int(round(-math.log10(step))) if step < 1 else 0))
        return round(rounded, decimals)

    async def _seed_settings(self):
        cfg = CFG["trading"]
        defaults = {
            "mode": self._mode,
            "delta_threshold": str(cfg["delta_threshold"]),
            "max_delta": str(cfg["max_delta"]),
            "max_vega": str(cfg["max_vega"]),
            "max_drawdown_pct": str(cfg["max_drawdown_pct"]),
            "capital_inr": str(cfg.get("capital_inr", 0)),
            "leverage": str(cfg.get("leverage", 15)),
            "live_trading": str(self._live).lower(),
            "exchange": "coindcx",
            "margin_currency": MARGIN_CCY,
            "perp_symbol": cfg["perp_symbol"],
        }
        # Always refresh sizing/mode from config.yaml so stale Redis (e.g. lev=5) cannot block entries
        force_keys = ("mode", "capital_inr", "leverage", "perp_symbol", "margin_currency", "live_trading")
        existing = await self.store.rd.hgetall("rubaih:settings")
        merged = dict(existing or {})
        merged.update(defaults)  # config wins for all defaults we care about
        await self.store.rd.hset("rubaih:settings", mapping={k: merged[k] for k in defaults})
        # Apply Redis risk knobs if present, but keep forced sizing from config
        await self._apply_settings(merged)
        for k in force_keys:
            if k == "leverage":
                self._leverage = int(float(defaults["leverage"]))
                self.cycle.leverage = self._leverage
            elif k == "capital_inr":
                self.cycle.capital_inr = float(defaults["capital_inr"])
            elif k == "mode":
                self._mode = str(defaults["mode"]).strip().lower()
        self.cycle.margin_buffer = float(cfg.get("margin_buffer", 0.85))
        self.cycle.usdt_inr = float(cfg.get("usdt_inr", 87))
        print(f"[SETTINGS] Forced from config: mode={self._mode} capital=₹{self.cycle.capital_inr} lev={self._leverage}x")

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
        if "capital_inr" in data:
            self.cycle.capital_inr = float(data["capital_inr"])
        if "leverage" in data:
            self._leverage = int(float(data["leverage"]))
            self.cycle.leverage = self._leverage
        print(
            f"[SETTINGS] mode={self._mode} threshold={self.strategist.delta_threshold} "
            f"max_delta={self.risk.max_delta} max_vega={self.risk.max_vega} "
            f"max_dd={self.risk.max_dd} capital_inr={self.cycle.capital_inr} lev={self._leverage}"
        )
    async def command_listener(self):
        """Honor kill-switch / settings from authenticated API."""
        pubsub = self.store.rd.pubsub()
        await pubsub.subscribe("rubaih:command")
        print("[CMD] Listening on rubaih:command")
        try:
            while self._running:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if not message or not message.get("data"):
                    await asyncio.sleep(0.05)
                    continue
                try:
                    payload = json.loads(message["data"])
                except Exception:
                    continue
                cmd = payload.get("command")
                if cmd == "KILL_SWITCH":
                    reason = f"API_KILL_SWITCH from {payload.get('source', 'unknown')}"
                    print(f"[CMD] {reason}")
                    self.risk.trigger_kill(reason)
                    await self.store.save_risk_event("KILL_SWITCH", reason)
                    await self.store.set_engine_status("kill_switch")
                    await self._emergency_unwind()
                    break
                if cmd == "UPDATE_SETTINGS":
                    data = payload.get("data") or {}
                    await self._apply_settings(data)
                    await self.store.rd.hset("rubaih:settings", mapping={k: str(v) for k, v in data.items()})
                    await self.store.save_risk_event("SETTINGS_UPDATE", json.dumps(data))
        finally:
            try:
                await pubsub.unsubscribe()
                await pubsub.close()
            except Exception:
                pass

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

        instruments = await self.client.get_active_instruments()
        target = CFG["trading"]["perp_symbol"]
        underlyings = {CFG["trading"]["underlying"]}

        # Always load configured hedge instrument + a small set of related perps
        to_load = [target]
        for pair in instruments:
            if not isinstance(pair, str):
                continue
            # e.g. B-BTC_USDT → BTC
            parts = pair.replace("B-", "").replace("I-", "").split("_")
            if parts and parts[0] in underlyings and pair not in to_load:
                to_load.append(pair)
        to_load = to_load[:20]

        for pair in to_load:
            try:
                details = await self.client.get_instrument(pair)
                inst = details.get("instrument", details) if isinstance(details, dict) else {}
                underlying = inst.get("underlying_currency_short_name") or inst.get("position_currency_short_name") or "BTC"
                prod = CoinDCXProduct(
                    pair=pair,
                    symbol=pair,
                    underlying=underlying,
                    is_perp=True,
                    contract_value=float(inst.get("unit_contract_value", 1.0) or 1.0),
                    quantity_increment=float(inst.get("quantity_increment", 0.001) or 0.001),
                    min_quantity=float(inst.get("min_quantity", 0.001) or 0.001),
                )
                self.products[pair] = prod
                self.portfolio.update_product(prod)
            except Exception as e:
                print(f"[RUBAIH] Failed to load instrument {pair}: {e}")

        if target not in self.products:
            # Fallback so hedging can still resolve the pair
            prod = CoinDCXProduct(pair=target, symbol=target, underlying=CFG["trading"]["underlying"])
            self.products[target] = prod
            self.portfolio.update_product(prod)

        self.pricing.update_surface("BTC", 0.55, -0.15, 0.08)
        self.pricing.update_surface("ETH", 0.60, -0.18, 0.10)
        print(f"[RUBAIH] Loaded {len(self.products)} CoinDCX instruments")
        print(f"[RUBAIH] Mode: {self._mode}")
        print(f"[RUBAIH] Hedge pair: {target}")
        print(f"[RUBAIH] AI augmentation: {'ENABLED' if self._ai_enabled else 'DISABLED'}")
        print(f"[RUBAIH] LIVE_TRADING: {'ON — real orders' if self._live else 'OFF — dry-run only'}")
        print(f"[RUBAIH] Capital target: ₹{CFG['trading'].get('capital_inr', '?')} INR-M @ {self._leverage}x")
        print(f"[RUBAIH] Margin currency: {MARGIN_CCY}")
        prod = self.products.get(target)
        spot_guess = 100000.0
        min_q = prod.min_quantity if prod else 0.001
        afford = self.cycle.affordable_qty(spot_guess, min_q)
        print(
            f"[RUBAIH] Cycle size estimate @ {spot_guess:.0f} USDT: "
            f"{afford if afford else 'NONE (raise leverage or capital)'} BTC (min={min_q})"
        )
        await self.store.set_engine_status("running" if self._live else "dry_run")

    async def ws_listener(self):
        """CoinDCX public socket.io orderbook stream for hedge pair (futures channel)."""
        pair = CFG["trading"]["perp_symbol"]
        # Spot: {pair}@orderbook@20 — Futures: {pair}@orderbook@20-futures
        channel = f"{pair}@orderbook@20-futures"
        self._ob_parse_errors = 0

        while self._running:
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
                    await asyncio.sleep(1)
            except Exception as e:
                print(f"[WS] Error: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)
            finally:
                try:
                    await sio.disconnect()
                except Exception:
                    pass

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
                prod = self.products.get(pair)
                if prod:
                    self.portfolio.update_spot(prod.underlying, mid)
                self.cycle.push_price(mid)
                self._ob_parse_errors = 0
        except Exception as e:
            self._ob_parse_errors = getattr(self, "_ob_parse_errors", 0) + 1
            if self._ob_parse_errors <= 3 or self._ob_parse_errors % 100 == 0:
                print(f"[WS] Orderbook parse error ({type(e).__name__}): {e!r}")

    async def price_poller(self):
        """REST orderbook fallback if socket drops."""
        pair = CFG["trading"]["perp_symbol"]
        while self._running:
            try:
                book = await self.client.get_orderbook(pair)
                await self._on_orderbook(pair, book)
            except Exception as e:
                print(f"[PRICE] Error: {e}")
            await asyncio.sleep(2)

    async def sync_positions(self):
        """Sync only the configured hedge pair — never ingest SLX / other books."""
        target = CFG["trading"]["perp_symbol"]
        while self._running:
            try:
                # Dry-run cycle: keep simulated position; refresh mark/PnL only
                if not self._live and self._mode == "futures_cycle" and self._dry_pos:
                    spot = self.portfolio.spot_prices.get(CFG["trading"]["underlying"], 0.0)
                    if spot > 0 and self._dry_pos.entry_price > 0:
                        direction = 1.0 if self._dry_pos.side == "buy" else -1.0
                        self._dry_pos.unrealized_pnl = (
                            (spot - self._dry_pos.entry_price) * self._dry_pos.size * direction
                        )
                    self.portfolio.update_positions([self._dry_pos])
                    await asyncio.sleep(10)
                    continue

                raw = await self.client.get_positions(pairs=target)
                if not raw:
                    raw = await self.client.get_positions()

                positions = []
                for rp in raw:
                    pair = rp.get("pair", "")
                    if pair and pair != target:
                        continue
                    active = float(rp.get("active_pos", 0) or 0)
                    if active == 0:
                        continue
                    if not pair:
                        pair = target
                    side = "buy" if active > 0 else "sell"
                    mark = float(rp.get("mark_price", 0) or 0)
                    avg = float(rp.get("avg_price", 0) or 0)
                    if mark > 0 and pair in self.products:
                        self.portfolio.update_spot(self.products[pair].underlying, mark)
                        self.cycle.push_price(mark)
                    upnl = 0.0
                    if mark > 0 and avg > 0:
                        upnl = (mark - avg) * active
                    positions.append(Position(
                        symbol=pair,
                        product_id=str(rp.get("id", pair)),
                        side=side,
                        size=abs(active),
                        entry_price=avg,
                        unrealized_pnl=upnl,
                    ))
                self.portfolio.update_positions(positions)
            except Exception as e:
                print(f"[SYNC] Error: {e}")
            await asyncio.sleep(10)

    async def ai_analysis_loop(self):
        """Periodic AI analysis — runs every 60 seconds."""
        if not self._ai_enabled:
            return
        while self._running and self.risk.alive:
            try:
                greeks = self.portfolio.compute_greeks()
                spot = self.portfolio.spot_prices.get(CFG["trading"]["underlying"], 0.0)
                if spot <= 0:
                    await asyncio.sleep(5)
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
                    print(f"[AI] {decision.model_used}: {decision.action} ({decision.confidence:.0%} confidence)")

                    if decision.action == "EMERGENCY" and decision.confidence > 0.95:
                        self.risk.trigger_kill(f"AI_EMERGENCY: {decision.reasoning}")
                        await self.store.save_risk_event("AI_EMERGENCY", decision.reasoning)
                        await self.store.set_engine_status("kill_switch")
                        await self._emergency_unwind()
                        break
            except Exception as e:
                print(f"[AI LOOP] Error: {e}")
            await asyncio.sleep(60)

    async def main_loop(self):
        target = CFG["trading"]["perp_symbol"]
        while self._running and self.risk.alive:
            try:
                greeks = self.portfolio.compute_greeks()
                spot = self.portfolio.spot_prices.get(CFG["trading"]["underlying"], 0.0)
                if spot <= 0:
                    await asyncio.sleep(1)
                    continue

                session_pnl = sum(p.unrealized_pnl for p in self.portfolio.positions.values())
                await self.store.save_greeks(greeks, spot, session_pnl)

                violation = self.risk.check(greeks, session_pnl)
                if violation:
                    await self.store.save_risk_event("VIOLATION", violation)
                    await self.store.set_engine_status("halted")
                    await self._emergency_unwind()
                    break

                if self._mode == "futures_cycle":
                    pos = self.portfolio.positions.get(target) or self._dry_pos
                    prod = self.products.get(target)
                    min_q = prod.min_quantity if prod else 0.001
                    signal = self.cycle.evaluate(spot, pos, min_q)
                    if signal and signal.hedge_size != 0:
                        print(f"[CYCLE] {signal.reason}")
                        await self._execute_hedge(signal)
                else:
                    signal = self.strategist.evaluate(greeks, spot)
                    if signal and signal.hedge_size != 0:
                        print(f"[HEDGE] {signal.reason}")
                        await self._execute_hedge(signal)
            except Exception as e:
                print(f"[LOOP] Error: {e}")
            await asyncio.sleep(1)

    async def _execute_hedge(self, signal: HedgeSignal, force: bool = False):
        if not force and not self.risk.alive:
            print("[HEDGE] Blocked — kill switch active")
            return
        if not self.risk.rate_limit_ok() and not force:
            print("[HEDGE] Rate limited")
            return
        perp_symbol = CFG["trading"]["perp_symbol"]
        perp_prod = self.products.get(perp_symbol)
        if not perp_prod:
            print(f"[HEDGE] Perp {perp_symbol} not found")
            return

        side = Side.BUY if signal.hedge_size > 0 else Side.SELL
        size = abs(signal.hedge_size)
        cfg = CFG["trading"]
        spot = self.portfolio.spot_prices.get(cfg["underlying"], 0.0)

        async def _place(qty: float):
            if qty < perp_prod.min_quantity:
                print(f"[HEDGE] Size {qty} below min {perp_prod.min_quantity}")
                return False
            if not self._live:
                print(f"[DRY-RUN] Would {side.value} {qty} {perp_symbol} @ ~{spot}")
                await self.store.save_hedge(signal, spot)
                self._apply_dry_fill(perp_symbol, side, qty, spot)
                return True
            result = await self.client.place_order(perp_prod.pair, side, qty, "market", self._leverage)
            print(f"[HEDGE] LIVE order: {result}")
            await self.store.save_hedge(signal, spot)
            return True

        if size > cfg["max_order_size_btc"]:
            slices = max(1, int(math.ceil(size / cfg["max_order_size_btc"])))
            slices = min(slices, cfg.get("twap_slices", 3) if not force else max(slices, 1))
            slice_size = self._round_qty(size / slices, perp_prod)
            print(f"[HEDGE] TWAP: {slices} slices of {slice_size} on {perp_symbol}{' (force)' if force else ''}")
            for i in range(slices):
                await _place(slice_size)
                if i < slices - 1:
                    await asyncio.sleep(cfg["twap_interval_sec"] if not force else max(2, cfg["twap_interval_sec"] // 2))
        else:
            qty = self._round_qty(size, perp_prod)
            await _place(qty)

    def _apply_dry_fill(self, symbol: str, side: Side, qty: float, price: float):
        """Simulate position so futures_cycle can exercise exits in dry-run."""
        if self._mode != "futures_cycle" or qty <= 0:
            return
        pos = self._dry_pos
        if side == Side.BUY:
            if pos and pos.side == "buy":
                new_size = pos.size + qty
                pos.entry_price = ((pos.entry_price * pos.size) + price * qty) / new_size
                pos.size = new_size
            else:
                self._dry_pos = Position(
                    symbol=symbol, product_id=f"dry-{symbol}", side="buy",
                    size=qty, entry_price=price, unrealized_pnl=0.0,
                )
        else:  # SELL
            if pos and pos.side == "buy":
                remain = pos.size - qty
                if remain <= 1e-12:
                    self._dry_pos = None
                    self.cycle._entry_ts = 0.0
                else:
                    pos.size = remain
            else:
                # opening short not used in long-only; clear
                self._dry_pos = None
        if self._dry_pos:
            self.portfolio.update_positions([self._dry_pos])
        else:
            self.portfolio.update_positions([])

    async def _emergency_unwind(self):
        print("[EMERGENCY] Flattening...")
        try:
            if self._live and self.client:
                await self.client.cancel_all_orders()
            else:
                print("[DRY-RUN] Would cancel all open orders")
        except Exception as e:
            print(f"[EMERGENCY] cancel_all failed: {e}")
        greeks = self.portfolio.compute_greeks()
        if abs(greeks.delta) > 0.001:
            signal = HedgeSignal(
                time.time(), 0.0, greeks.delta, -greeks.delta,
                "emergency", "EMERGENCY_UNWIND", False,
            )
            await self._execute_hedge(signal, force=True)
        self._dry_pos = None
        self._running = False
        await self.store.set_engine_status("stopped")

    async def run(self):
        self._running = True
        await self.bootstrap()
        print("\n" + "=" * 60)
        print("  RUBAIH v2")
        print("  Exchange: CoinDCX")
        print(f"  Strategy: {self._mode}")
        print(f"  Orders: {'LIVE' if self._live else 'DRY-RUN (set LIVE_TRADING=true)'}")
        print(f"  Underlying: {CFG['trading']['underlying']}")
        print(f"  Pair: {CFG['trading']['perp_symbol']}")
        print(f"  AI: {'ENABLED' if self._ai_enabled else 'DISABLED'}")
        print("=" * 60 + "\n")

        tasks = [
            self.command_listener(),
            self.ws_listener(),
            self.price_poller(),
            self.sync_positions(),
            self.main_loop(),
        ]
        if self._ai_enabled:
            tasks.append(self.ai_analysis_loop())

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
