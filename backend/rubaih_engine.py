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

from openrouter_ai import OpenRouterAI, AIDecision, ai_configured

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

    async def _signed_post(self, path: str, body: Optional[Dict] = None):
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
                print(f"[API ERROR] {path} ({resp.status}): {data}")
            else:
                self._auth_errors = 0
                self._auth_ok = True
            # CoinDCX futures/positions returns a JSON array — do not coerce to {}
            return data

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
# FUTURES CYCLE STRATEGIST (flat → scan → buy → sell)
# ==============================================================================
class FuturesCycleStrategist:
    """Long-only micro scalper with multi-pair momentum scan."""

    def __init__(self):
        tcfg = CFG["trading"]
        scfg = tcfg.get("strategy") or {}
        self.entry_cooldown = float(scfg.get("entry_cooldown_sec", 90))
        self.lookback = int(scfg.get("momentum_lookback", 24))
        self.entry_move_pct = float(scfg.get("entry_move_pct", 0.0012))
        self.take_profit_pct = float(scfg.get("take_profit_pct", 0.004))
        self.stop_loss_pct = float(scfg.get("stop_loss_pct", 0.0025))
        self.max_loss_inr = float(scfg.get("max_loss_inr", 200))
        self.max_hold_sec = float(scfg.get("max_hold_sec", 1200))
        self.allow_short = bool(scfg.get("allow_short", False))
        self.target_margin_inr = float(tcfg.get("target_margin_inr", 1200))
        self.max_margin_inr = float(tcfg.get("max_margin_inr", 1500))
        self.target_size = float(scfg.get("position_size_btc", 0) or 0)
        self.capital_inr = float(tcfg.get("capital_inr", 5000))
        self.leverage = int(tcfg.get("leverage", 10))
        self.usdt_inr = float(tcfg.get("usdt_inr", 87))
        self.margin_buffer = float(tcfg.get("margin_buffer", 0.85))
        self.taker_fee = float(CFG.get("exchange", {}).get("taker_fee", 0.00075))
        self.min_interval = float(tcfg.get("min_hedge_interval_sec", 45))
        self.trail_giveback_inr = float(scfg.get("profit_trail_giveback_inr", 50))
        self.trail_arm_inr = float(scfg.get("profit_trail_arm_inr", 100))
        self._last_signal = 0.0
        self._entry_ts = 0.0
        self._peak_pnl_inr = 0.0
        self._peak_price = 0.0
        self._entry_price = 0.0
        self._tp_price = 0.0
        self._sl_price = 0.0
        self._plan_pair: Optional[str] = None
        self._plan_size = 0.0
        self._prices: Dict[str, Deque[float]] = {}
        self._last_scan_log = 0.0
        self._last_entry_pair: Optional[str] = None
        self._last_scan_msg: Optional[str] = None
        self._last_hold_log = 0.0

    def _buf(self, pair: str) -> Deque[float]:
        if pair not in self._prices:
            self._prices[pair] = deque(maxlen=max(self.lookback + 5, 40))
        return self._prices[pair]

    def push_price(self, mid: float, pair: Optional[str] = None):
        if mid and mid > 0:
            key = pair or CFG["trading"]["perp_symbol"]
            self._buf(key).append(float(mid))

    def affordable_qty(self, spot: float, min_qty: float) -> float:
        """Size from target INR margin × leverage. Never exceed max_margin_inr."""
        if spot <= 0 or self.usdt_inr <= 0 or self.leverage <= 0:
            return 0.0
        usable_inr = self.capital_inr * self.margin_buffer
        hard_cap = min(self.max_margin_inr, usable_inr)
        target_margin = min(self.target_margin_inr, hard_cap)
        if target_margin <= 0:
            return 0.0
        notional_inr = target_margin * self.leverage
        qty = notional_inr / (spot * self.usdt_inr)
        max_qty = (hard_cap * self.leverage) / (spot * self.usdt_inr)
        qty = min(qty, max_qty)
        if self.target_size > 0:
            qty = min(qty, self.target_size)
        if qty + 1e-12 < min_qty:
            min_margin = (min_qty * spot * self.usdt_inr) / self.leverage
            # Do NOT bump into a huge lot — skip pair if min lot > hard cap
            if min_margin <= hard_cap * 1.001:
                return float(min_qty)
            return 0.0
        # Final guard
        margin = (qty * spot * self.usdt_inr) / self.leverage
        if margin > hard_cap * 1.02:
            qty = (hard_cap * self.leverage) / (spot * self.usdt_inr)
            if qty + 1e-12 < min_qty:
                return 0.0
        return qty

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
        """Unrealized PnL in INR (USDT-quoted futures × FX)."""
        return (spot - entry) * size * self.usdt_inr

    def arm_trade(self, pair: str, entry: float, size: float):
        """Lock TP/SL prices at buy time — checked every tick afterward."""
        if entry <= 0 or size <= 0:
            return
        self._plan_pair = pair
        self._plan_size = size
        self._entry_price = entry
        self._entry_ts = time.time()
        self._tp_price = entry * (1.0 + self.take_profit_pct)
        self._sl_price = entry * (1.0 - self.stop_loss_pct)
        self._peak_price = entry
        self._peak_pnl_inr = 0.0
        self._last_entry_pair = pair
        print(
            f"[PLAN] {pair} entry={entry:.4f} size={size} "
            f"TP={self._tp_price:.4f} (+{self.take_profit_pct:.2%}) "
            f"SL={self._sl_price:.4f} (-{self.stop_loss_pct:.2%}) "
            f"trail_arm=₹{self.trail_arm_inr:.0f} giveback=₹{self.trail_giveback_inr:.0f} "
            f"max_loss=₹{self.max_loss_inr:.0f}"
        )

    def clear_trade(self):
        self._plan_pair = None
        self._plan_size = 0.0
        self._entry_price = 0.0
        self._tp_price = 0.0
        self._sl_price = 0.0
        self._peak_price = 0.0
        self._peak_pnl_inr = 0.0
        self._entry_ts = 0.0

    def trade_plan_dict(self) -> Dict:
        return {
            "pair": self._plan_pair,
            "entry": self._entry_price,
            "tp": self._tp_price,
            "sl": self._sl_price,
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
        self._peak_pnl_inr = float(data.get("peak_pnl_inr") or 0)
        self._peak_price = float(data.get("peak_price") or self._entry_price or 0)
        self._plan_size = float(data.get("size") or 0)
        self._entry_ts = float(data.get("entry_ts") or 0)

    def pick_entry(
        self,
        pair_mids: Dict[str, float],
        min_qty_by_pair: Dict[str, float],
    ) -> Optional[HedgeSignal]:
        """When flat: rank scanned pairs by momentum and pick best long."""
        now = time.time()
        if now - self._last_signal < max(self.min_interval, self.entry_cooldown):
            return None

        ranked = []
        skipped_size = 0
        skipped_move = 0
        warming = 0
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
            qty = self.affordable_qty(spot, min_q)
            if qty <= 0:
                skipped_size += 1
                continue
            score = move
            if pair == self._last_entry_pair:
                score *= 0.85
            margin_inr = (qty * spot * self.usdt_inr) / max(self.leverage, 1)
            ranked.append((score, move, pair, spot, qty, margin_inr))

        if now - self._last_scan_log > 30:
            self._last_scan_log = now
            top = ", ".join(
                f"{p}:{m:.2%}~₹{mar:.0f}" for _s, m, p, _sp, _q, mar in sorted(ranked, reverse=True)[:5]
            ) or "none"
            self._last_scan_msg = (
                f"[SCAN] candidates={len(pair_mids)} qualifying={len(ranked)} "
                f"warm={warming} flat={skipped_move} unaffordable={skipped_size} top=[{top}]"
            )
            print(self._last_scan_msg)

        if not ranked:
            return None

        ranked.sort(reverse=True)
        _score, move, pair, spot, qty, margin_inr = ranked[0]
        notional_inr = qty * spot * self.usdt_inr
        fee_inr = notional_inr * self.taker_fee * 2
        tp_inr = notional_inr * self.take_profit_pct
        if tp_inr < fee_inr * 1.3:
            print(
                f"[SCAN] skip {pair}: TP ₹{tp_inr:.0f} < 1.3× fees ₹{fee_inr:.0f}"
            )
            return None

        self._last_signal = now
        # TP/SL locked in arm_trade() right after fill with real entry price
        return HedgeSignal(
            now, qty, 0.0, qty, "passive",
            f"ENTRY_LONG: {pair} move={move:.2%} size={qty:.6f} margin~₹{margin_inr:.0f}",
            False,
            pair=pair,
        )

    def evaluate_exit(
        self,
        spot: float,
        position: Optional[Position],
        pair: str,
    ) -> Optional[HedgeSignal]:
        """Exit using locked TP/SL prices + INR trail + hard max-loss.

        Never blocked by entry cooldown / min_interval.
        """
        now = time.time()
        if spot <= 0 or not position or position.size <= 0:
            return None

        side = (position.side or "buy").lower()
        entry = position.entry_price or self._entry_price or 0.0
        size = position.size
        if side != "buy" or entry <= 0:
            if side == "sell":
                self.clear_trade()
                self._last_signal = now
                return HedgeSignal(
                    now, 0.0, -size, size, "immediate",
                    f"EXIT_FLATTEN_SHORT: {pair} size={size}", False, pair=pair,
                )
            return None

        # Ensure plan exists (e.g. after restart with open position)
        if self._tp_price <= 0 or self._sl_price <= 0 or self._plan_pair != pair:
            self.arm_trade(pair, entry, size)

        if self._entry_ts <= 0:
            self._entry_ts = now

        pnl = self.pnl_inr(entry, spot, size)
        # Prefer exchange-reported uPnL if present (often USDT); convert if huge vs our est
        if position.unrealized_pnl:
            exch = float(position.unrealized_pnl)
            # Heuristic: values with |exch| similar to INR already; else treat as USDT
            if abs(exch) > abs(pnl) * 3 and abs(exch) > 20:
                pnl = exch  # already INR-like
            elif abs(exch) * self.usdt_inr > abs(pnl) * 0.5:
                pnl = exch * self.usdt_inr

        self._peak_pnl_inr = max(self._peak_pnl_inr, pnl)
        self._peak_price = max(self._peak_price, spot)
        giveback = self._peak_pnl_inr - pnl
        # Price trail equivalent to ₹50 giveback
        price_giveback_usdt = self.trail_giveback_inr / max(size * self.usdt_inr, 1e-9)
        price_drop = self._peak_price - spot
        held = now - self._entry_ts

        if now - self._last_hold_log > 8:
            self._last_hold_log = now
            print(
                f"[HOLD] {pair} spot={spot:.4f} pnl=₹{pnl:.0f} peak=₹{self._peak_pnl_inr:.0f} "
                f"giveback=₹{giveback:.0f} TP={self._tp_price:.4f} SL={self._sl_price:.4f}"
            )

        def _exit(reason: str) -> HedgeSignal:
            self._last_signal = now
            self.clear_trade()
            return HedgeSignal(
                now, 0.0, size, -size, "immediate", reason, False, pair=pair,
            )

        # 1) Locked TP price
        if spot >= self._tp_price:
            return _exit(
                f"EXIT_TP: {pair} spot={spot:.4f}>=TP={self._tp_price:.4f} pnl=₹{pnl:.0f}"
            )
        # 2) Locked SL price
        if spot <= self._sl_price:
            return _exit(
                f"EXIT_SL: {pair} spot={spot:.4f}<=SL={self._sl_price:.4f} pnl=₹{pnl:.0f}"
            )
        # 3) Hard INR loss cap (anti-liquidation)
        if pnl <= -abs(self.max_loss_inr):
            return _exit(
                f"EXIT_MAXLOSS: {pair} pnl=₹{pnl:.0f} <= -₹{self.max_loss_inr:.0f}"
            )
        # 4) Trail giveback ₹50 after arming
        trail_hit = (
            self._peak_pnl_inr >= self.trail_arm_inr
            and (
                giveback >= self.trail_giveback_inr
                or price_drop >= price_giveback_usdt
            )
        )
        if trail_hit:
            return _exit(
                f"EXIT_TRAIL: {pair} peak=₹{self._peak_pnl_inr:.0f} now=₹{pnl:.0f} "
                f"giveback=₹{giveback:.0f} price_drop={price_drop:.4f}"
            )
        # 5) % fallback (in case plan prices drifted)
        pnl_pct = (spot - entry) / entry
        if pnl_pct >= self.take_profit_pct:
            return _exit(f"EXIT_TP_PCT: {pair} +{pnl_pct:.2%} pnl=₹{pnl:.0f}")
        if pnl_pct <= -self.stop_loss_pct:
            return _exit(f"EXIT_SL_PCT: {pair} {pnl_pct:.2%} pnl=₹{pnl:.0f}")
        if held >= self.max_hold_sec:
            return _exit(f"EXIT_TIMEOUT: {pair} held={held:.0f}s pnl=₹{pnl:.0f}")
        return None

    def evaluate(
        self,
        spot: float,
        position: Optional[Position],
        min_qty: float,
        pair: Optional[str] = None,
    ) -> Optional[HedgeSignal]:
        pair = pair or CFG["trading"]["perp_symbol"]
        if position and position.size > 0:
            return self.evaluate_exit(spot, position, pair)
        self.clear_trade()
        return self.pick_entry({pair: spot}, {pair: min_qty})


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

    async def _seed_settings(self):
        cfg = CFG["trading"]
        defaults = {
            "mode": self._mode,
            "delta_threshold": str(cfg["delta_threshold"]),
            "max_delta": str(cfg["max_delta"]),
            "max_vega": str(cfg["max_vega"]),
            "max_drawdown_pct": str(cfg["max_drawdown_pct"]),
            "capital_inr": str(cfg.get("capital_inr", 5000)),
            "target_margin_inr": str(cfg.get("target_margin_inr", 1200)),
            "max_margin_inr": str(cfg.get("max_margin_inr", 1500)),
            "profit_trail_giveback_inr": str((cfg.get("strategy") or {}).get("profit_trail_giveback_inr", 50)),
            "profit_trail_arm_inr": str((cfg.get("strategy") or {}).get("profit_trail_arm_inr", 100)),
            "max_loss_inr": str((cfg.get("strategy") or {}).get("max_loss_inr", 200)),
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
            "mode", "capital_inr", "target_margin_inr", "max_margin_inr", "leverage",
            "perp_symbol", "margin_currency", "live_trading", "max_delta",
        )
        existing = await self.store.rd.hgetall("rubaih:settings")
        merged = dict(existing or {})
        merged.update(defaults)
        await self.store.rd.hset("rubaih:settings", mapping={k: merged[k] for k in defaults})
        await self._apply_settings(merged)
        for k in force_keys:
            if k == "leverage":
                self._leverage = int(float(defaults["leverage"]))
                self.cycle.leverage = self._leverage
            elif k == "capital_inr":
                self.cycle.capital_inr = float(defaults["capital_inr"])
            elif k == "target_margin_inr":
                self.cycle.target_margin_inr = float(defaults["target_margin_inr"])
            elif k == "max_margin_inr":
                self.cycle.max_margin_inr = float(defaults["max_margin_inr"])
            elif k == "mode":
                self._mode = str(defaults["mode"]).strip().lower()
            elif k == "max_delta":
                self.risk.max_delta = float(defaults["max_delta"])
        self.cycle.margin_buffer = float(cfg.get("margin_buffer", 0.85))
        self.cycle.usdt_inr = float(cfg.get("usdt_inr", 87))
        scfg = cfg.get("strategy") or {}
        self.cycle.max_loss_inr = float(scfg.get("max_loss_inr", 200))
        self.cycle.trail_giveback_inr = float(scfg.get("profit_trail_giveback_inr", 50))
        self.cycle.trail_arm_inr = float(scfg.get("profit_trail_arm_inr", 100))
        self.cycle.take_profit_pct = float(scfg.get("take_profit_pct", 0.004))
        self.cycle.stop_loss_pct = float(scfg.get("stop_loss_pct", 0.0025))
        print(
            f"[SETTINGS] Forced from config: mode={self._mode} capital=₹{self.cycle.capital_inr} "
            f"target_margin=₹{self.cycle.target_margin_inr} max_margin=₹{self.cycle.max_margin_inr} "
            f"TP={self.cycle.take_profit_pct:.2%} SL={self.cycle.stop_loss_pct:.2%} "
            f"trail=₹{self.cycle.trail_giveback_inr}/arm₹{self.cycle.trail_arm_inr} "
            f"max_loss=₹{self.cycle.max_loss_inr} lev={self._leverage}x"
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
        if "capital_inr" in data:
            self.cycle.capital_inr = float(data["capital_inr"])
        if "target_margin_inr" in data:
            self.cycle.target_margin_inr = float(data["target_margin_inr"])
        if "leverage" in data:
            self._leverage = int(float(data["leverage"]))
            self.cycle.leverage = self._leverage
        print(
            f"[SETTINGS] mode={self._mode} threshold={self.strategist.delta_threshold} "
            f"max_delta={self.risk.max_delta} max_vega={self.risk.max_vega} "
            f"max_dd={self.risk.max_dd} capital_inr={self.cycle.capital_inr} "
            f"target_margin=₹{self.cycle.target_margin_inr} lev={self._leverage}"
        )
    async def command_listener(self):
        """Honor kill-switch / settings from authenticated API. Reconnects if Redis drops."""
        while self._running:
            pubsub = None
            try:
                if self.store.rd is None:
                    await self.store.connect()
                pubsub = self.store.rd.pubsub()
                await pubsub.subscribe("rubaih:command")
                print("[CMD] Listening on rubaih:command")
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
                        data = payload.get("data") or {}
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

        # Fill remaining slots with other active INR pairs (skip obvious locked names)
        extras = []
        for pair in instruments:
            if not isinstance(pair, str) or pair in majors:
                continue
            up = pair.upper()
            if "SLX" in up:
                continue
            extras.append(pair)
        to_load = majors + extras
        to_load = to_load[: max(self._scan_max, len(majors))]

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
        print(f"[RUBAIH] Loaded {len(self.products)} CoinDCX instruments")
        print(f"[RUBAIH] Scan pairs ({len(self._scan_pairs)}): {', '.join(self._scan_pairs[:8])}{'…' if len(self._scan_pairs) > 8 else ''}")
        print(f"[RUBAIH] Mode: {self._mode} | scan={'ON' if self._scan_enabled else 'OFF'}")
        print(f"[RUBAIH] Active pair: {self._active_pair}")
        print(f"[RUBAIH] AI augmentation: {'ENABLED' if self._ai_enabled else 'DISABLED'}")
        print(f"[RUBAIH] LIVE_TRADING: {'ON — real orders' if self._live else 'OFF — dry-run only'}")
        print(f"[RUBAIH] Capital: ₹{CFG['trading'].get('capital_inr', '?')} | target margin/trade: ₹{CFG['trading'].get('target_margin_inr', 2000)} @ {self._leverage}x")
        print(f"[RUBAIH] Margin currency: {MARGIN_CCY}")
        await self.store.rd.hset(
            "rubaih:settings",
            mapping={"scan_pairs": ",".join(self._scan_pairs), "active_pair": self._active_pair},
        )
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
            direction = 1.0 if (self._dry_pos.side or "buy").lower() == "buy" else -1.0
            self._dry_pos.unrealized_pnl = (
                (spot - self._dry_pos.entry_price) * self._dry_pos.size * direction
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

    async def sync_positions(self):
        """Sync positions for scan allowlist only — never ingest SLX / other books."""
        while self._running:
            try:
                allow = set(self._scan_pairs) | {self._active_pair}

                # futures_cycle: local cycle position is source of truth for dashboard
                if self._mode == "futures_cycle" and self._dry_pos and self._dry_pos.size > 0:
                    spot = self._mid_for(self._dry_pos.symbol)
                    if spot > 0 and self._dry_pos.entry_price > 0:
                        direction = 1.0 if self._dry_pos.side == "buy" else -1.0
                        self._dry_pos.unrealized_pnl = (
                            (spot - self._dry_pos.entry_price) * self._dry_pos.size * direction
                        )
                    self.portfolio.update_positions([self._dry_pos])
                    await self._set_active_pair(self._dry_pos.symbol)
                    # Live: optionally overlay exchange marks, but never wipe local while open
                    if self._live and self.client and self.client._auth_ok:
                        try:
                            raw = await self.client.get_positions() or []
                            for rp in raw:
                                pair = rp.get("pair", "")
                                if pair != self._dry_pos.symbol:
                                    continue
                                active = float(rp.get("active_pos", 0) or 0)
                                mark = float(rp.get("mark_price", 0) or 0)
                                avg = float(rp.get("avg_price", 0) or 0)
                                if abs(active) > 0:
                                    self._dry_pos.size = abs(active)
                                    self._dry_pos.side = "buy" if active > 0 else "sell"
                                    if avg > 0:
                                        self._dry_pos.entry_price = avg
                                    if mark > 0 and avg > 0:
                                        self._dry_pos.unrealized_pnl = (mark - avg) * active
                                    self.portfolio.update_positions([self._dry_pos])
                                break
                        except Exception as e:
                            print(f"[SYNC] live overlay: {e}")
                    await asyncio.sleep(10)
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
                        upnl = (mark - avg) * active
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
                if positions:
                    await self._set_active_pair(positions[0].symbol)
                    # Mirror into cycle tracker so exits/dashboard stay consistent
                    if self._mode == "futures_cycle":
                        self._dry_pos = positions[0]
                        if self.cycle._tp_price <= 0 or self.cycle._plan_pair != positions[0].symbol:
                            self.cycle.arm_trade(
                                positions[0].symbol,
                                positions[0].entry_price,
                                positions[0].size,
                            )
                            await self.store.save_trade_plan(self.cycle.trade_plan_dict())
                            await self._log(
                                f"[SYNC] Armed TP/SL for open {positions[0].symbol} "
                                f"entry={positions[0].entry_price}"
                            )
                else:
                    if self._mode == "futures_cycle" and not self._dry_pos:
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

                violation = self.risk.check(greeks, session_pnl)
                if violation:
                    await self.store.save_risk_event("VIOLATION", violation)
                    await self.store.set_engine_status("halted")
                    await self._emergency_unwind()
                    break

                if self._mode == "futures_cycle":
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
                        self.cycle.clear_trade()
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
                        signal = self.cycle.pick_entry(mids, min_qty_by_pair)
                    if signal and signal.hedge_size != 0:
                        if signal.pair:
                            await self._set_active_pair(signal.pair)
                        await self._log(f"[CYCLE] {signal.reason}")
                        # Exits must never be rate-limited or soft-blocked
                        is_exit = signal.hedge_size < 0
                        await self._execute_hedge(signal, force=is_exit)
                        await self._publish_snapshot()
                        if is_exit:
                            await self.store.save_trade_plan({})
                        else:
                            await self.store.save_trade_plan(self.cycle.trade_plan_dict())
                else:
                    signal = self.strategist.evaluate(greeks, spot if spot > 0 else next(iter(self._pair_mids.values()), 0.0))
                    if signal and signal.hedge_size != 0:
                        await self._log(f"[HEDGE] {signal.reason}")
                        await self._execute_hedge(signal)
                        await self._publish_snapshot()
            except Exception as e:
                await self._log(f"[LOOP] Error: {e}")
            await asyncio.sleep(1)

    async def _execute_hedge(self, signal: HedgeSignal, force: bool = False):
        if not force and not self.risk.alive:
            print("[HEDGE] Blocked — kill switch active")
            return
        if not self.risk.rate_limit_ok() and not force:
            print("[HEDGE] Rate limited")
            return
        perp_symbol = signal.pair or self._active_pair or CFG["trading"]["perp_symbol"]
        perp_prod = self.products.get(perp_symbol)
        if not perp_prod:
            print(f"[HEDGE] Perp {perp_symbol} not found")
            return
        if not signal.pair:
            signal.pair = perp_symbol

        side = Side.BUY if signal.hedge_size > 0 else Side.SELL
        size = abs(signal.hedge_size)
        cfg = CFG["trading"]
        spot = self._mid_for(perp_symbol)
        if spot <= 0:
            spot = self.portfolio.spot_prices.get(perp_prod.underlying, 0.0)

        async def _place(qty: float):
            if qty <= 0 or qty < perp_prod.min_quantity:
                await self._log(f"[HEDGE] Size {qty} below min {perp_prod.min_quantity}")
                return False
            live_ok = self._live and self.client and self.client._auth_ok
            if not live_ok:
                if self._live and self.client and not self.client._auth_ok:
                    if not getattr(self, "_live_block_warned", False):
                        self._live_block_warned = True
                        await self._log(
                            "[LIVE BLOCKED] CoinDCX auth failed — simulating fills until AUTH OK. "
                            "Set LIVE_TRADING=false or fix API keys."
                        )
                    await self._log(f"[SIM] Would {side.value} {qty} {perp_symbol} @ ~{spot} (auth blocked)")
                else:
                    await self._log(f"[DRY-RUN] Would {side.value} {qty} {perp_symbol} @ ~{spot}")
                await self.store.save_hedge(signal, spot, size=qty)
                self._apply_dry_fill(perp_symbol, side, qty, spot)
                return True

            result = await self.client.place_order(perp_prod.pair, side, qty, "market", self._leverage)
            await self._log(f"[HEDGE] LIVE order: {result}")
            if isinstance(result, dict) and (
                result.get("status") == "error"
                or result.get("code") in (400, 401, 403, 500, "400", "401", "403", "500")
                or result.get("error")
            ):
                await self._log(f"[HEDGE] Order rejected — not updating position: {result}")
                return False
            await self.store.save_hedge(signal, spot, size=qty)
            self._apply_dry_fill(perp_symbol, side, qty, spot)
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
        """Update local cycle position (dry-run and live) so dashboard reflects fills."""
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
            # Lock TP/SL immediately at fill price
            if self._dry_pos:
                self.cycle.arm_trade(symbol, self._dry_pos.entry_price, self._dry_pos.size)
        else:  # SELL
            if pos and pos.side == "buy":
                remain = pos.size - qty
                if remain <= 1e-12:
                    self._dry_pos = None
                    self.cycle.clear_trade()
                else:
                    pos.size = remain
            else:
                self._dry_pos = None
                self.cycle.clear_trade()
        if self._dry_pos:
            self.portfolio.update_positions([self._dry_pos])
        else:
            self.portfolio.update_positions([])
            self.cycle.clear_trade()

    async def _emergency_unwind(self):
        print("[EMERGENCY] Flattening...")
        try:
            if self._live and self.client:
                await self.client.cancel_all_orders()
            else:
                print("[DRY-RUN] Would cancel all open orders")
        except Exception as e:
            print(f"[EMERGENCY] cancel_all failed: {e}")
        pos, pos_pair = self._find_open_position()
        if pos and pos_pair and pos.size > 0:
            flatten = -pos.size if (pos.side or "buy").lower() == "buy" else pos.size
            signal = HedgeSignal(
                time.time(), 0.0, pos.size, flatten,
                "emergency", "EMERGENCY_UNWIND", False,
                pair=pos_pair,
            )
            await self._execute_hedge(signal, force=True)
        else:
            greeks = self.portfolio.compute_greeks()
            if abs(greeks.delta) > 0.001:
                signal = HedgeSignal(
                    time.time(), 0.0, greeks.delta, -greeks.delta,
                    "emergency", "EMERGENCY_UNWIND", False,
                    pair=self._active_pair,
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
        print(f"  Scan: {'ON' if self._scan_enabled else 'OFF'} ({len(self._scan_pairs)} pairs)")
        print(f"  Active pair: {self._active_pair}")
        print(f"  Capital: ₹{CFG['trading'].get('capital_inr', '?')} @ {self._leverage}x")
        print(f"  AI: {'ENABLED' if self._ai_enabled else 'DISABLED'}")
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
