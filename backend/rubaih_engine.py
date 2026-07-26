"""
================================================================================
RUBAIH v2 — Delta Exchange Delta-Hedge Bot with OpenRouter AI
================================================================================
⚠️  EDUCATIONAL / RESEARCH PURPOSES ONLY.
    Test on Delta Exchange Testnet for minimum 30 days before live capital.
================================================================================
"""

import asyncio
import hashlib
import hmac
import json
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional
from collections import defaultdict

import aiohttp
import asyncpg
import numpy as np
import redis.asyncio as redis
import yaml
from scipy.stats import norm

from openrouter_ai import OpenRouterAI, AIDecision

# ==============================================================================
# CONFIG
# ==============================================================================
with open("config.yaml", "r") as f:
    CFG = yaml.safe_load(f)

USE_TESTNET = CFG["exchange"]["use_testnet"]
REST_URL = CFG["exchange"]["testnet_rest_url" if USE_TESTNET else "rest_url"]
WS_URL = CFG["exchange"]["testnet_ws_url" if USE_TESTNET else "ws_url"]
API_KEY = os.getenv("DELTA_API_KEY", "")
API_SECRET = os.getenv("DELTA_API_SECRET", "")

# ==============================================================================
# MODELS
# ==============================================================================
class OptionType(Enum):
    CALL = "call_options"
    PUT = "put_options"

class Side(Enum):
    BUY = "buy"
    SELL = "sell"

@dataclass
class DeltaProduct:
    product_id: int
    symbol: str
    underlying: str
    strike: float
    expiry_ts: float
    opt_type: Optional[OptionType]
    is_perp: bool = False
    contract_value: float = 0.001

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
    product_id: int
    side: str
    size: float
    entry_price: float
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0

# ==============================================================================
# DELTA EXCHANGE CLIENT
# ==============================================================================
class DeltaAuth:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret.encode()

    def sign(self, method: str, path: str, payload: str = "") -> Dict[str, str]:
        timestamp = str(int(time.time()))
        signature_data = method + timestamp + path + payload
        signature = hmac.new(self.api_secret, signature_data.encode(), hashlib.sha256).hexdigest()
        return {
            "api-key": self.api_key,
            "signature": signature,
            "timestamp": timestamp,
            "Content-Type": "application/json"
        }

class DeltaClient:
    def __init__(self, auth: DeltaAuth):
        self.auth = auth
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def _request(self, method: str, path: str, payload: Optional[Dict] = None) -> Dict:
        url = f"{REST_URL}{path}"
        body = json.dumps(payload) if payload else ""
        headers = self.auth.sign(method, path, body)
        async with self.session.request(method, url, headers=headers, data=body) as resp:
            data = await resp.json()
            if not data.get("success", False):
                print(f"[API ERROR] {path}: {data}")
            return data

    async def get_products(self) -> List[Dict]:
        data = await self._request("GET", "/products")
        return data.get("result", [])

    async def get_positions(self) -> List[Dict]:
        data = await self._request("GET", "/positions")
        return data.get("result", [])

    async def place_order(self, product_id: int, side: Side, size: float, order_type: str = "market") -> Dict:
        payload = {
            "product_id": product_id,
            "side": side.value,
            "size": int(size),
            "order_type": order_type,
            "time_in_force": "ioc" if order_type == "market" else "gtc"
        }
        return await self._request("POST", "/orders", payload)

    async def cancel_all_orders(self) -> Dict:
        return await self._request("DELETE", "/orders/all")

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
        self.products: Dict[str, DeltaProduct] = {}

    def update_spot(self, underlying: str, price: float):
        self.spot_prices[underlying] = price

    def update_product(self, product: DeltaProduct):
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
            if spot <= 0:
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

        if gamma_save < est_cost * self.cost_mult:
            return HedgeSignal(now, 0.0, delta, 0.0, "none",
                f"cost_reject: save=${gamma_save:.2f} < cost=${est_cost:.2f}", False)

        urgency = "immediate" if abs(delta) > self.delta_threshold * 3 else "passive"
        self._last_hedge = now
        self._last_delta = 0.0
        return HedgeSignal(now, 0.0, delta, hedge_size, urgency,
            f"delta={delta:.4f}, cost=${est_cost:.2f}, save=${gamma_save:.2f}", False)

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
        self.pg = await asyncpg.create_pool(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME", "rubaih"),
            user=os.getenv("DB_USER", "rubaih"),
            password=os.getenv("DB_PASSWORD", "rubaih_secret_2026")
        )
        self.rd = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            decode_responses=True
        )
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

    async def save_greeks(self, g: GreeksSnapshot, spot: float):
        await self.pg.execute(
            "INSERT INTO greeks_snapshots (delta, gamma, vega, theta, spot_price) VALUES ($1,$2,$3,$4,$5)",
            g.delta, g.gamma, g.vega, g.theta, spot
        )
        await self.rd.publish("rubaih:greeks", json.dumps({
            "timestamp": g.timestamp, "delta": g.delta, "gamma": g.gamma,
            "vega": g.vega, "theta": g.theta, "spot": spot
        }))

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
        self.auth = DeltaAuth(API_KEY, API_SECRET)
        self.pricing = PricingEngine()
        self.portfolio = PortfolioRiskEngine(self.pricing)
        self.strategist = HedgingStrategist()
        self.risk = RiskManager()
        self.store = DataStore()
        self.ai = OpenRouterAI()
        self.client: Optional[DeltaClient] = None
        self.products: Dict[str, DeltaProduct] = {}
        self._running = False
        self._ai_enabled = os.getenv("OPENROUTER_API_KEY", "") != ""
        self._hedge_history: List[Dict] = []

    async def bootstrap(self):
        print("[RUBAIH] Bootstrapping...")
        await self.store.connect()
        self.client = DeltaClient(self.auth)
        await self.client.__aenter__()

        prods = await self.client.get_products()
        for p in prods:
            symbol = p.get("symbol", "")
            ctype = p.get("contract_type", "")
            opt_type = None
            if "call_options" in ctype:
                opt_type = OptionType.CALL
            elif "put_options" in ctype:
                opt_type = OptionType.PUT

            expiry = 0.0
            if p.get("contract_expiry"):
                try:
                    dt = datetime.strptime(p["contract_expiry"], "%Y-%m-%dT%H:%M:%SZ")
                    expiry = dt.timestamp()
                except:
                    pass

            prod = DeltaProduct(
                product_id=p.get("id", 0), symbol=symbol,
                underlying=p.get("underlying_asset", {}).get("symbol", "BTC"),
                strike=float(p.get("strike_price", 0) or 0),
                expiry_ts=expiry, opt_type=opt_type,
                is_perp="perpetual" in ctype,
                contract_value=float(p.get("contract_value", 0.001) or 0.001)
            )
            self.products[symbol] = prod
            self.portfolio.update_product(prod)

        self.pricing.update_surface("BTC", 0.55, -0.15, 0.08)
        self.pricing.update_surface("ETH", 0.60, -0.18, 0.10)
        print(f"[RUBAIH] Loaded {len(self.products)} products")
        print(f"[RUBAIH] AI augmentation: {'ENABLED' if self._ai_enabled else 'DISABLED'}")

    async def ws_listener(self):
        import websockets
        symbols = [CFG["trading"]["perp_symbol"]]
        for sym, prod in self.products.items():
            if prod.underlying == CFG["trading"]["underlying"] and prod.opt_type:
                symbols.append(sym)
        symbols = symbols[:12]

        while self._running:
            try:
                async with websockets.connect(WS_URL) as ws:
                    payload = {
                        "type": "subscribe",
                        "payload": {
                            "channels": [
                                {"name": "ob_l1", "symbols": symbols},
                                {"name": "ticker", "symbols": symbols}
                            ]
                        }
                    }
                    await ws.send(json.dumps(payload))
                    print(f"[WS] Subscribed to {len(symbols)} symbols")
                    async for msg in ws:
                        if not self._running:
                            break
                        await self._on_ws_message(json.loads(msg))
            except Exception as e:
                print(f"[WS] Error: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)

    async def _on_ws_message(self, msg: Dict):
        msg_type = msg.get("type", "")
        if msg_type == "ob_l1":
            symbol = msg.get("sy", "")
            bid = float(msg.get("bp", 0))
            ask = float(msg.get("ap", 0))
            if bid > 0 and ask > 0:
                prod = self.products.get(symbol)
                if prod and prod.is_perp:
                    self.portfolio.update_spot(prod.underlying, (bid + ask) / 2)
        elif msg_type == "ticker":
            result = msg.get("result", msg)
            symbol = result.get("symbol", "")
            spot = float(result.get("spot_price", 0))
            iv = float(result.get("mark_vol", 0)) / 100 if result.get("mark_vol") else None
            if iv:
                self.pricing.update_iv(symbol, iv)
            if spot > 0:
                prod = self.products.get(symbol)
                if prod and prod.is_perp:
                    self.portfolio.update_spot(prod.underlying, spot)

    async def sync_positions(self):
        while self._running:
            try:
                raw = await self.client.get_positions()
                positions = []
                for rp in raw:
                    positions.append(Position(
                        symbol=rp.get("product_symbol", ""),
                        product_id=rp.get("product_id", 0),
                        side=rp.get("side", "buy"),
                        size=float(rp.get("size", 0)),
                        entry_price=float(rp.get("entry_price", 0)),
                        unrealized_pnl=float(rp.get("unrealized_pnl", 0)),
                        realized_pnl=float(rp.get("realized_pnl", 0))
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
                    "iv_change_1h": 0.0,  # Would compute from history
                    "funding_rate": 0.0001,
                    "time_since_last_hedge": time.time() - self.strategist._last_hedge,
                    "quant_signal": "HOLD" if abs(greeks.delta) < self.strategist.delta_threshold else "HEDGE"
                }

                decision = await self.ai.analyze_market(context)
                if decision:
                    await self.store.save_ai_decision(decision, greeks.delta)
                    print(f"[AI] {decision.model_used}: {decision.action} ({decision.confidence:.0%} confidence)")

                    # AI can override or confirm quantitative signal
                    if decision.action == "EMERGENCY" and decision.confidence > 0.95:
                        self.risk.trigger_kill(f"AI_EMERGENCY: {decision.reasoning}")
            except Exception as e:
                print(f"[AI LOOP] Error: {e}")
            await asyncio.sleep(60)

    async def main_loop(self):
        while self._running and self.risk.alive:
            try:
                greeks = self.portfolio.compute_greeks()
                spot = self.portfolio.spot_prices.get(CFG["trading"]["underlying"], 0.0)
                if spot <= 0:
                    await asyncio.sleep(1)
                    continue

                await self.store.save_greeks(greeks, spot)

                session_pnl = sum(p.unrealized_pnl for p in self.portfolio.positions.values())
                violation = self.risk.check(greeks, session_pnl)
                if violation:
                    await self.store.save_risk_event("VIOLATION", violation)
                    await self._emergency_unwind()
                    break

                signal = self.strategist.evaluate(greeks, spot)
                if signal and signal.hedge_size != 0:
                    print(f"[HEDGE] {signal.reason}")
                    await self._execute_hedge(signal)
            except Exception as e:
                print(f"[LOOP] Error: {e}")
            await asyncio.sleep(1)

    async def _execute_hedge(self, signal: HedgeSignal):
        if not self.risk.rate_limit_ok():
            print("[HEDGE] Rate limited")
            return
        perp_symbol = CFG["trading"]["perp_symbol"]
        perp_prod = None
        for p in self.products.values():
            if p.symbol == perp_symbol and p.is_perp:
                perp_prod = p
                break
        if not perp_prod:
            print(f"[HEDGE] Perp {perp_symbol} not found")
            return

        side = Side.BUY if signal.hedge_size > 0 else Side.SELL
        size = abs(signal.hedge_size)
        cfg = CFG["trading"]

        if size > cfg["max_order_size_btc"]:
            slices = cfg["twap_slices"]
            slice_size = size / slices
            print(f"[HEDGE] TWAP: {slices} slices of {slice_size:.4f} BTC")
            for i in range(slices):
                await self.client.place_order(perp_prod.product_id, side, slice_size, "market")
                await self.store.save_hedge(signal, 0.0)
                if i < slices - 1:
                    await asyncio.sleep(cfg["twap_interval_sec"])
        else:
            result = await self.client.place_order(perp_prod.product_id, side, size, "market")
            print(f"[HEDGE] Order: {result}")
            await self.store.save_hedge(signal, 0.0)

    async def _emergency_unwind(self):
        print("[EMERGENCY] Flattening...")
        await self.client.cancel_all_orders()
        greeks = self.portfolio.compute_greeks()
        if abs(greeks.delta) > 0.001:
            signal = HedgeSignal(time.time(), 0.0, greeks.delta, -greeks.delta, "emergency", "EMERGENCY_UNWIND", False)
            await self._execute_hedge(signal)
        self._running = False

    async def run(self):
        self._running = True
        await self.bootstrap()
        print("\n" + "="*60)
        print("  🤖 RUBAIH v2 IS LIVE")
        print(f"  Exchange: Delta Exchange {'TESTNET' if USE_TESTNET else 'LIVE'}")
        print(f"  Underlying: {CFG['trading']['underlying']}")
        print(f"  AI: {'ENABLED' if self._ai_enabled else 'DISABLED'}")
        print("="*60 + "\n")

        tasks = [
            self.ws_listener(),
            self.sync_positions(),
            self.main_loop()
        ]
        if self._ai_enabled:
            tasks.append(self.ai_analysis_loop())

        await asyncio.gather(*tasks)

    async def shutdown(self):
        self._running = False
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
        asyncio.run(bot.shutdown())
