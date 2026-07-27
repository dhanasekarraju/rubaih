"""
================================================================================
RUBAIH API v2 — Production FastAPI backend
================================================================================
"""

import os
import json
import asyncio
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional, List

from dotenv import load_dotenv

load_dotenv()

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

API_TOKEN = os.getenv("RUBAIH_API_TOKEN", "").strip()
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").strip().lower() in ("1", "true", "yes")

pg_pool: Optional[asyncpg.Pool] = None
rd_client: Optional[redis.Redis] = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pg_pool, rd_client
    if not API_TOKEN or len(API_TOKEN) < 16:
        raise RuntimeError(
            "RUBAIH_API_TOKEN must be set to a strong secret (>=16 chars) before starting the API"
        )
    pg_pool = await asyncpg.create_pool(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "rubaih"),
        user=os.getenv("DB_USER", "rubaih"),
        password=os.getenv("DB_PASSWORD", ""),
        min_size=1,
        max_size=10,
    )
    rd_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
    )
    await rd_client.ping()
    yield
    if pg_pool:
        await pg_pool.close()
    if rd_client:
        await rd_client.close()


app = FastAPI(title="Rubaih API v2", version="2.0.0", lifespan=lifespan)

# Credentials not used with wildcard; keep origins explicit for mobile HTTP clients
_cors = os.getenv("CORS_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _cors if o.strip()],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "OPTIONS"],
    allow_headers=["*"],
)


def _extract_token(
    authorization: Optional[str] = None,
    x_api_token: Optional[str] = None,
    token: Optional[str] = None,
) -> str:
    if x_api_token:
        return x_api_token.strip()
    if authorization and authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    if token:
        return token.strip()
    return ""


async def require_token(
    authorization: Optional[str] = Header(default=None),
    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token"),
):
    provided = _extract_token(authorization, x_api_token)
    if not provided or not secrets.compare_digest(provided, API_TOKEN):
        raise HTTPException(status_code=401, detail="Unauthorized")


class DashboardData(BaseModel):
    timestamp: str
    spot_price: float
    delta: float
    gamma: float
    vega: float
    theta: float
    session_pnl: float
    num_positions: int
    status: str
    ai_enabled: bool
    ai_last_action: Optional[str]
    ai_confidence: Optional[float]
    live_trading: bool
    exchange: str = "coindcx"


class HedgeTrade(BaseModel):
    id: int
    timestamp: str
    symbol: str
    side: str
    size: float
    price: float
    reason: str
    ai_augmented: bool


class AIDecisionData(BaseModel):
    id: int
    timestamp: str
    model: str
    action: str
    confidence: float
    reasoning: str
    risk_assessment: str
    portfolio_delta: float


class SettingsUpdate(BaseModel):
    delta_threshold: Optional[float] = Field(default=None, gt=0)
    max_delta: Optional[float] = Field(default=None, gt=0)
    max_vega: Optional[float] = Field(default=None, gt=0)
    max_drawdown_pct: Optional[float] = Field(default=None, gt=0, le=1)


@app.get("/api/health")
async def health():
    db_ok = False
    rd_ok = False
    try:
        if pg_pool:
            await pg_pool.fetchval("SELECT 1")
            db_ok = True
    except Exception:
        pass
    try:
        if rd_client:
            await rd_client.ping()
            rd_ok = True
    except Exception:
        pass
    status = "ok" if db_ok and rd_ok else "degraded"
    return {
        "status": status,
        "service": "rubaih-api-v2",
        "timestamp": _utc_now(),
        "db": db_ok,
        "redis": rd_ok,
        "live_trading": LIVE_TRADING,
        "exchange": "coindcx",
    }


@app.get("/api/dashboard", response_model=DashboardData, dependencies=[Depends(require_token)])
async def dashboard():
    row = await pg_pool.fetchrow(
        "SELECT * FROM greeks_snapshots ORDER BY timestamp DESC LIMIT 1"
    )
    ai_row = await pg_pool.fetchrow(
        "SELECT action, confidence FROM ai_decisions ORDER BY timestamp DESC LIMIT 1"
    )
    count = await pg_pool.fetchval(
        "SELECT COUNT(*) FROM hedge_trades WHERE timestamp > NOW() - INTERVAL '24 hours'"
    )
    session_pnl = float(await rd_client.get("rubaih:session_pnl") or 0)
    engine_status = await rd_client.get("rubaih:engine_status") or ("no_data" if not row else "running")

    if not row:
        return DashboardData(
            timestamp=_utc_now(),
            spot_price=0.0, delta=0.0, gamma=0.0, vega=0.0, theta=0.0,
            session_pnl=session_pnl, num_positions=0, status=engine_status,
            ai_enabled=bool(os.getenv("OPENROUTER_API_KEY", "").strip()),
            ai_last_action=None, ai_confidence=None,
            live_trading=LIVE_TRADING,
        )

    return DashboardData(
        timestamp=row["timestamp"].isoformat(),
        spot_price=float(row["spot_price"]),
        delta=float(row["delta"]),
        gamma=float(row["gamma"]),
        vega=float(row["vega"]),
        theta=float(row["theta"]),
        session_pnl=session_pnl,
        num_positions=int(count or 0),
        status=engine_status,
        ai_enabled=bool(os.getenv("OPENROUTER_API_KEY", "").strip()),
        ai_last_action=ai_row["action"] if ai_row else None,
        ai_confidence=float(ai_row["confidence"]) if ai_row else None,
        live_trading=LIVE_TRADING,
    )


@app.get("/api/hedge-history", response_model=List[HedgeTrade], dependencies=[Depends(require_token)])
async def hedge_history(limit: int = Query(default=20, ge=1, le=100)):
    rows = await pg_pool.fetch(
        "SELECT * FROM hedge_trades ORDER BY timestamp DESC LIMIT $1", limit
    )
    out = []
    for r in rows:
        keys = set(r.keys())
        out.append(HedgeTrade(
            id=r["id"], timestamp=r["timestamp"].isoformat(),
            symbol=r["symbol"], side=r["side"],
            size=float(r["size"]), price=float(r["price"]),
            reason=r["reason"],
            ai_augmented=bool(r["ai_augmented"]) if "ai_augmented" in keys else False,
        ))
    return out


@app.get("/api/ai-decisions", response_model=List[AIDecisionData], dependencies=[Depends(require_token)])
async def ai_decisions(limit: int = Query(default=20, ge=1, le=100)):
    rows = await pg_pool.fetch(
        "SELECT * FROM ai_decisions ORDER BY timestamp DESC LIMIT $1", limit
    )
    return [
        AIDecisionData(
            id=r["id"], timestamp=r["timestamp"].isoformat(),
            model=r["model"], action=r["action"],
            confidence=float(r["confidence"]), reasoning=r["reasoning"],
            risk_assessment=r["risk_assessment"],
            portfolio_delta=float(r["portfolio_delta"]),
        ) for r in rows
    ]


@app.get("/api/risk-events", dependencies=[Depends(require_token)])
async def risk_events(limit: int = Query(default=20, ge=1, le=100)):
    rows = await pg_pool.fetch(
        "SELECT * FROM risk_events ORDER BY timestamp DESC LIMIT $1", limit
    )
    return [{"id": r["id"], "timestamp": r["timestamp"].isoformat(),
             "type": r["event_type"], "details": r["details"]} for r in rows]


@app.post("/api/kill-switch", dependencies=[Depends(require_token)])
async def kill_switch():
    await rd_client.publish(
        "rubaih:command",
        json.dumps({"command": "KILL_SWITCH", "source": "mobile_app", "ts": _utc_now()}),
    )
    await pg_pool.execute(
        "INSERT INTO risk_events (event_type, details) VALUES ($1, $2)",
        "KILL_SWITCH", "Triggered manually from authenticated mobile/API client",
    )
    await rd_client.set("rubaih:engine_status", "kill_switch")
    return {
        "status": "kill_switch_triggered",
        "message": "Emergency halt signal sent to Rubaih engine",
    }


@app.get("/api/settings", dependencies=[Depends(require_token)])
async def get_settings():
    settings = await rd_client.hgetall("rubaih:settings")
    if not settings:
        settings = {
            "delta_threshold": "0.0005",
            "max_delta": "0.002",
            "max_vega": "100.0",
            "max_drawdown_pct": "0.15",
            "capital_inr": "1000",
            "leverage": "5",
            "live_trading": str(LIVE_TRADING).lower(),
            "exchange": "coindcx",
            "margin_currency": "INR",
            "perp_symbol": "B-BTC_USDT",
        }
    # Keep non-float metadata as strings in response
    out = {}
    for k, v in settings.items():
        try:
            out[k] = float(v)
        except (TypeError, ValueError):
            out[k] = v
    out["live_trading"] = LIVE_TRADING
    return out


@app.put("/api/settings", dependencies=[Depends(require_token)])
async def update_settings(s: SettingsUpdate):
    updates = {k: str(v) for k, v in s.model_dump(exclude_unset=True).items()}
    if updates:
        await rd_client.hset("rubaih:settings", mapping=updates)
        await rd_client.publish(
            "rubaih:command",
            json.dumps({"command": "UPDATE_SETTINGS", "data": updates, "ts": _utc_now()}),
        )
    return {"status": "updated", "settings": await get_settings()}


class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def send(self, ws: WebSocket, message: dict):
        try:
            await ws.send_json(message)
            return True
        except Exception:
            self.disconnect(ws)
            return False


manager = ConnectionManager()


@app.websocket("/ws")
async def websocket_endpoint(
    ws: WebSocket,
    token: Optional[str] = Query(default=None),
):
    # Prefer query token for RN WebSocket (headers are awkward)
    provided = _extract_token(token=token)
    try:
        auth_header = ws.headers.get("authorization")
        api_header = ws.headers.get("x-api-token")
        if auth_header or api_header:
            provided = _extract_token(authorization=auth_header, x_api_token=api_header) or provided
    except Exception:
        pass

    if not provided or not secrets.compare_digest(provided, API_TOKEN):
        await ws.close(code=4401)
        return

    await manager.connect(ws)
    pubsub = rd_client.pubsub()
    await pubsub.subscribe("rubaih:greeks", "rubaih:risk", "rubaih:ai", "rubaih:status")

    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message.get("data"):
                try:
                    data = json.loads(message["data"])
                except Exception:
                    data = {"raw": message["data"]}
                ok = await manager.send(ws, {"channel": message["channel"], "data": data})
                if not ok:
                    break
            try:
                msg = await asyncio.wait_for(ws.receive_text(), timeout=0.5)
                if msg == "ping":
                    await ws.send_text("pong")
            except asyncio.TimeoutError:
                pass
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(ws)
        try:
            await pubsub.unsubscribe()
            await pubsub.close()
        except Exception:
            pass
