"""
================================================================================
RUBAIH API v2 — FastAPI backend with AI decision streaming
================================================================================
"""

import os
import json
import asyncio
from datetime import datetime
from typing import Optional, List

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Rubaih API v2", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pg_pool: Optional[asyncpg.Pool] = None
rd_client: Optional[redis.Redis] = None

@app.on_event("startup")
async def startup():
    global pg_pool, rd_client
    pg_pool = await asyncpg.create_pool(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "rubaih"),
        user=os.getenv("DB_USER", "rubaih"),
        password=os.getenv("DB_PASSWORD", "rubaih_secret_2026")
    )
    rd_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True
    )

@app.on_event("shutdown")
async def shutdown():
    if pg_pool:
        await pg_pool.close()
    if rd_client:
        await rd_client.close()

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
    delta_threshold: Optional[float] = None
    max_delta: Optional[float] = None
    max_vega: Optional[float] = None
    max_drawdown_pct: Optional[float] = None

@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "rubaih-api-v2", "timestamp": datetime.utcnow().isoformat()}

@app.get("/api/dashboard", response_model=DashboardData)
async def dashboard():
    row = await pg_pool.fetchrow(
        "SELECT * FROM greeks_snapshots ORDER BY timestamp DESC LIMIT 1"
    )
    ai_row = await pg_pool.fetchrow(
        "SELECT action, confidence FROM ai_decisions ORDER BY timestamp DESC LIMIT 1"
    )
    count = await pg_pool.fetchval("SELECT COUNT(*) FROM hedge_trades WHERE timestamp > NOW() - INTERVAL '24 hours'")

    if not row:
        return DashboardData(
            timestamp=datetime.utcnow().isoformat(),
            spot_price=0.0, delta=0.0, gamma=0.0, vega=0.0, theta=0.0,
            session_pnl=0.0, num_positions=0, status="no_data",
            ai_enabled=os.getenv("OPENROUTER_API_KEY", "") != "",
            ai_last_action=None, ai_confidence=None
        )

    return DashboardData(
        timestamp=row["timestamp"].isoformat(),
        spot_price=float(row["spot_price"]),
        delta=float(row["delta"]),
        gamma=float(row["gamma"]),
        vega=float(row["vega"]),
        theta=float(row["theta"]),
        session_pnl=0.0,
        num_positions=count,
        status="running",
        ai_enabled=os.getenv("OPENROUTER_API_KEY", "") != "",
        ai_last_action=ai_row["action"] if ai_row else None,
        ai_confidence=float(ai_row["confidence"]) if ai_row else None
    )

@app.get("/api/hedge-history", response_model=List[HedgeTrade])
async def hedge_history(limit: int = 20):
    rows = await pg_pool.fetch(
        "SELECT * FROM hedge_trades ORDER BY timestamp DESC LIMIT $1", limit
    )
    return [
        HedgeTrade(
            id=r["id"], timestamp=r["timestamp"].isoformat(),
            symbol=r["symbol"], side=r["side"],
            size=float(r["size"]), price=float(r["price"]),
            reason=r["reason"], ai_augmented=r.get("ai_augmented", False)
        ) for r in rows
    ]

@app.get("/api/ai-decisions", response_model=List[AIDecisionData])
async def ai_decisions(limit: int = 20):
    rows = await pg_pool.fetch(
        "SELECT * FROM ai_decisions ORDER BY timestamp DESC LIMIT $1", limit
    )
    return [
        AIDecisionData(
            id=r["id"], timestamp=r["timestamp"].isoformat(),
            model=r["model"], action=r["action"],
            confidence=float(r["confidence"]), reasoning=r["reasoning"],
            risk_assessment=r["risk_assessment"],
            portfolio_delta=float(r["portfolio_delta"])
        ) for r in rows
    ]

@app.get("/api/risk-events")
async def risk_events(limit: int = 20):
    rows = await pg_pool.fetch(
        "SELECT * FROM risk_events ORDER BY timestamp DESC LIMIT $1", limit
    )
    return [{"id": r["id"], "timestamp": r["timestamp"].isoformat(),
             "type": r["event_type"], "details": r["details"]} for r in rows]

@app.post("/api/kill-switch")
async def kill_switch():
    await rd_client.publish("rubaih:command", json.dumps({"command": "KILL_SWITCH", "source": "mobile_app"}))
    await pg_pool.execute(
        "INSERT INTO risk_events (event_type, details) VALUES ($1, $2)",
        "KILL_SWITCH", "Triggered manually from mobile app"
    )
    return {"status": "kill_switch_triggered", "message": "Emergency halt signal sent to Rubaih engine"}

@app.get("/api/settings")
async def get_settings():
    settings = await rd_client.hgetall("rubaih:settings")
    if not settings:
        settings = {
            "delta_threshold": "0.05",
            "max_delta": "0.50",
            "max_vega": "5000.0",
            "max_drawdown_pct": "0.05"
        }
    return {k: float(v) for k, v in settings.items()}

@app.put("/api/settings")
async def update_settings(s: SettingsUpdate):
    updates = {k: str(v) for k, v in s.dict(exclude_unset=True).items()}
    if updates:
        await rd_client.hset("rubaih:settings", mapping=updates)
        await rd_client.publish("rubaih:command", json.dumps({"command": "UPDATE_SETTINGS", "data": updates}))
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

    async def broadcast(self, message: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

manager = ConnectionManager()

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    pubsub = rd_client.pubsub()
    await pubsub.subscribe("rubaih:greeks", "rubaih:risk", "rubaih:ai")

    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message:
                data = json.loads(message["data"])
                await manager.broadcast({"channel": message["channel"], "data": data})
            try:
                msg = await asyncio.wait_for(ws.receive_text(), timeout=0.5)
                if msg == "ping":
                    await ws.send_text("pong")
            except asyncio.TimeoutError:
                pass
    except WebSocketDisconnect:
        manager.disconnect(ws)
        await pubsub.unsubscribe()
