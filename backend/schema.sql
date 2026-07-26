-- Rubaih Database Schema
-- Auto-executed on PostgreSQL container startup

CREATE TABLE IF NOT EXISTS greeks_snapshots (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    delta NUMERIC,
    gamma NUMERIC,
    vega NUMERIC,
    theta NUMERIC,
    spot_price NUMERIC
);

CREATE TABLE IF NOT EXISTS hedge_trades (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    symbol TEXT,
    side TEXT,
    size NUMERIC,
    price NUMERIC,
    reason TEXT,
    ai_augmented BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS risk_events (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    event_type TEXT,
    details TEXT
);

CREATE TABLE IF NOT EXISTS ai_decisions (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    model TEXT,
    action TEXT,
    confidence NUMERIC,
    reasoning TEXT,
    risk_assessment TEXT,
    portfolio_delta NUMERIC
);

CREATE INDEX idx_greeks_timestamp ON greeks_snapshots(timestamp DESC);
CREATE INDEX idx_hedges_timestamp ON hedge_trades(timestamp DESC);
CREATE INDEX idx_ai_timestamp ON ai_decisions(timestamp DESC);
