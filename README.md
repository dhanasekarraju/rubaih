# 🤖 Rubaih — Delta-Hedge Bot for Delta Exchange India

> **⚠️ EDUCATIONAL / RESEARCH PURPOSES ONLY.**
> Test on Delta Exchange Testnet for minimum 30 days before live capital.

## What is Rubaih?

Rubaih is a professional-grade crypto options delta-hedge bot built for **Delta Exchange India**. It combines:

- **Quantitative engine**: Smile-adjusted Black-Scholes Greeks, cost-aware rebalancing
- **AI augmentation**: OpenRouter free-tier models (Nemotron, Llama, Qwen) for strategy overlay
- **Mobile control panel**: React Native app with real-time WebSocket streaming
- **Docker deployment**: One-command VPS setup with PostgreSQL + Redis

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Mobile App │────▶│  FastAPI    │────▶│   Engine    │
│  (React Nat)│◀────│  (VPS:8000) │◀────│  (Python)   │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                                │
                    ┌─────────────┐     ┌──────▼──────┐
                    │  PostgreSQL │◀────▶│    Redis    │
                    │  (Ledger)   │     │  (Pub/Sub)  │
                    └─────────────┘     └─────────────┘
                                                │
                                          ┌─────▼─────┐
                                          │  OpenRouter│
                                          │  (Free AI) │
                                          └───────────┘
```

## Quick Start

### 1. Clone & Configure

```bash
git clone https://github.com/YOUR_USERNAME/rubaih.git
cd rubaih
cp .env.example .env
# Edit .env with your Delta Exchange TESTNET keys and OpenRouter API key
```

### 2. Deploy on VPS

```bash
docker-compose up -d --build
```

### 3. Mobile App

```bash
cd mobile
npm install
# Update API_URL in App.js with your VPS IP
npx expo start
# Or build APK:
eas build --platform android --profile preview
```

## OpenRouter AI Integration

Rubaih uses OpenRouter's **free-tier models** for AI-augmented trading decisions:

| Model | Role | Cost |
|-------|------|------|
| `nvidia/nemotron-3-ultra:free` | Deep strategy analysis | $0 |
| `nvidia/nemotron-3-super:free` | Multi-step reasoning | $0 |
| `meta-llama/llama-4-maverick:free` | General trading logic | $0 |
| `qwen/qwen3-coder:free` | Structured JSON output | $0 |

**Rate limits**: 20 req/min, 1000 req/day (after $10 deposit on OpenRouter)

The AI provides a **qualitative overlay** on top of the quantitative Greeks engine — it does not replace math, it augments it.

## File Structure

```
rubaih/
├── docker-compose.yml
├── .env.example
├── nginx.conf
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── config.yaml
│   ├── schema.sql
│   ├── rubaih_engine.py    # Main trading bot
│   ├── openrouter_ai.py    # AI decision engine
│   └── api.py              # FastAPI backend
├── mobile/
│   ├── App.js              # React Native app
│   └── package.json
└── .github/workflows/
    └── build-apk.yml       # CI/CD for APK
```

## Delta Exchange India

- **REST API**: `https://api.india.delta.exchange/v2`
- **WebSocket**: `wss://socket.india.delta.exchange`
- **Testnet**: Available for paper trading
- **Fees**: 0.03% options + 18% GST (~0.0354% per side)

## License

MIT — Educational use only. Not financial advice.
