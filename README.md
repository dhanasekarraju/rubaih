# 🤖 Rubaih — CoinDCX Futures Delta-Hedge Bot

> **Educational / research software. Live trading can lose money.**
> Default mode is **dry-run**. Real orders require `LIVE_TRADING=true`.

## Production checklist

1. Copy `.env.example` → `.env`
2. Set `COINDCX_API_KEY` / `COINDCX_API_SECRET` (futures trade permission, IP whitelist if possible)
3. Set strong `DB_PASSWORD` and `RUBAIH_API_TOKEN` (`openssl rand -hex 32`)
4. Deploy with `sudo bash setup-vps.sh`
5. Confirm dry-run logs look correct (`docker compose logs -f rubaih_engine`)
6. Only then set `LIVE_TRADING=true` and `docker compose up -d --force-recreate rubaih_engine rubaih_api`
7. Build APK after `mobile/config.js` has your VPS IP + same API token

## Architecture

```
Mobile (token) → Nginx :8080 → FastAPI → Redis/Postgres
                              ↑
                         Rubaih engine → CoinDCX futures
```

## Safety controls

| Control | Behavior |
|---------|----------|
| `LIVE_TRADING` | Must be `true` for real CoinDCX orders; otherwise dry-run logs only |
| `RUBAIH_API_TOKEN` | Required on all API routes except `/api/health`; WS needs `?token=` |
| Kill switch | Authenticated POST → Redis command → engine halt + flatten |
| Risk limits | max drawdown / notional / day-loss → emergency unwind |
| API bind | FastAPI on `127.0.0.1:8010` only; public via nginx `:8080` |

## Quick deploy

```bash
cp .env.example .env
# edit secrets…
sudo bash setup-vps.sh
```

Public endpoints after deploy:

- `http://YOUR_IP:8080/api/health` (open)
- `http://YOUR_IP:8080/api/dashboard` (requires `X-API-Token`)
- `ws://YOUR_IP:8080/ws?token=YOUR_TOKEN`

## Mobile / APK

Set GitHub Actions secrets for a ready-to-install APK:

- `MOBILE_API_HOST` — e.g. `http://YOUR_VPS_IP`
- `MOBILE_API_TOKEN` — same value as `RUBAIH_API_TOKEN`

Or run the workflow manually with those inputs.

```bash
cd mobile
# config.js must have API_HOST + API_TOKEN (setup-vps patches these on the VPS)
npm install
# GitHub Actions builds APK on push to main, or:
npx expo prebuild --platform android
cd android && ./gradlew assembleRelease
```

## CoinDCX

- REST: `https://api.coindcx.com`
- Public: `https://public.coindcx.com`
- Stream: `https://stream.coindcx.com`
- Default hedge pair: `B-BTC_USDT`

## License

MIT — not financial advice.
