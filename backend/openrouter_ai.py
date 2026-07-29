"""
================================================================================
RUBAIH AI — NVIDIA NIM primary, OpenRouter fallback
================================================================================
1) NVIDIA NIM (NVIDIA_API_KEY) — preferred
2) OpenRouter free models (OPENROUTER_API_KEY) — fallback
Quant / futures_cycle engine keeps running if both fail.
================================================================================
"""

import os
import json
import time
import re
from typing import Dict, Optional, List
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

import aiohttp

NVIDIA_KEY = os.getenv("NVIDIA_API_KEY", "").strip()
NVIDIA_MODEL = (
    os.getenv("NVIDIA_MODEL", "nvidia/nemotron-3-nano-30b-a3b").strip()
    or "nvidia/nemotron-3-nano-30b-a3b"
)
NVIDIA_URL = "https://integrate.api.nvidia.com/v1/chat/completions"
NVIDIA_TIMEOUT_SEC = float(os.getenv("NVIDIA_TIMEOUT_SEC", "60"))

OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

OPENROUTER_MODELS = [
    "openrouter/free",
    "openai/gpt-oss-20b:free",
    "nvidia/nemotron-3-nano-30b-a3b:free",
    "google/gemma-3-27b-it:free",
]


@dataclass
class AIDecision:
    action: str
    confidence: float
    reasoning: str
    suggested_hedge_size: Optional[float]
    risk_assessment: str
    model_used: str


def ai_configured() -> bool:
    return bool(NVIDIA_KEY or OPENROUTER_KEY)


def _extract_json(text: str) -> dict:
    text = (text or "").strip()
    if "```" in text:
        parts = text.split("```")
        for part in parts[1::2]:
            chunk = part.strip()
            if chunk.startswith("json"):
                chunk = chunk[4:].strip()
            try:
                return json.loads(chunk)
            except json.JSONDecodeError:
                continue
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}", text)
        if m:
            return json.loads(m.group(0))
        raise


class OpenRouterAI:
    """NVIDIA NIM first, then OpenRouter. Name retained for engine compatibility."""

    def __init__(self):
        self.nvidia_key = NVIDIA_KEY
        self.openrouter_key = OPENROUTER_KEY
        self.session: Optional[aiohttp.ClientSession] = None
        self._call_history: List[Dict] = []
        self._last_call: float = 0.0
        self._min_interval = 180.0  # 3 min between AI attempts
        self._fail_streak = 0
        self._backoff_until = 0.0
        self._dead_models: Dict[str, float] = {}
        self._nvidia_warned = False
        if self.nvidia_key:
            print(f"[AI] Provider order: NVIDIA/{NVIDIA_MODEL} → OpenRouter fallback")
        elif self.openrouter_key:
            print("[AI] NVIDIA key missing — OpenRouter only")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    def _prompts(self, context: Dict):
        system_prompt = """You are Rubaih, a crypto INR-M futures cycle assistant on CoinDCX.
You review portfolio state and may suggest HOLD / HEDGE / EMERGENCY.

Respond ONLY in valid JSON:
{
  "action": "HEDGE" | "HOLD" | "ADJUST_THRESHOLD" | "EMERGENCY",
  "confidence": 0.0 to 1.0,
  "reasoning": "concise explanation",
  "suggested_hedge_size": float or null,
  "risk_assessment": "LOW | MEDIUM | HIGH | CRITICAL"
}

Rules:
- Prefer HOLD when flat or already managed by the quantitative cycle
- EMERGENCY only for extreme risk
- Confidence >0.8 for HEDGE, >0.95 for EMERGENCY"""

        user_prompt = f"""Current market state:
- Portfolio delta: {context['portfolio_greeks']['delta']:.4f} BTC
- Portfolio gamma: {context['portfolio_greeks']['gamma']:.6f}
- Portfolio vega: {context['portfolio_greeks']['vega']:.2f}
- Portfolio theta: {context['portfolio_greeks']['theta']:.2f}
- Spot price: {context['spot_price']:,.2f} USDT
- IV change (1h): {context.get('iv_change_1h', 0):+.2%}
- Funding rate: {context.get('funding_rate', 0):+.4%}
- Time since last hedge: {context.get('time_since_last_hedge', 0):.0f}s
- Quantitative engine signal: {context.get('quant_signal', 'UNKNOWN')}
- Recent hedges (last 5): {json.dumps(context.get('recent_hedges', [])[-5:])}

What is your decision?"""
        return system_prompt, user_prompt

    async def _call_nvidia(self, messages: List[Dict]) -> Optional[str]:
        if not self.nvidia_key:
            return None
        dead_until = self._dead_models.get("nvidia", 0)
        if time.time() < dead_until:
            return None

        session = await self._get_session()
        headers = {
            "Authorization": f"Bearer {self.nvidia_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": NVIDIA_MODEL,
            "messages": messages,
            "temperature": 0.2,
            "max_tokens": 800,
            "stream": False,
        }
        try:
            async with session.post(
                NVIDIA_URL,
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=NVIDIA_TIMEOUT_SEC),
            ) as resp:
                text = await resp.text()
                if resp.status == 200:
                    data = json.loads(text)
                    return data["choices"][0]["message"]["content"]
                if resp.status == 429:
                    self._dead_models["nvidia"] = time.time() + 1800
                    if not self._nvidia_warned:
                        self._nvidia_warned = True
                        print("[AI] NVIDIA rate limited — skip 30m, using OpenRouter")
                    return None
                if resp.status in (400, 403, 404):
                    self._dead_models["nvidia"] = time.time() + 1800
                    print(f"[AI] NVIDIA error {resp.status}: {text[:120]} — OpenRouter fallback")
                    return None
                print(f"[AI] NVIDIA error {resp.status}: {text[:160]}")
                return None
        except Exception as e:
            detail = str(e) or "(no message)"
            print(f"[AI] NVIDIA {type(e).__name__}: {detail} — OpenRouter fallback")
            return None

    async def _call_openrouter(self, model: str, messages: List[Dict]) -> Optional[str]:
        if not self.openrouter_key:
            return None
        dead_until = self._dead_models.get(model, 0)
        if time.time() < dead_until:
            return None

        session = await self._get_session()
        headers = {
            "Authorization": f"Bearer {self.openrouter_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://rubaih-bot.local",
            "X-Title": "Rubaih CoinDCX Futures Bot",
        }
        payload = {
            "model": model,
            "messages": messages,
            "temperature": 0.2,
            "max_tokens": 800,
        }
        if model != "openrouter/free":
            payload["response_format"] = {"type": "json_object"}

        try:
            async with session.post(
                OPENROUTER_URL, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                text = await resp.text()
                if resp.status == 200:
                    data = json.loads(text)
                    return data["choices"][0]["message"]["content"]
                if resp.status == 429:
                    self._dead_models[model] = time.time() + 300
                    if self._fail_streak < 2:
                        print(f"[AI] OpenRouter rate limited on {model}")
                    return None
                if resp.status == 404:
                    self._dead_models[model] = time.time() + 86400
                    if self._fail_streak < 2:
                        print(f"[AI] OpenRouter model gone ({model})")
                    return None
                if self._fail_streak < 3:
                    print(f"[AI] OpenRouter {resp.status} from {model}: {text[:140]}")
                return None
        except Exception as e:
            if self._fail_streak < 3:
                print(f"[AI] OpenRouter exception ({model}): {e}")
            return None

    def _parse_decision(self, content: str, model_used: str) -> Optional[AIDecision]:
        try:
            parsed = _extract_json(content)
            decision = AIDecision(
                action=parsed.get("action", "HOLD"),
                confidence=float(parsed.get("confidence", 0.0)),
                reasoning=parsed.get("reasoning", ""),
                suggested_hedge_size=parsed.get("suggested_hedge_size"),
                risk_assessment=parsed.get("risk_assessment", "UNKNOWN"),
                model_used=model_used,
            )
            self._call_history.append({
                "timestamp": time.time(),
                "model": model_used,
                "decision": parsed,
            })
            self._fail_streak = 0
            print(f"[AI] {model_used} → {decision.action} (conf={decision.confidence:.2f})")
            return decision
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            if self._fail_streak < 3:
                print(f"[AI] Parse error from {model_used}: {e}")
            return None

    async def analyze_market(self, context: Dict) -> Optional[AIDecision]:
        if not ai_configured():
            return None
        now = time.time()
        if now < self._backoff_until:
            return None
        if now - self._last_call < self._min_interval:
            return None
        self._last_call = now

        system_prompt, user_prompt = self._prompts(context)
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        # 1) NVIDIA NIM primary
        nvidia_text = await self._call_nvidia(messages)
        if nvidia_text:
            decision = self._parse_decision(nvidia_text, f"nvidia/{NVIDIA_MODEL}")
            if decision:
                return decision

        # 2) OpenRouter fallback
        for model in OPENROUTER_MODELS:
            content = await self._call_openrouter(model, messages)
            if content:
                decision = self._parse_decision(content, model)
                if decision:
                    return decision

        self._fail_streak += 1
        wait = min(1800, 120 * (2 ** min(self._fail_streak - 1, 4)))
        self._backoff_until = time.time() + wait
        if self._fail_streak <= 3 or self._fail_streak % 10 == 0:
            print(f"[AI] NVIDIA+OpenRouter failed (x{self._fail_streak}). Quant continues. Retry in {wait:.0f}s")
        return None

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
