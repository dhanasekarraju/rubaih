"""
================================================================================
RUBAIH AI — OpenRouter Integration for Trading Decisions
================================================================================
Uses OpenRouter free-tier models to optionally augment quantitative decisions.

Free models churn often — prefer openrouter/free router, then stable :free slugs.
On repeated failure we back off (bot keeps running on futures_cycle / quant).
================================================================================
"""

import os
import json
import time
from typing import Dict, Optional, List
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

import aiohttp

OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# Prefer auto free router; then currently common free slugs (catalog changes often)
MODEL_CHAIN = [
    "openrouter/free",
    "meta-llama/llama-3.3-70b-instruct:free",
    "openai/gpt-oss-20b:free",
    "google/gemma-4-31b-it:free",
    "nvidia/nemotron-3-nano-30b-a3b:free",
]

@dataclass
class AIDecision:
    action: str           # "HEDGE" | "HOLD" | "ADJUST_THRESHOLD" | "EMERGENCY"
    confidence: float       # 0.0 - 1.0
    reasoning: str
    suggested_hedge_size: Optional[float]
    risk_assessment: str
    model_used: str

class OpenRouterAI:
    """
    AI-augmented trading decision engine.

    Called periodically (not every tick). Quantitative / futures_cycle engine
    remains the authority when AI is unavailable.
    """

    def __init__(self):
        self.api_key = OPENROUTER_KEY
        self.session: Optional[aiohttp.ClientSession] = None
        self._call_history: List[Dict] = []
        self._last_call: float = 0.0
        self._min_interval = 60.0  # seconds between AI attempts
        self._fail_streak = 0
        self._backoff_until = 0.0
        self._dead_models: Dict[str, float] = {}  # model -> retry_after ts

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def _call_model(self, model: str, messages: List[Dict], temperature: float = 0.2) -> Optional[Dict]:
        """Call a single OpenRouter model."""
        if not self.api_key:
            return None
        dead_until = self._dead_models.get(model, 0)
        if time.time() < dead_until:
            return None

        session = await self._get_session()
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://rubaih-bot.local",
            "X-Title": "Rubaih CoinDCX Futures Bot"
        }

        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": 800,
        }
        # json_object not supported by every free model
        if model != "openrouter/free":
            payload["response_format"] = {"type": "json_object"}

        try:
            async with session.post(
                OPENROUTER_URL, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                text = await resp.text()
                if resp.status == 429:
                    self._dead_models[model] = time.time() + 300
                    if self._fail_streak < 2:
                        print(f"[AI] Rate limited on {model} — cooling 5m")
                    return None
                if resp.status == 404:
                    self._dead_models[model] = time.time() + 86400
                    if self._fail_streak < 2:
                        print(f"[AI] Model gone ({model}) — skip 24h")
                    return None
                if self._fail_streak < 3:
                    print(f"[AI] Error {resp.status} from {model}: {text[:160]}")
                return None
        except Exception as e:
            if self._fail_streak < 3:
                print(f"[AI] Exception calling {model}: {e}")
            return None

    async def analyze_market(self, context: Dict) -> Optional[AIDecision]:
        now = time.time()
        if now < self._backoff_until:
            return None
        if now - self._last_call < self._min_interval:
            return None
        self._last_call = now

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

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        for model in MODEL_CHAIN:
            result = await self._call_model(model, messages)
            if result and "choices" in result:
                try:
                    content = result["choices"][0]["message"]["content"]
                    # strip markdown fences if free router wraps JSON
                    if "```" in content:
                        content = content.split("```")[1]
                        if content.startswith("json"):
                            content = content[4:]
                    parsed = json.loads(content.strip())
                    decision = AIDecision(
                        action=parsed.get("action", "HOLD"),
                        confidence=float(parsed.get("confidence", 0.0)),
                        reasoning=parsed.get("reasoning", ""),
                        suggested_hedge_size=parsed.get("suggested_hedge_size"),
                        risk_assessment=parsed.get("risk_assessment", "UNKNOWN"),
                        model_used=model
                    )
                    self._call_history.append({
                        "timestamp": time.time(),
                        "model": model,
                        "decision": parsed
                    })
                    self._fail_streak = 0
                    print(f"[AI] {model} → {decision.action} (conf={decision.confidence:.2f})")
                    return decision
                except (json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError) as e:
                    if self._fail_streak < 3:
                        print(f"[AI] Parse error from {model}: {e}")
                    continue

        self._fail_streak += 1
        # Exponential backoff: 2m, 5m, 15m, cap 30m
        wait = min(1800, 120 * (2 ** min(self._fail_streak - 1, 4)))
        self._backoff_until = time.time() + wait
        if self._fail_streak <= 3 or self._fail_streak % 10 == 0:
            print(f"[AI] All models failed (x{self._fail_streak}). Quant engine continues. Retry in {wait:.0f}s")
        return None
