"""
================================================================================
RUBAIH AI — OpenRouter Integration for Trading Decisions
================================================================================
Uses OpenRouter's free-tier models to augment quantitative hedging decisions.

Recommended free models (zero cost):
  - nvidia/nemotron-3-ultra-550b-a55b:free  → Deep reasoning, strategy review
  - nvidia/nemotron-3-super-120b-a12b:free  → Multi-step analysis
  - meta-llama/llama-4-maverick:free        → General trading logic
  - qwen/qwen3-coder:free                   → Structured JSON output

Rate limits: 20 req/min, 1000 req/day (after $10 deposit on OpenRouter)
================================================================================
"""

import os
import json
import time
from typing import Dict, Optional, List
from dataclasses import dataclass

import aiohttp

OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# Fallback model chain — tries each in order if one fails
MODEL_CHAIN = [
    "nvidia/nemotron-3-ultra-550b-a55b:free",
    "nvidia/nemotron-3-super-120b-a12b:free",
    "meta-llama/llama-4-maverick:free",
    "qwen/qwen3-coder:free",
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

    Called every 30-60 seconds (not every tick — rate limits).
    Provides qualitative overlay on top of quantitative Greeks engine.
    """

    def __init__(self):
        self.api_key = OPENROUTER_KEY
        self.session: Optional[aiohttp.ClientSession] = None
        self._call_history: List[Dict] = []
        self._last_call: float = 0.0
        self._min_interval = 30.0  # seconds between AI calls

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def _call_model(self, model: str, messages: List[Dict], temperature: float = 0.2) -> Optional[Dict]:
        """Call a single OpenRouter model."""
        if not self.api_key:
            return None

        session = await self._get_session()
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://rubaih-bot.local",
            "X-Title": "Rubaih Delta Hedge Bot"
        }

        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": 800,
            "response_format": {"type": "json_object"}
        }

        try:
            async with session.post(OPENROUTER_URL, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data
                elif resp.status == 429:
                    print(f"[AI] Rate limited on {model}")
                    return None
                else:
                    text = await resp.text()
                    print(f"[AI] Error {resp.status} from {model}: {text[:200]}")
                    return None
        except Exception as e:
            print(f"[AI] Exception calling {model}: {e}")
            return None

    async def analyze_market(self, context: Dict) -> Optional[AIDecision]:
        """
        Main entry point. Sends market context to AI, gets structured decision.

        context = {
            "portfolio_greeks": {"delta": 0.12, "gamma": 0.0003, "vega": 1200, "theta": -45},
            "spot_price": 95200.0,
            "recent_hedges": [...],
            "iv_change_1h": 0.02,
            "funding_rate": 0.0001,
            "time_since_last_hedge": 45.0,
            "quant_signal": "HOLD"  # What the quantitative engine says
        }
        """
        now = time.time()
        if now - self._last_call < self._min_interval:
            return None
        self._last_call = now

        system_prompt = """You are Rubaih, an elite crypto options delta-hedge strategist. 
You analyze portfolio Greeks and market microstructure to make hedging decisions.

Respond ONLY in valid JSON with this exact structure:
{
  "action": "HEDGE" | "HOLD" | "ADJUST_THRESHOLD" | "EMERGENCY",
  "confidence": 0.0 to 1.0,
  "reasoning": "concise explanation",
  "suggested_hedge_size": float or null,
  "risk_assessment": "LOW | MEDIUM | HIGH | CRITICAL"
}

Rules:
- HEDGE only when delta drift is meaningful AND market conditions support it
- HOLD when transaction costs would exceed gamma P&L benefit
- ADJUST_THRESHOLD when vol regime is changing
- EMERGENCY only for extreme tail risks (IV spike >50%, liquidation cascade)
- Confidence >0.8 required for HEDGE, >0.95 for EMERGENCY
- Consider funding rate, IV term structure, and recent hedge frequency"""

        user_prompt = f"""Current market state:
- Portfolio delta: {context['portfolio_greeks']['delta']:.4f} BTC
- Portfolio gamma: {context['portfolio_greeks']['gamma']:.6f}
- Portfolio vega: {context['portfolio_greeks']['vega']:.2f} USD/vol
- Portfolio theta: {context['portfolio_greeks']['theta']:.2f} USD/day
- Spot price: ${context['spot_price']:,.2f}
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

        # Try models in chain until one succeeds
        for model in MODEL_CHAIN:
            result = await self._call_model(model, messages)
            if result and "choices" in result:
                try:
                    content = result["choices"][0]["message"]["content"]
                    parsed = json.loads(content)
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
                    print(f"[AI] {model} → {decision.action} (conf={decision.confidence:.2f})")
                    return decision
                except Exception as e:
                    print(f"[AI] Failed to parse response from {model}: {e}")
                    continue

        print("[AI] All models failed. Falling back to quantitative engine.")
        return None

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
