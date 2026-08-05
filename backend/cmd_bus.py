"""
Signed Redis control-plane messages for rubaih:command.

API and engine share RUBAIH_API_TOKEN. Unsigned kill/settings publishes are rejected.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
from typing import Any, Dict, Optional


def _canonical(payload: Dict[str, Any]) -> str:
    body = {k: payload[k] for k in sorted(payload.keys()) if k != "sig"}
    return json.dumps(body, separators=(",", ":"), sort_keys=True, default=str)


def sign_command(
    secret: str,
    command: str,
    data: Optional[Dict[str, Any]] = None,
    source: str = "api",
) -> Dict[str, Any]:
    if not secret:
        raise ValueError("command bus secret required")
    payload: Dict[str, Any] = {
        "command": command,
        "source": source,
        "ts": time.time(),
        "nonce": secrets.token_hex(8),
    }
    if data is not None:
        payload["data"] = data
    payload["sig"] = hmac.new(
        secret.encode("utf-8"),
        _canonical(payload).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return payload


def verify_command(
    secret: str,
    payload: Dict[str, Any],
    max_age_sec: float = 120.0,
) -> bool:
    if not secret or not isinstance(payload, dict):
        return False
    sig = str(payload.get("sig") or "")
    if not sig or len(sig) < 32:
        return False
    try:
        ts = float(payload.get("ts") or 0)
    except (TypeError, ValueError):
        return False
    if abs(time.time() - ts) > max_age_sec:
        return False
    expect = hmac.new(
        secret.encode("utf-8"),
        _canonical(payload).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(sig, expect)


# Settings keys allowed via signed UPDATE_SETTINGS (no free-capital poison).
ALLOWED_SETTINGS_KEYS = frozenset(
    {
        "max_drawdown_pct",
        "max_delta",
        "max_vega",
        "delta_threshold",
        "margin_use_frac",
        "margin_use_max_frac",
        "take_profit_price_pct",
        "take_profit_pct",
        "stop_loss_price_pct",
        "stop_loss_pct",
        "entry_move_pct",
        "entry_cooldown_sec",
        "max_hold_sec",
        "max_loss_frac",
        "trail_arm_r",
        "trail_giveback_r",
        "leverage",
        "scan_enabled",
        "mode",
    }
)


def filter_settings(data: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in (data or {}).items() if k in ALLOWED_SETTINGS_KEYS}
