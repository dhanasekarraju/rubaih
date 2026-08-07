"""
Single source of truth for Rubaih runtime config.

Both the engine and API load `config.yaml` from here. Risk limits must never
be re-defined as hardcoded API fallbacks.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import yaml

_CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"
_CFG: Dict[str, Any] | None = None


def config_path() -> Path:
    return _CONFIG_PATH


def load_config(force: bool = False) -> Dict[str, Any]:
    global _CFG
    if _CFG is not None and not force:
        return _CFG
    path = config_path()
    if not path.is_file():
        raise RuntimeError(f"config.yaml not found at {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise RuntimeError("config.yaml must parse to a mapping")
    assert_risk_limits(data)
    _CFG = data
    return _CFG


def assert_risk_limits(cfg: Dict[str, Any] | None = None) -> None:
    """Fail loudly if authoritative risk limits are missing or non-positive."""
    data = cfg if cfg is not None else load_config()
    trading = data.get("trading") or {}
    required = (
        "max_delta",
        "max_vega",
        "max_drawdown_pct",
        "max_notional_inr",
        "max_day_loss_inr",
    )
    missing = [k for k in required if k not in trading or trading[k] is None]
    if missing:
        raise RuntimeError(
            f"config.yaml trading section missing required risk limits: {', '.join(missing)}"
        )
    for key in required:
        try:
            val = float(trading[key])
        except (TypeError, ValueError) as e:
            raise RuntimeError(f"config.yaml trading.{key} must be a number") from e
        if val <= 0:
            raise RuntimeError(
                f"config.yaml trading.{key} must be > 0 (got {val})"
            )


def settings_defaults_from_config(
    cfg: Dict[str, Any] | None = None,
    *,
    live_trading: bool | None = None,
) -> Dict[str, str]:
    """Redis/API settings strings derived only from config.yaml (+ live flag)."""
    data = cfg if cfg is not None else load_config()
    trading = data["trading"]
    scfg = trading.get("strategy") or {}
    live = (
        live_trading
        if live_trading is not None
        else os.getenv("LIVE_TRADING", "false").strip().lower() in ("1", "true", "yes")
    )
    scan_pairs = trading.get("scan_pairs") or [trading.get("perp_symbol", "B-ETH_USDT")]
    if isinstance(scan_pairs, list):
        scan_csv = ",".join(str(p) for p in scan_pairs)
    else:
        scan_csv = str(scan_pairs)
    tp = float(scfg.get("take_profit_price_pct", scfg.get("take_profit_pct", 0.014)))
    sl = float(scfg.get("stop_loss_price_pct", scfg.get("stop_loss_pct", 0.007)))
    lev = float(trading.get("leverage", 10) or 10)
    return {
        "mode": str(trading.get("mode", "futures_cycle")),
        "delta_threshold": str(trading["delta_threshold"]),
        "max_delta": str(trading["max_delta"]),
        "max_vega": str(trading["max_vega"]),
        "max_drawdown_pct": str(trading["max_drawdown_pct"]),
        "max_notional_inr": str(trading["max_notional_inr"]),
        "max_day_loss_inr": str(trading["max_day_loss_inr"]),
        "capital_inr": str(trading.get("capital_inr", 1000)),
        "margin_use_frac": str(trading.get("margin_use_frac", 0.25)),
        "margin_use_max_frac": str(trading.get("margin_use_max_frac", 0.30)),
        "take_profit_price_pct": str(tp),
        "stop_loss_price_pct": str(sl),
        "take_profit_pct": str(scfg.get("take_profit_pct", tp)),
        "stop_loss_pct": str(scfg.get("stop_loss_pct", sl)),
        "take_profit_roe": str(tp * lev),
        "stop_loss_roe": str(sl * lev),
        "tp_display": f"Price +{tp * 100:.2f}% (ROE≈+{tp * lev * 100:.0f}% @{lev:.0f}x)",
        "sl_display": f"Price −{sl * 100:.2f}% (ROE≈−{sl * lev * 100:.0f}% @{lev:.0f}x)",
        "leverage": str(int(lev)),
        "live_trading": str(live).lower(),
        "exchange": str((data.get("exchange") or {}).get("name", "coindcx")),
        "margin_currency": str((data.get("exchange") or {}).get("margin_currency", "INR")),
        "perp_symbol": str(trading.get("perp_symbol", "B-ETH_USDT")),
        "active_pair": str(trading.get("perp_symbol", "B-ETH_USDT")),
        "scan_enabled": str(bool(trading.get("scan_enabled", True))).lower(),
        "scan_pairs": scan_csv,
        "max_loss_frac": str(scfg.get("max_loss_frac", 0.08)),
        "trail_arm_r": str(scfg.get("trail_arm_r", 0.35)),
        "trail_giveback_r": str(scfg.get("trail_giveback_r", 0.30)),
    }
