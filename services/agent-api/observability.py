from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger("dp.observability")


def emit(event: str, **fields: Any) -> None:
    """Emit one structured operational event.

    Values are JSON-safe; anything else is stringified. The event must never
    carry passwords, tokens, full restricted beneficiary records or secret
    configuration.
    """
    logger.info(json.dumps({"event": event, **fields}, default=str, separators=(",", ":")))


def brief(value: str, limit: int = 200) -> str:
    """Truncate free text so operational logs stay bounded."""
    value = (value or "").strip().replace("\\n", " ")
    return value if len(value) <= limit else value[: limit - 3] + "..."