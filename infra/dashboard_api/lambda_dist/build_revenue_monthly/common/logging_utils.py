import json
import logging
from datetime import datetime
from typing import Any, Dict


_LOGGER = logging.getLogger("analytics")
_LOGGER.setLevel(logging.INFO)


def _emit(level: str, message: str, **fields: Any) -> None:
    payload: Dict[str, Any] = {
        "level": level,
        "message": message,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        **fields,
    }
    _LOGGER.log(getattr(logging, level, logging.INFO), json.dumps(payload, default=str))


def log_info(message: str, **fields: Any) -> None:
    _emit("INFO", message, **fields)


def log_warning(message: str, **fields: Any) -> None:
    _emit("WARNING", message, **fields)


def log_error(message: str, **fields: Any) -> None:
    _emit("ERROR", message, **fields)
