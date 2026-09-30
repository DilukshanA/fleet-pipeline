"""One JSON log line format shared by every stage of the pipeline.

Every component calls get_logger(stage) once and then logger.info(msg, **fields).
Output is a single JSON object per line so it can be grepped, tailed, or
shipped to a log collector without a custom parser.
"""
import json
import logging
import sys
import time


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "stage": getattr(record, "stage", record.name),
            "message": record.getMessage(),
        }
        # any extra fields passed via logger.info(msg, extra={...}) or the
        # StageLogger wrapper below get merged in flat.
        for key, value in getattr(record, "fields", {}).items():
            payload[key] = value
        return json.dumps(payload, default=str)


class StageLogger:
    """Thin wrapper so call sites can do log.info("msg", rows_in=10, rows_out=9)."""

    def __init__(self, stage: str):
        self._logger = logging.getLogger(stage)
        self._stage = stage

    def _log(self, level, message, **fields):
        self._logger.log(
            level, message, extra={"stage": self._stage, "fields": fields}
        )

    def info(self, message, **fields):
        self._log(logging.INFO, message, **fields)

    def warning(self, message, **fields):
        self._log(logging.WARNING, message, **fields)

    def error(self, message, **fields):
        self._log(logging.ERROR, message, **fields)

    def debug(self, message, **fields):
        self._log(logging.DEBUG, message, **fields)


def get_logger(stage: str) -> StageLogger:
    root = logging.getLogger(stage)
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        root.propagate = False
    return StageLogger(stage)


def now_ms() -> int:
    return int(time.time() * 1000)
