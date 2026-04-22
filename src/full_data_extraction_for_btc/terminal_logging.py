from __future__ import annotations

import logging
import sys
import threading
import time
from typing import TextIO


class InlineConsole:
    def __init__(self, stream: TextIO | None = None, force_line_interval_seconds: float = 1.0) -> None:
        self._stream = stream or sys.stderr
        self._enabled = bool(getattr(self._stream, "isatty", lambda: False)())
        self._lock = threading.Lock()
        self._last_inline_len = 0
        self._force_line_interval_seconds = max(0.0, force_line_interval_seconds)
        self._last_force_line_at = time.monotonic()

    def line(self, message: str) -> None:
        with self._lock:
            self._clear_inline_locked()
            self._stream.write(f"{message}\n")
            self._stream.flush()

    def inline(self, message: str) -> None:
        with self._lock:
            if not self._enabled:
                self._stream.write(f"{message}\n")
                self._stream.flush()
                return

            clear_padding = max(0, self._last_inline_len - len(message))
            self._stream.write(f"\r{message}{' ' * clear_padding}")
            self._stream.flush()
            self._last_inline_len = len(message)
            now = time.monotonic()
            if now - self._last_force_line_at >= self._force_line_interval_seconds:
                # Emit a real newline periodically to avoid terminals that only repaint
                # carriage-return updates after user input.
                self._stream.write("\n")
                self._stream.flush()
                self._last_inline_len = 0
                self._last_force_line_at = now

    def clear_inline(self) -> None:
        with self._lock:
            self._clear_inline_locked()

    def _clear_inline_locked(self) -> None:
        if self._last_inline_len <= 0:
            return
        if self._enabled:
            self._stream.write(f"\r{' ' * self._last_inline_len}\r")
            self._stream.flush()
        self._last_inline_len = 0


class InlineAwareStreamHandler(logging.Handler):
    def __init__(self, console: InlineConsole) -> None:
        super().__init__()
        self._console = console

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            self._console.line(message)
        except Exception:  # noqa: BLE001
            self.handleError(record)


def configure_terminal_logging(level: int = logging.INFO) -> InlineConsole:
    console = InlineConsole()
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    handler = InlineAwareStreamHandler(console)
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(handler)
    return console
