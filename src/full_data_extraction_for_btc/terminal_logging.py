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
    def __init__(self, console: InlineConsole, inline_logger_prefixes: tuple[str, ...] = ()) -> None:
        super().__init__()
        self._console = console
        self._inline_logger_prefixes = inline_logger_prefixes

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            if self._should_inline(record):
                self._console.inline(message)
            else:
                self._console.line(message)
        except Exception:  # noqa: BLE001
            self.handleError(record)

    def _should_inline(self, record: logging.LogRecord) -> bool:
        if record.levelno > logging.INFO:
            return False
        if not self._inline_logger_prefixes:
            return False
        return any(record.name.startswith(prefix) for prefix in self._inline_logger_prefixes)


def configure_terminal_logging(
    level: int = logging.INFO,
    force_line_interval_seconds: float = 1.0,
    inline_logger_prefixes: tuple[str, ...] = (),
) -> InlineConsole:
    console = InlineConsole(force_line_interval_seconds=force_line_interval_seconds)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    handler = InlineAwareStreamHandler(console, inline_logger_prefixes=inline_logger_prefixes)
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(handler)
    return console
