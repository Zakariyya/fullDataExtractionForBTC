from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DownloadTask:
    task_id: str
    status: str
    created_at: float
    updated_at: float
    request: dict[str, Any]
    summaries: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None
    events_history: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "request": self.request,
            "summaries": self.summaries,
            "error": self.error,
        }
