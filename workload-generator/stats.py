"""In-process counters for operations, the change events they produce, and failures."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Counter:
    operations: int = 0
    events: int = 0
    errors: int = 0
