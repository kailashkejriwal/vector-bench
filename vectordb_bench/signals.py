"""Signals for async benchmark runner (avoids circular imports)."""

from enum import Enum


class SIGNAL(Enum):
    SUCCESS = 0
    ERROR = 1
    WIP = 2
