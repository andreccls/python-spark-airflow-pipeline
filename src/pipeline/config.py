"""Filesystem layout of the local data lake. The only place that knows paths."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_DATA_DIR = "data"


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    seed: int = 42
    orders_per_day: int = 300
    max_reject_ratio: float = 0.10

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            data_dir=Path(os.environ.get("PIPELINE_DATA_DIR", DEFAULT_DATA_DIR)),
            seed=int(os.environ.get("PIPELINE_SEED", "42")),
            orders_per_day=int(os.environ.get("PIPELINE_ORDERS_PER_DAY", "300")),
            max_reject_ratio=float(os.environ.get("PIPELINE_MAX_REJECT_RATIO", "0.10")),
        )

    def layer(self, layer: str, table: str) -> Path:
        return self.data_dir / layer / table
