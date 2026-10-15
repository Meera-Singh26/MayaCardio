from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Config:
    data_dir: str = "./data"
    mock_host: str = "127.0.0.1"
    mock_port: int = 7100
    cloud_url: str = "http://127.0.0.1:8080"
    api_key: str = ""
    sync: str = "FULL"                 # SQLite synchronous level: FULL = fsync on every commit
    poll_interval: float = 0.3         # seconds between reads when the mock has no backlog
    read_timeout: float = 3.0          # mock socket timeout
    probe_interval: float = 1.0        # heartbeat period
    probe_timeout: float = 2.0
    request_timeout: float = 4.0       # live uploads
    backfill_timeout: float = 8.0
    live_batch: int = 6000
    backfill_batch: int = 10000
    stuck_s: float = 12.0              # online window not confirmed by cloud this long -> process on device
    disk_full_retry_s: float = 5.0
    reserve_mb: int = 16

    @property
    def db_path(self) -> str:
        return os.path.join(self.data_dir, "device.db")

    @property
    def state_path(self) -> str:
        return os.path.join(self.data_dir, "state.json")
