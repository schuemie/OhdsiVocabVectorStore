from dataclasses import dataclass
from typing import Optional, Dict, Any, List


@dataclass
class VocabDownloadSettings:
    sqlite_path: str
    log_path: str
    batch_size: int
    domain_ids: List[str]

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        if config is None:
            return
        system = config["system"]
        for key, value in system.items():
            setattr(self, key, value)
        system = config["content"]
        for key, value in system.items():
            setattr(self, key, value)
