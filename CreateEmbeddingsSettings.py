from dataclasses import dataclass
from typing import Optional, Dict, Any, List


@dataclass
class CreateEmbeddingsSettings:
    sqlite_path: str
    log_path: str
    batch_size: int
    parquet_path: str
    use_synonyms: bool
    use_mapped_terms: bool

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        if config is None:
            return
        system = config["system"]
        for key, value in system.items():
            setattr(self, key, value)
        system = config["text"]
        for key, value in system.items():
            setattr(self, key, value)
