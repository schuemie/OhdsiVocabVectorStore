from dataclasses import dataclass
from typing import Optional, Dict, Any, List


@dataclass
class UploadEmbeddingVectorsSettings:
    log_path: str
    parquet_folder: str
    schema: str
    table: str
    store_type: str

    PGVECTOR = "pgvector"
    PGVECTOR_HALFVEC = "pgvector_halfvec"

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        if config is None:
            return
        system = config["system"]
        for key, value in system.items():
            setattr(self, key, value)
        vector_store = config["vector_store"]
        for key, value in vector_store.items():
            setattr(self, key, value)

    def __post_init__(self):
        if self.store_type not in [self.PGVECTOR, self.PGVECTOR_HALFVEC]:
            raise ValueError(f"store_type must be '{self.PGVECTOR}' or '{self.PGVECTOR_HALFVEC}'")