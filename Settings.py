from dataclasses import dataclass
from typing import Optional, Dict, Any, List


@dataclass
class Settings:
    log_folder: str
    terms_db_path: str
    download_batch_size: int
    embeddings_folder: str
    embedding_batch_size: int

    domain_ids: List[str]
    include_classification_concepts: bool
    classification_vocabularies: List[str]
    include_synonyms: bool
    include_mapped_terms: bool
    max_text_characters: int
    restrict_to_used_concepts: bool

    schema: str
    vector_table: str
    record_count_table: str
    store_type: str

    PGVECTOR = "pgvector"
    PGVECTOR_HALFVEC = "pgvector_halfvec"

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        if config is None:
            return
        system = config["system"]
        for key, value in system.items():
            setattr(self, key, value)
        vector_store = config["terms"]
        for key, value in vector_store.items():
            setattr(self, key, value)
        vector_store = config["database_details"]
        for key, value in vector_store.items():
            setattr(self, key, value)

    def __post_init__(self):
        if self.store_type not in [self.PGVECTOR, self.PGVECTOR_HALFVEC]:
            raise ValueError(f"store_type must be '{self.PGVECTOR}' or '{self.PGVECTOR_HALFVEC}'")