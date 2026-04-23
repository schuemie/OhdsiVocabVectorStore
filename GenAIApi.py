import json
import os

import numpy as np
from openai import OpenAI
from typing import List, Optional, Dict, Any, Tuple
from dotenv import load_dotenv

load_dotenv()

_PRICING_TABLE = {
    # OpenAI models
    "text-embedding-3-small": {"input": 0.02, "output": 0.00},
    "text-embedding-3-large": {"input": 0.13, "output": 0.00},
    # Local models (Free)
    "local": {"input": 0.00, "output": 0.00},
}



class _AIClientFactory:

    @staticmethod
    def get_client(task_type: str = "llm") -> Tuple[Any, str, str]:
        """
        Args:
        task_type: 'llm' or 'embedding'

        Returns:
            client: Configured AI client
            model_name: Name of model
            provider_type: 'openai', 'azure', or 'local'
        """
        provider = os.getenv("GENAI_PROVIDER").lower()
        api_key: Optional[str] = None


        model_name = os.getenv("EMBEDDING_MODEL")
        if provider != "lm-studio":
            api_key = os.getenv("EMBEDDING_API_KEY")

        if provider == "azure":
            endpoint = os.getenv("AZURE_EMBEDDING_ENDPOINT")

            # Seems a bug, but must provide api key in both headers and api-key argument or we get an error:
            client = OpenAI(
                api_key=api_key,
                base_url=endpoint,
                default_query={"api-version": os.getenv("AZURE_OPENAI_API_VERSION")},
                default_headers={"api-key": api_key},
            )
            return client, model_name, "azure"
        elif provider == "lm-studio":
            endpoint = os.getenv("LM_STUDIO_ENDPOINT")
            client = OpenAI(base_url=endpoint, api_key="lm-studio")
            return client, model_name, "local"

        else:  # OpenAI Direct
            client = OpenAI(api_key=api_key)
            return client, model_name, "openai"


def _calculate_cost(model_name: str, input_tok: int, output_tok: int, provider_type: str) -> float:
    if provider_type == "local":
        return 0.0
    # Match longest keys first so specific models (e.g. o3-mini) win over generic prefixes (e.g. o3).
    keys = sorted(_PRICING_TABLE.keys(), key=len, reverse=True)
    price_key = next((k for k in keys if k in model_name), None)
    if not price_key:
        return 0.0
    prices = _PRICING_TABLE[price_key]
    return round(
        ((input_tok / 1e6) * prices["input"]) + ((output_tok / 1e6) * prices["output"]),
        6,
    )


def get_embedding_vectors(texts: List[str]) -> Dict[str, Any]:
    """
    Generates embedding vectors for a list of texts using the embedding-specific config.

    Args:
        texts: List of texts to generate embeddings for.

    Returns:
        A dictionary containing:
            - "embeddings": A numpy array of embedding vectors.
            - "usage": A dictionary with token usage and cost details.
    """

    client, model, provider = _AIClientFactory.get_client(task_type="embedding")

    batch_size = 100
    verbose = len(texts) > batch_size
    batch_results = []
    total_tokens = 0
    for i in range(0, len(texts), batch_size):
        # if i != 9400:
        #     continue
        batch = texts[i : i + batch_size]
        if verbose:
            print(f"Getting embedding vectors for batch {i + 1} - {i + len(batch)} ({len(texts)} total)")
        response = client.embeddings.create(input=batch, model=model)
        data = sorted(response.data, key=lambda x: x.index)
        np_vectors = np.array([item.embedding for item in data])
        batch_results.append(np_vectors)
        usage = response.usage
        total_tokens = total_tokens + usage.prompt_tokens
    total_cost = _calculate_cost(model, total_tokens, 0, provider)

    return {
        "embeddings": np.concatenate(batch_results, axis=0),
        "usage": {
            "input_tokens": total_tokens,
            "output_tokens": 0,
            "reasoning_tokens": 0,
            "total_cost_usd": total_cost,
            "model_used": model,
        },
    }



if __name__ == "__main__":
    texts = ["Acute Myocardial Infarction", "Liver Failure"]
    embeddings_result = get_embedding_vectors(texts)
    print("Embeddings Result:", embeddings_result)

