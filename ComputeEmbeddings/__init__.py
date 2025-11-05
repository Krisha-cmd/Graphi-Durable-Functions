import json
import logging
from typing import List

from shared.embeddings import compute_embeddings


def _try_pinecone_embed(text: str):
    from pinecone import Pinecone
    import json

    cfg = json.load(open("config.json")).get("pinecone", {})
    api_key = cfg.get("api_key")
    if not api_key:
        raise RuntimeError("pinecone api_key missing")

    pc = Pinecone(api_key=api_key)
    info = pc.describe_index("graphi")
    print("Graphi info", info)
    resp = pc.inference.embed(model="", inputs=[text])
    return resp[0].values



def main(text: str):
    # Compute a single embedding vector for `text`.
    print("entered compute embeddings with text length:", len(text) if text else 0)
    if not text:
        return []

    # First attempt: use Pinecone integrated embeddings (if available)
    try:
        vec = _try_pinecone_embed(text)
        if vec:
            return vec
    except Exception:
        pass

    # Fallback to local/shared embedding implementation
    try:
        vecs = compute_embeddings([text])
        return vecs[0] if vecs else []
    except Exception:
        logging.exception("Failed to compute embeddings")
        return []
