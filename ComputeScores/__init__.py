import json
import logging
from typing import Dict, List

from shared.utils import normalize_doi
from shared.pinecone_client import get_pinecone_index


def _cosine(a, b):
    # simple cosine similarity
    if not a or not b:
        return 0.0
    try:
        dot = sum(x * y for x, y in zip(a, b))
        import math

        na = math.sqrt(sum(x * x for x in a))
        nb = math.sqrt(sum(x * x for x in b))
        if na == 0 or nb == 0:
            return 0.0
        return dot / (na * nb)
    except Exception:
        return 0.0


def main(params: dict) -> Dict[str, float]:
    """Compute similarity scores between parent DOI and children DOIs using Pinecone.

    params: { "parent": <doi>, "children": [<doi>, ...] }
    returns: { child_doi: score }
    """
    parent = params.get("parent")
    children = params.get("children") or []
    idx = get_pinecone_index()
    if idx is None:
        logging.warning("Pinecone not configured; returning zero scores")
        return {c: 0.0 for c in children}

    try:
        parent_id = normalize_doi(parent)
        # fetch vectors for parent and children
        ids = [parent_id] + [normalize_doi(c) for c in children]

        res = idx.fetch(ids=ids, namespace="my-namespace")
        parent_vec = res.vectors[parent_id].values

        scores = {}
        for c in children:
            cid = normalize_doi(c)
            child_vec = res.vectors[cid].values
            scores[c] = float(_cosine(parent_vec, child_vec)) if child_vec else 0.0
        return scores
    except Exception:
        logging.exception("ComputeScores failed")
        return {c: 0.0 for c in children}
