import json
import logging
import requests

try:
    from azure.cosmos import CosmosClient
except Exception:
    CosmosClient = None

from shared.utils import normalize_doi
from shared.pinecone_client import get_pinecone_index


def _load_config():
    try:
        return json.load(open("config.json"))
    except Exception:
        return {}


def _fetch_metadata(doi: str) -> dict:
    if not doi:
        return {}
    try:
        resp = requests.get(f"https://api.openalex.org/works/https://doi.org/{doi}", timeout=20)
        w = resp.json()
    except Exception:
        return {}

    title = w.get("title")
    authors = [a.get("author", {}).get("display_name") for a in w.get("authorships", []) if a.get("author")]
    year = w.get("publication_year")
    venue = (w.get("host_venue") or {}).get("display_name")
    citations = w.get("cited_by_count") or 0
    references = len(w.get("referenced_works", []) or [])

    # reconstruct abstract like GetMetadata
    abstract = ""
    try:
        inv = w.get("abstract_inverted_index")
        if inv and isinstance(inv, dict):
            max_pos = -1
            for positions in inv.values():
                for p in positions or []:
                    try:
                        pi = int(p)
                        if pi > max_pos:
                            max_pos = pi
                    except Exception:
                        continue
            if max_pos >= 0:
                tokens = [""] * (max_pos + 1)
                for word, positions in inv.items():
                    for p in positions or []:
                        try:
                            pi = int(p)
                            if 0 <= pi < len(tokens):
                                tokens[pi] = word
                        except Exception:
                            continue
                abstract = " ".join(tokens).split()
                abstract = " ".join(abstract)
        if not abstract:
            abstract = w.get("abstract") or ""
    except Exception:
        abstract = w.get("abstract") or ""

    keywords = [c.get("display_name") for c in w.get("concepts", [])][:10]

    return {
        "id": doi,
        "title": title,
        "authors": [a for a in authors if a],
        "year": year,
        "venue": venue,
        "doi": doi,
        "citations": citations,
        "references": references,
        "keywords": keywords,
        "abstract": abstract,
        "citating": w.get("cited_by_count", 0),
        "referenced_works": w.get("referenced_works_count", 0),
    }


def _fetch_vectors(index, ids: list) -> dict:
    """Fetch vectors from Pinecone index for a list of normalized ids.
    Returns mapping id -> vector (or empty list).
    """
    out = {}
    if not index or not ids:
        return out
    try:
        res = index.fetch(ids=ids, namespace="my-namespace")
        # vectors = resp.get("vectors", {})
        for _id in ids:
            out[_id]=res.vectors[_id].values
    except Exception:
        logging.exception("Failed to fetch vectors from Pinecone")
    return out


def main(params: dict):
    doi = params.get("doi")
    request_for = params.get("requestFor")
    gen1 = params.get("gen1") or []
    gen2 = params.get("gen2") or []
    scores = params.get("scores") or {}

    cfg = _load_config()

    # Build list of children depending on request_for
    children = [d for d in (gen1 + gen2) if d]

    # Collect all DOIs we need metadata for: root + children
    all_dois = [doi] + children

    # Use provided metadata_map and vectors_map if the orchestrator passed them
    meta_map = params.get("metadata_map") if isinstance(params.get("metadata_map"), dict) else None
    # vec_map = params.get("vectors_map") if isinstance(params.get("vectors_map"), dict) else None

    if meta_map is None:
        # Fetch metadata for all DOIs (fallback)
        meta_map = {}
        for d in all_dois:
            try:
                meta_map[d] = _fetch_metadata(d)
            except Exception:
                meta_map[d] = {}

    idx = get_pinecone_index()
    normalized_ids = [normalize_doi(d) for d in all_dois]
    vec_map = _fetch_vectors(idx, normalized_ids) if idx is not None else {}

    # Compress function: reduce vector dimensionality for stored JSON only
    def _compress_vector(vec, target=16):
        if not vec:
            return []
        try:
            vals = [float(x) for x in vec]
        except Exception:
            return []
        L = len(vals)
        if L <= target:
            return vals[:target] + [0.0] * max(0, target - L)
        import math

        out = []
        for i in range(target):
            start = int(math.floor(i * L / target))
            end = int(math.floor((i + 1) * L / target))
            if end <= start:
                end = min(start + 1, L)
            seg = vals[start:end]
            out.append(sum(seg) / len(seg) if seg else 0.0)
        return out

    # Helper to build paper object
    def build_paper(d, include_score=False):
        m = meta_map.get(d) or {}
        nid = normalize_doi(d)
        v = vec_map.get(nid) or []
        cv = _compress_vector(v)
        paper = {
            "doi": d,
            "title": m.get("title") or "",
            "year": m.get("year"),
            "authors": m.get("authors") or [],
            "venue": m.get("venue") or "",
            "keywords": m.get("keywords") or [],
            "abstract": m.get("abstract") or "",
            "vector": cv,
            "score": float(scores.get(d, 0.0)) if include_score else None,
            "references": m.get("references") or 0,
            "citations": m.get("citations") or 0,
            "citatingPapers": [],
            "referredPapers": [],
        }
        # remove score key when not requested
        if not include_score:
            paper.pop("score", None)
        return paper

    # Build root object
    root_meta = meta_map.get(doi) or {}
    root_vector = vec_map.get(normalize_doi(doi)) or []
    root_vector = _compress_vector(root_vector)

    result = {
        "id": normalize_doi(doi),
        "doi": doi,
        "title": root_meta.get("title") or "",
        "year": root_meta.get("year"),
        "authors": root_meta.get("authors") or [],
        "venue": root_meta.get("venue") or "",
        "keywords": root_meta.get("keywords") or [],
        "abstract": root_meta.get("abstract") or "",
        "vector": root_vector,
        "citations": root_meta.get("citations") or 0,
        "references": root_meta.get("references") or 0,
        "referredPapers": [],
        "citatingPapers": [],
        "computedReferences": "Y" if request_for == "references" else "N",
        "computedCitating": "Y" if request_for == "citating" else "N",
    }

    # Fill children into appropriate array with detailed metadata and score
    children_objs = [build_paper(d, include_score=True) for d in children]

    if request_for == "references":
        result["referredPapers"] = children_objs
        result["citatingPapers"] = []
    else:
        result["citatingPapers"] = children_objs
        result["referredPapers"] = []

    # Save to Cosmos DB (best-effort) and merge when object partially exists
    cosmos_cfg = cfg.get("cosmos", {})
    if CosmosClient and cosmos_cfg.get("connection_string"):
        try:
            client = CosmosClient.from_connection_string(cosmos_cfg.get("connection_string"))
            db = client.get_database_client(cosmos_cfg.get("database"))
            container = db.get_container_client(cosmos_cfg.get("container"))

            # Attempt to find existing object by doi
            existing = None
            try:
                query = "SELECT * FROM c WHERE c.doi = @doi"
                items = list(container.query_items(query=query, parameters=[{"name":"@doi","value":doi}], enable_cross_partition_query=True))
                if items:
                    existing = items[0]
            except Exception:
                logging.exception("Cosmos query failed")

            if existing:
                # If object exists, prefer any existing child lists; if a list
                # is missing, use the newly computed list. After merging we
                # mark both computed flags as 'Y' (the existing object implies
                # the alternate request was already computed).

                existing_cit = existing.get("citatingPapers")
                existing_ref = existing.get("referredPapers")

                # Choose lists: keep existing if present and non-empty, else use result's
                chosen_cit = existing_cit if existing_cit else result.get("citatingPapers", [])
                chosen_ref = existing_ref if existing_ref else result.get("referredPapers", [])

                # Ensure vectors in child items are compressed for storage
                def _ensure_compressed_list(lst):
                    out_list = []
                    for it in lst or []:
                        if isinstance(it, dict):
                            it_vec = it.get("vector") or []
                            it["vector"] = _compress_vector(it_vec)
                            out_list.append(it)
                        else:
                            out_list.append(it)
                    return out_list

                merged_cit = _ensure_compressed_list(chosen_cit)
                merged_ref = _ensure_compressed_list(chosen_ref)

                existing["citatingPapers"] = merged_cit
                existing["referredPapers"] = merged_ref

                # mark both computed flags as Y since we now have both lists
                existing["computedCitating"] = "Y"
                existing["computedReferences"] = "Y"

                # ensure root-level metadata present (prefer existing values)
                for k in ("authors", "venue", "keywords", "abstract", "vector"):
                    if not existing.get(k) and result.get(k) is not None:
                        existing[k] = result.get(k)

                container.upsert_item(existing)
                result = existing
                
            else:
                container.upsert_item(result)

        except Exception:
            logging.exception("Failed to upsert item to CosmosDB")
    else:
        logging.warning("Cosmos client not configured or missing; skipping Cosmos save")

    # Save to Redis (best-effort) under normalized doi-id key
    redis_cfg = cfg.get("redis", {})
    key = normalize_doi(doi)
    if redis_cfg.get("url"):
        try:
            from shared.redis_client import get_redis_client

            r = get_redis_client()
            if r is not None:
                r.set(key, json.dumps(result))
            else:
                logging.warning("No redis client available; skipping Redis save")
        except Exception:
            logging.exception("Failed to save item to Redis")
    else:
        logging.warning("Redis not configured or client missing; skipping Redis save")

    return {"status": "saved", "doi": doi}
