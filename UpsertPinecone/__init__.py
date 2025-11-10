import json
import logging
from shared.pinecone_client import get_pinecone_index
from shared.utils import normalize_doi

OPENALEX_PREFIX = "https://openalex.org/"


def _clean_value(v):
    """Recursively clean values in metadata:
    - Strip OpenAlex prefix from strings that start with it
    - Recurse into lists and dicts
    - Remove any 'abstract' keys in dicts
    """
    if isinstance(v, str):
        if v.startswith(OPENALEX_PREFIX):
            return v[len(OPENALEX_PREFIX) :]
        return v
    if isinstance(v, list):
        return [_clean_value(x) for x in v]
    if isinstance(v, dict):
        cleaned = {}
        for k, val in v.items():
            if k == "abstract":
                # drop abstract from metadata
                continue
            cleaned[k] = _clean_value(val)
        return cleaned
    return v


def _clean_metadata(meta):
    if not isinstance(meta, dict):
        return meta
    return _clean_value(meta)


def main(params: dict):
    doi = params.get("doi")
    abstract = params.get("abstract") or "NA"
    metadata = params.get("metadata") or {}

    if isinstance(abstract, list):
        abstract = " ".join(abstract)

    # Clean metadata before sending to Pinecone: remove abstract fields and
    # strip the https://openalex.org/ prefix from strings found in arrays/dicts.
    cleaned_metadata = _clean_metadata(metadata)

    # Helpers to produce flat lists of strings for authors, keywords and
    # references. This ensures Pinecone metadata fields are simple arrays of
    # strings (no nested dicts/jsons).
    def _strip_prefix(s: str) -> str:
        if not isinstance(s, str):
            return str(s)
        return s[len(OPENALEX_PREFIX) :] if s.startswith(OPENALEX_PREFIX) else s

    def _to_string_list(val):
        out = []
        if val is None:
            return out
        if isinstance(val, str):
            # split comma-separated strings
            parts = [p.strip() for p in val.split(",") if p.strip()]
            return [_strip_prefix(p) for p in parts]
        if isinstance(val, list):
            for item in val:
                if item is None:
                    continue
                if isinstance(item, str):
                    out.append(_strip_prefix(item))
                    continue
                if isinstance(item, dict):
                    # prefer common string fields
                    for key in ("display_name", "name", "title", "id", "label"):
                        v = item.get(key)
                        if isinstance(v, str) and v:
                            out.append(_strip_prefix(v))
                            break
                    else:
                        # fallback: flatten any string values inside
                        for v in item.values():
                            if isinstance(v, str) and v:
                                out.append(_strip_prefix(v))
                                break
                    continue
                # fallback to string conversion
                out.append(_strip_prefix(str(item)))
            return out
        if isinstance(val, dict):
            # sometimes a dict maps token->pos or id->meta; try to extract stringy parts
            for k, v in val.items():
                if isinstance(v, str):
                    out.append(_strip_prefix(v))
                elif isinstance(v, list):
                    out.extend(_to_string_list(v))
                elif isinstance(v, dict):
                    # look for inner string fields
                    for key in ("display_name", "name", "title", "id", "label"):
                        sv = v.get(key)
                        if isinstance(sv, str) and sv:
                            out.append(_strip_prefix(sv))
                            break
            return out
        # last resort
        return [_strip_prefix(str(val))]

    # Try several common keys used by OpenAlex for authors, references and
    # keywords; produce flat lists and assign to those canonical keys.
    def _extract_first_list(meta, keys):
        for k in keys:
            if k in meta and meta[k]:
                return _to_string_list(meta[k])
        return []

    authors_keys = ["authors", "authorships", "authorships_parsed", "author"]
    references_keys = ["references", "referenced_works"]
    keywords_keys = ["keywords", "concepts", "subjects", "tags"]

    authors_list = _extract_first_list(cleaned_metadata, authors_keys)
    references_list = _extract_first_list(cleaned_metadata, references_keys)
    keywords_list = _extract_first_list(cleaned_metadata, keywords_keys)

    # Overwrite/add canonical simple lists
    cleaned_metadata["authors"] = authors_list
    cleaned_metadata["references"] = references_list
    cleaned_metadata["keywords"] = keywords_list

    idx = get_pinecone_index()
    if idx is None:
        logging.warning(
            "Pinecone not configured or client missing; skipping upsert for %s", doi
        )
        return {"status": "skipped"}

    item_id = doi.replace("/", "_")

    try:
        # Build payload with canonical top-level metadata fields instead of a
        # single collective metadata JSON. This keeps authors/keywords/
        # references as simple lists of strings.
        payload = {
            "id": normalize_doi(doi),
            "abstract": abstract,  # used for embedding by ComputeEmbeddings
            "authors": cleaned_metadata.get("authors", ["NA"]),
            "references": cleaned_metadata.get("references", ["NA"]),
            "keywords": cleaned_metadata.get("keywords", ["NA"]),
        }
        print("Upserting to Pinecone:", payload)
        idx.upsert_records("my-namespace", [payload])
        return {"status": "ok", "id": item_id}
    except Exception as e:
        logging.exception("Failed upsert to Pinecone for %s", doi)
        return {"status": "error", "error": str(e)}
