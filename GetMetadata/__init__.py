import requests
import json


def main(doi: str) -> dict:
    """Fetch metadata for a DOI from OpenAlex and return a simplified JSON structure."""
    if not doi:
        return {}
    try:
        w = requests.get(f"https://api.openalex.org/works/https://doi.org/{doi}", timeout=20).json()
    except Exception:
        return {}

    # Map OpenAlex fields to our metadata shape
    title = w.get("title")
    authors = [a.get("author", {}).get("display_name") for a in w.get("authorships", []) if a.get("author")]
    year = w.get("publication_year")
    venue = (w.get("host_venue") or {}).get("display_name")
    citations = w.get("cited_by_count")
    references = len(w.get("referenced_works", []) or [])
    # OpenAlex sometimes returns an inverted index for the abstract where
    # keys are words and values are lists of positions. Reconstruct the
    # abstract from that when present; otherwise use the `abstract` field.
    abstract = ""
    try:
        inv = w.get("abstract_inverted_index")
        if inv and isinstance(inv, dict):
            # determine size
            max_pos = -1
            for positions in inv.values():
                # positions is typically a list of ints
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
                # join and collapse any accidental multiple spaces
                abstract = " ".join(tokens).split()
                abstract = " ".join(abstract)
        # fallback to plain abstract field
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
        "referenced_works": w.get("referenced_works_count", 0)
    }
