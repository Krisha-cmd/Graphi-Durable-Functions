import requests
import json
from typing import List

def _openalex_get(url: str, params=None):
    headers = {}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    print("Returned", url, resp.json())
    resp.raise_for_status()
    return resp.json()


def main(params: dict) -> List[str]:
    """Fetch related DOIs (either 'citating' or 'references') for a given DOI.

    Returns a list of DOI strings (not full URLs) up to a reasonable limit.
    """
    doi = params.get("doi")
    request_for = (params.get("requestFor") or "").lower()
    if not doi or request_for not in ("citating", "references"):
        return []

    # query the OpenAlex work by DOI
    work_url = f"https://api.openalex.org/works/https://doi.org/{doi}"
    print("Fetching related works for DOI:", doi, "requestFor:", work_url)
    try:
        w = _openalex_get(work_url)
    except Exception:
        return []

    results = []

    # For references: the work contains 'referenced_works' (OpenAlex IDs)
    if request_for == "references":
        refs = w.get("referenced_works", []) or []
        print("Found referenced works:", refs)
        # resolve a handful of referenced works to DOIs
        for rid in refs[:10]:
            try:
                r = _openalex_get(rid)
                doi_r = r.get("doi")
                if doi_r:
                    results.append(doi_r)
            except Exception:
                continue

    else:
        # For citating: query works that cite this work using OpenAlex filter
        openalex_id = w.get("id")
        if openalex_id:
            # OpenAlex supports filter=cites:OPENALEX_ID
            try:
                page = 1
                per_page = 50
                while True:
                    params = {"filter": f"cites:{openalex_id}", "per_page": per_page, "page": page}
                    r = _openalex_get(f"https://api.openalex.org/works", params=params)
                    for item in r.get("results", []):
                        doi_r = item.get("doi")
                        if doi_r:
                            results.append(doi_r)
                    # stop if fewer than per_page or we collected enough
                    if len(r.get("results", [])) < per_page or len(results) >= 200:
                        break
                    page += 1
            except Exception:
                pass

    # normalize and dedupe
    seen = set()
    out = []
    for d in results:
        if not d:
            continue
        norm = d.strip()
        if norm not in seen:
            seen.add(norm)
            out.append(norm)
    return out
