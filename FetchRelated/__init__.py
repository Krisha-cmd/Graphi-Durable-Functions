import requests
import json
from typing import List

def _openalex_get(url: str, params=None):
    headers = {}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    # print("Returned", url, resp.json())
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
        # OpenAlex returns referenced_works as OpenAlex work URLs/IDs. We need
        # to fetch each work and extract its 'doi' field. To speed this up we
        # resolve them concurrently with a small pool.
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _resolve_openalex_to_doi(rid: str):
            # Convert an OpenAlex work reference (could be a webpage URL or ID)
            # into an API call to fetch the work JSON and extract its DOI.
            try:
                # normalize id: accept forms like 'https://openalex.org/W123' or 'W123'
                oid = rid
                if isinstance(rid, str) and "openalex.org" in rid:
                    oid = rid.rstrip("/\n \t").split("/")[-1]

                api_url = f"https://api.openalex.org/works/{oid}"

                # small retry (2 attempts)
                for attempt in range(2):
                    try:
                        resp = _openalex_get(api_url)
                        # DOI may be available in resp['ids']['doi'] or top-level 'doi'
                        doi_val = None
                        ids = resp.get("ids") or {}
                        doi_val = ids.get("doi") or resp.get("doi")
                        if doi_val:
                            return doi_val
                        # no DOI present for this work
                        return None
                    except Exception:
                        # small backoff
                        import time

                        time.sleep(0.2 + 0.2 * attempt)
                        continue
            except Exception:
                return None

        # limit how many references we resolve to a reasonable count
        to_resolve = refs[:10]
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(_resolve_openalex_to_doi, rid): rid for rid in to_resolve}
            for fut in as_completed(futures):
                try:
                    doi_r = fut.result()
                    if doi_r:
                        results.append(doi_r)
                except Exception:
                    # individual failures are expected; continue
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
