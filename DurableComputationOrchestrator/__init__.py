import azure.durable_functions as df


def _normalize_doi(doi: str) -> str:
    if not doi:
        return doi
    # strip anything up to .org/ if present
    if ".org/" in doi:
        return doi.split('.org/')[-1]
    return doi


def orchestrator_function(context: df.DurableOrchestrationContext):
    input_ = context.get_input() or {}
    doi = _normalize_doi(input_.get("doi"))
    request_for = (input_.get("requestFor") or "").lower()

    if not doi or request_for not in ("citating", "references"):
        return {"error": "Missing or invalid doi/requestFor"}
    print("Orchestrator started for DOI:", doi, "requestFor:", request_for)
    # Level 0 is the input
    level0 = [doi]

    # Fetch gen1: direct children (citers or references depending on request_for)
    gen1 = []
    for d in level0:
        related = yield context.call_activity('FetchRelated', {"doi": d, "requestFor": request_for})
        gen1.extend(related or [])
    gen1 = list(dict.fromkeys(gen1))  # dedupe preserving order

    print("Gen1 DOIs:", gen1)
    # Fetch gen2: children of gen1
    gen2 = []
    for d in gen1:
        related = yield context.call_activity('FetchRelated', {"doi": d, "requestFor": request_for})
        gen2.extend(related or [])
    gen2 = [x for x in dict.fromkeys(gen2) if x not in gen1 and x not in level0]

    print("Gen2 DOIs:", gen2)

    # Combine all DOIs to process embeddings and metadata
    all_dois = list(dict.fromkeys(level0 + gen1 + gen2))
    print(all_dois)

    total = len(all_dois)
    processed = 0
    thresholds = [20, 40, 60, 80]

    # set initial progress to 20%
    yield context.call_activity('UpdateProgress', {"doi": doi, "progress": 20})

    # For each DOI: fetch metadata -> compute embeddings -> upsert pinecone
    # Collect metadata and vectors into maps so SaveCosmosRedis can use them
    metadata_map = {}
    # vectors_map = {}
    for d in all_dois:
        meta = yield context.call_activity('GetMetadata', d)
        abstract = (meta or {}).get('abstract') or ""
        print("Abstract extracted:",abstract)
        # compute embeddings (may use Pinecone integrated embeddings or fallback)

        # upsert into pinecone including vector and cleaned metadata
        yield context.call_activity('UpsertPinecone', {"doi": d, "abstract": abstract, "metadata": meta})

        metadata_map[d] = meta or {}
        # vectors_map[d] = vector or []

        processed += 1
        pct = int(processed / total * 100)
        # update progress at thresholds 20,40,60,80
        while thresholds and pct >= thresholds[0]:
            t = thresholds.pop(0)
            print("Updating progress to", t, "% for DOI:", doi)
            yield context.call_activity('UpdateProgress', {"doi": doi, "progress": t})

    # After upserts, compute similarity scores via Pinecone
    children = [d for d in (gen1 + gen2) if d != doi]
    scores = yield context.call_activity('ComputeScores', {"parent": doi, "children": children})

    # Save assembled results into Cosmos then Redis for the input DOI (include scores)
    # Pass collected metadata and vectors to SaveCosmosRedis to avoid re-querying OpenAlex
    yield context.call_activity('SaveCosmosRedis', {"doi": doi, "requestFor": request_for, "gen1": gen1, "gen2": gen2, "scores": scores, "metadata_map": metadata_map})

    return {"status": "started", "processed": len(all_dois)}


main = df.Orchestrator.create(orchestrator_function)
