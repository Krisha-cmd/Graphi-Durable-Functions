import json
import logging

import azure.functions as func

from shared.utils import normalize_doi

try:
    from azure.cosmos import CosmosClient
except Exception:
    CosmosClient = None

try:
    import redis
except Exception:
    redis = None

try:
    import pinecone
except Exception:
    pinecone = None


def _load_config():
    try:
        return json.load(open("config.json"))
    except Exception:
        return {}


def main(req: func.HttpRequest) -> func.HttpResponse:
    """DummyStore HTTP API

    Accepts JSON body containing at least "doi". Example body:
    {
      "doi": "10.1234/example.doi",
      "title": "Some Paper",
      "abstract": "Abstract text...",
      "vector": [0.1, 0.2, ...],
      "authors": ["A", "B"]
    }

    The function attempts to:
    - upsert the object into Cosmos DB (if configured)
    - set the object into Redis under the normalized DOI key
    - upsert the vector into Pinecone (if vector provided and configured)

    Returns a JSON object with per-service statuses.
    """
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid or missing JSON body", status_code=400)

    doi = body.get("doi")
    if not doi:
        return func.HttpResponse("Missing 'doi' in request body", status_code=400)

    key = normalize_doi(doi)
    title = body.get("title", "")
    abstract = body.get("abstract", "")
    vector = body.get("vector")
    authors = body.get("authors", [])

    item = {
        "id": key,
        "doi": doi,
        "title": title,
        "authors": authors,
        "abstract": abstract,
        "vector": vector or [],
    }

    cfg = _load_config()
    results = {}

    # Cosmos DB
    cosmos_cfg = cfg.get("cosmos", {})
    if CosmosClient and cosmos_cfg.get("connection_string"):
        try:
            client = CosmosClient.from_connection_string(cosmos_cfg.get("connection_string"))
            db = client.get_database_client(cosmos_cfg.get("database"))
            container = db.get_container_client(cosmos_cfg.get("container"))
            container.upsert_item(item)
            results["cosmos"] = "ok"
        except Exception as e:
            logging.exception("Cosmos upsert failed")
            results["cosmos"] = f"error: {str(e)}"
    else:
        results["cosmos"] = "skipped"

    # Redis
    redis_cfg = cfg.get("redis", {})
    if redis_cfg.get("url"):
        try:
            from shared.redis_client import get_redis_client

            r = get_redis_client()
            if r is not None:
                # store final object as JSON under normalized DOI key
                r.set(key, json.dumps(item))
                results["redis"] = "ok"
            else:
                results["redis"] = "skipped"
        except Exception as e:
            logging.exception("Redis set failed")
            results["redis"] = f"error: {str(e)}"
    else:
        results["redis"] = "skipped"

    # Pinecone
    pine_cfg = cfg.get("pinecone", {})
    api_key = pine_cfg.get("api_key")
    env = pine_cfg.get("environment")
    index_name = pine_cfg.get("index_name")
    if api_key and index_name and vector:
        try:
            from shared.pinecone_client import get_pinecone_index

            idx = get_pinecone_index()
            if idx is not None:
                idx.upsert(vectors=[(key, vector, {"title": title, "doi": doi})])
                results["pinecone"] = "ok"
            else:
                results["pinecone"] = "skipped"
        except Exception as e:
            logging.exception("Pinecone upsert failed")
            results["pinecone"] = f"error: {str(e)}"
    else:
        # if vector missing or pinecone not configured
        results["pinecone"] = "skipped"

    return func.HttpResponse(json.dumps({"status": "ok", "key": key, "services": results}), mimetype="application/json")
