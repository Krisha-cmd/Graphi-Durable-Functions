import json
import logging

try:
    import redis
except Exception:
    redis = None

from shared.utils import normalize_doi


def _load_config():
    return json.load(open("config.json"))


def main(params: dict):
    """Update progress for a DOI in Redis. params: { "doi": <doi>, "progress": <int> }

    Stores the numeric progress value under the normalized DOI key.
    """
    doi = params.get("doi")
    progress = params.get("progress")
    cfg = _load_config().get("redis", {})
    url = cfg.get("url")
    key = normalize_doi(doi)

    if not url:
        logging.warning("Redis URL not configured; skipping progress update for %s", doi)
        return {"status": "skipped"}

    try:
        from shared.redis_client import get_redis_client

        r = get_redis_client()
        if r is None:
            logging.warning("No redis client available; skipping progress update for %s", doi)
            return {"status": "skipped"}

        # store simple numeric progress (as string)
        r.set(key, str(int(progress)))
        return {"status": "ok", "progress": int(progress)}
    except Exception:
        logging.exception("Failed to update progress to Redis for %s", doi)
        return {"status": "error"}
