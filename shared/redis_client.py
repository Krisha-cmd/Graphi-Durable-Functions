"""Redis client factory that prefers Upstash client when available,
and falls back to standard redis-py.

This module keeps Redis usage in the codebase consistent and allows
you to swap providers by installing the appropriate client library.
"""
from typing import Optional
import json


def _load_config() -> dict:
    try:
        return json.load(open("config.json"))
    except Exception:
        return {}


def get_redis_client(url: Optional[str] = None):
    """Return a redis-like client.

    Behavior:
    - If `url` is provided, try to create a client for that URL.
    - If `url` is None, read `config.json` for `redis.url` and optional `redis.token`.

    Prefers Upstash client (`upstash_redis.Redis`) when available, falling back to
    redis-py (`redis.from_url`). Returns None when no client can be created.
    """
    cfg = _load_config()
    redis_cfg = cfg.get("redis", {})
    token = redis_cfg.get("token")

    if url is None:
        url = redis_cfg.get("url")

    if not url:
        return None

    # Try Upstash client first
    try:
        import upstash_redis
        # prefer explicit Redis constructor with token when available
        try:
            Redis = getattr(upstash_redis, "Redis", None)
            if Redis is not None:
                # upstash_redis.Redis(url=..., token=...)
                try:
                    return Redis(url=url, token=token) if token else Redis(url=url)
                except TypeError:
                    # fallback to from_url if signature differs
                    if hasattr(Redis, "from_url"):
                        return Redis.from_url(url)
            # otherwise try module-level from_url
            if hasattr(upstash_redis, "from_url"):
                return upstash_redis.from_url(url)
        except Exception:
            # If Upstash import succeeds but client initialization fails, continue to fallback
            pass
    except Exception:
        upstash_redis = None

    # Fallback to redis-py
    try:
        import redis as redis_py

        return redis_py.from_url(url)
    except Exception:
        return None
