"""Pinecone client helper that supports multiple client APIs.

Preferred usage (if available):
  from pinecone import Pinecone
  pc = Pinecone(api_key=...)
  index = pc.Index(name)

Fallback: old-style `pinecone` package with `pinecone.init(...)` and `pinecone.Index(name)`.
This helper reads `config.json` for api_key/environment/index_name when needed.
"""
import json
import logging
from typing import Optional


def _load_config():
    try:
        return json.load(open("config.json"))
    except Exception:
        return {}


def get_pinecone_index(index_name: Optional[str] = None):
    cfg = _load_config().get("pinecone", {})
    api_key = cfg.get("api_key")
    idx_name = index_name or cfg.get("index_name")

    if not api_key or not idx_name:
        logging.warning("Pinecone api_key or index_name missing in config.json")
        return None

    # Preferred new client API
    try:
        from pinecone import Pinecone

        pc = Pinecone(api_key=api_key)
        return pc.Index(idx_name)
    except Exception:
        pass

    # Fallback to old-style pinecone module
    try:
        import pinecone as pine_mod

        try:
            pine_mod.init(api_key=api_key)
        except Exception:
            # init may be optional for some builds
            pass
        return pine_mod.Index(idx_name)
    except Exception:
        logging.exception("No Pinecone client available")
        return None
