from typing import Dict


def get_citation(text: str) -> Dict[str, str]:
    """Placeholder citation extraction.

    Replace this with your actual citation logic (NLP model or DB lookup).
    Returns a small example structure for demonstration.
    """
    # Very simple placeholder: return the first 200 chars and a fake source
    snippet = (text or "").strip()[:200]
    return {
        "citation": snippet,
        "source": "example-corpus",
        "confidence": "0.5",
    }
