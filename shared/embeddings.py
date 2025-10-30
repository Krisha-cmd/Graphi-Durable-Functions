from typing import List


def compute_embeddings(texts: List[str]) -> List[List[float]]:
    """Placeholder embeddings function.

    Replace with real embedding implementation (OpenAI, SBERT, etc.).
    For now returns zero vectors with length 3 for each input for demo purposes.
    """
    vectors = []
    for _ in texts:
        vectors.append([0.0, 0.0, 0.0])
    return vectors
