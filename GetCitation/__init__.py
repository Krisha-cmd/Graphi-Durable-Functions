import azure.functions as func
from shared.citation import get_citation
import json


def main(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP function that returns a citation for provided text.

    Example request body: { "text": "Some document text to cite..." }
    """
    try:
        body = req.get_json()
        text = body.get("text") if isinstance(body, dict) else None
    except ValueError:
        # No json; fallback to query param
        text = req.params.get("text")

    if not text:
        return func.HttpResponse("Missing 'text' in body or query", status_code=400)

    citation = get_citation(text)
    return func.HttpResponse(json.dumps(citation), mimetype="application/json")
