import azure.functions as func
import azure.durable_functions as df


async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    """HTTP starter that kicks off the DurableComputation orchestrator.

    Expects JSON body or query params: { "doi": "...", "requestFor": "citating"|"references" }
    """
    try:
        body = req.get_json()
    except ValueError:
        body = None

    doi = (body or {}).get("doi") or req.params.get("doi")
    request_for = (body or {}).get("requestFor") or req.params.get("requestFor")

    if not doi or not request_for:
        return func.HttpResponse("Missing 'doi' or 'requestFor'", status_code=400)

    client = df.DurableOrchestrationClient(starter)
    instance_id = await client.start_new('DurableComputationOrchestrator', None, {"doi": doi, "requestFor": request_for})
    return client.create_check_status_response(req, instance_id)
