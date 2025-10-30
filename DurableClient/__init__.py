import azure.functions as func
import azure.durable_functions as df


async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    # HTTP starter that launches the HelloOrchestrator orchestration
    client = df.DurableOrchestrationClient(starter)
    instance_id = await client.start_new('HelloOrchestrator', None, None)
    return client.create_check_status_response(req, instance_id)
