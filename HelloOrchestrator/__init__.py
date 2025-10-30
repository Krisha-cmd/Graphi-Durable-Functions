import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    # A simple orchestrator that calls the SayHello activity for three cities
    outputs = []
    outputs.append((yield context.call_activity('SayHello', 'Tokyo')))
    outputs.append((yield context.call_activity('SayHello', 'Seattle')))
    outputs.append((yield context.call_activity('SayHello', 'London')))
    return outputs


main = df.Orchestrator.create(orchestrator_function)
