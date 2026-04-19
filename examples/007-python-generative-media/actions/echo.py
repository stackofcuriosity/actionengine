import actionengine


async def run(action: actionengine.Action) -> None:
    response = action.get_output("response")
    try:
        async for message in action.get_input("text"):
            await response.put(message)
    finally:
        await response.finalize()


SCHEMA = actionengine.ActionSchema(
    name="echo",
    inputs=[("text", "text/plain")],
    outputs=[("response", "text/plain")],
    description='Echoes back the text provided in the input "text" into the output "response", chunk by chunk.',
)
