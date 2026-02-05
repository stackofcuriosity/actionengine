import asyncio

import actionengine
import pytest


async def run_echo(action: actionengine.Action):
    print("Running echo", flush=True)
    async for chunk in action["input"]:
        await action["output"].put(chunk)
    await action["output"].finalize()
    print("Echo complete", flush=True)


ECHO_SCHEMA = actionengine.ActionSchema(
    name="echo",
    inputs=[("input", "text/plain")],
    outputs=[("output", "text/plain")],
    description="An action that echoes input to output.",
)


def make_action_registry():
    registry = actionengine.ActionRegistry()
    registry.register("echo", ECHO_SCHEMA, run_echo)
    return registry


@pytest.mark.asyncio
async def test_action_runs():
    registry = make_action_registry()
    node_map = actionengine.NodeMap()

    echo = registry.make_action("echo", node_map=node_map).run_in_background()
    await echo["input"].put_and_finalize("Hello!")

    received = await echo["output"].consume(allow_none=True)
    assert received == "Hello!"

    print(f"Received: {received}", flush=True)

    await echo.wait_until_complete()
    print("Echo complete outside", flush=True)


async def main():
    await test_action_runs()


if __name__ == "__main__":
    asyncio.run(main())
