import argparse
import asyncio
import uuid

import actionengine

import actions


def make_action_registry():
    registry = actionengine.ActionRegistry()
    registry.register(
        "infer_updated_facts", actions.memory.INFER_UPDATED_FACTS_SCHEMA
    )
    return registry


async def main(args: argparse.Namespace):
    action_registry = make_action_registry()
    node_map = actionengine.NodeMap()
    stream = actionengine.webrtc.make_webrtc_stream(
        str(uuid.uuid4()), "demoserver1", "wss://actionengine.dev:19001"
    )

    session = actionengine.Session(node_map, action_registry)
    session.dispatch_from(stream)

    update_facts = action_registry.make_action(
        "infer_updated_facts", node_map=node_map, stream=stream, session=session
    )
    await asyncio.gather(
        update_facts.call(),
        update_facts["session_token"].put_and_finalize(args.session_token),
        update_facts["fact_node_id"].put_and_finalize(args.fact_node_id),
    )

    async for fact in update_facts["facts"]:
        print(fact)

    await update_facts.wait_until_complete()
    await asyncio.to_thread(stream.half_close)


def setup_action_engine():
    settings = actionengine.get_global_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True

    # a temporary hack to get the schema registered for serialization
    actionengine.to_chunk(actions.text_to_image.ProgressMessage(step=1))


def sync_main(args: argparse.Namespace):
    setup_action_engine()
    asyncio.run(main(args))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the Action Engine client."
    )

    parser.add_argument(
        "--session-token",
        type=str,
        default=None,
        help="The session token to use.",
    )
    parser.add_argument(
        "--fact-node-id",
        type=str,
        default=None,
        help="The node ID to use for facts.",
    )

    args = parser.parse_args()
    sync_main(args)
