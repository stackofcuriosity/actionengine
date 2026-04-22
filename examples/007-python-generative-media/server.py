import argparse
import asyncio
import logging
import os

import actionengine
from actionengine.sdk import interaction, rehydrate_interaction
from actionengine.sdk.anthropic.generate_content_claude import (
    GENERATE_CONTENT_CLAUDE_SCHEMA,
)
from actionengine.sdk.anthropic.generate_content_claude_handler import (
    generate_content_claude,
)
from actionengine.sdk.google.generate_content_gemini import (
    GENERATE_CONTENT_GEMINI_SCHEMA,
)
from actionengine.sdk.google.generate_content_gemini_handler import (
    generate_content_gemini,
)
from actionengine.sdk.llm.generate_content import (
    GENERATE_CONTENT_SCHEMA,
)
from actionengine.sdk.llm.generate_content_handler import (
    generate_content,
)
from actionengine.sdk.ollama.generate_content_ollama import (
    GENERATE_CONTENT_OLLAMA_SCHEMA,
)
from actionengine.sdk.ollama.generate_content_ollama_handler import (
    generate_content_ollama,
)


import actions


def make_action_registry():
    registry = actionengine.ActionRegistry()

    registry.register("echo", actions.echo.SCHEMA, actions.echo.run)
    registry.register(
        "rehydrate_interaction",
        rehydrate_interaction.REHYDRATE_INTERACTION_SCHEMA,
        interaction.rehydrate_interaction,
    )
    registry.register(
        "infer_updated_facts",
        actions.memory.INFER_UPDATED_FACTS_SCHEMA,
        actions.memory.infer_updated_facts,
    )
    registry.register(
        "generate_content_claude",
        GENERATE_CONTENT_CLAUDE_SCHEMA,
        generate_content_claude,
    )
    registry.register(
        "generate_content_gemini",
        GENERATE_CONTENT_GEMINI_SCHEMA,
        generate_content_gemini,
    )
    registry.register(
        "generate_content_ollama",
        GENERATE_CONTENT_OLLAMA_SCHEMA,
        generate_content_ollama,
    )
    registry.register(
        "generate_content",
        GENERATE_CONTENT_SCHEMA,
        generate_content,
    )
    registry.register(
        "text_to_image", actions.text_to_image.SCHEMA, actions.text_to_image.run
    )
    registry.register(
        "execute_prompt",
        actions.gemini_fc.EXECUTE_PROMPT_SCHEMA,
        actions.gemini_fc.execute_prompt,
    )
    registry.register(
        "locate_objects",
        actions.sam.LOCATE_OBJECTS_SCHEMA,
        actions.sam.locate_objects,
    )
    registry.register(
        "ocr",
        actions.ocr.SCHEMA,
        actions.ocr.run,
    )

    actions.redis.register_actions(registry)
    actions.deep_research.register_deep_research_actions(registry)

    return registry


REGISTRY = make_action_registry()


def setup_action_engine():
    settings = actionengine.get_global_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True

    # will not be needed later:
    actionengine.to_bytes(
        actions.deep_research.deep_research.DeepResearchAction(type="", id="")
    )
    actionengine.to_bytes(actions.redis.ReadStoreRequest(key=""))
    actionengine.to_bytes(
        actions.redis.WriteRedisStoreRequest(key="", data=b"")
    )
    actionengine.to_chunk(
        actions.text_to_image.DiffusionRequest(
            prompt="a hack to get the schema registered for serialization",
        )
    )
    actionengine.to_bytes(
        actions.ocr.TextBox(text="", bbox=(0, 0, 0, 0)),
    )


async def sleep_forever():
    while True:
        await asyncio.sleep(1)


async def main(args: argparse.Namespace):
    action_registry = make_action_registry()
    service = actionengine.Service(action_registry)
    rtc_config = actionengine.webrtc.RtcConfig()
    rtc_config.turn_servers = [
        actionengine.webrtc.TurnServer.from_string(
            "helena:actionengine-webrtc-testing@actionengine.dev",
        ),
    ]
    server = actionengine.webrtc.WebRtcServer.create(
        service,
        args.host,
        args.webrtc_identity,
        f"wss://{args.webrtc_signalling_server}:{args.webrtc_signalling_port}",
        rtc_config,
    )

    if api_key := os.environ.get("AEP_API_KEY"):
        print("Using API key from AEP_API_KEY environment variable.")
        server.set_signalling_header("X-API-Key", api_key)

    if timed_peer_token := os.environ.get("AEP_TIMED_PEER_TOKEN"):
        print(
            "Using timed peer token from AEP_TIMED_PEER_TOKEN environment variable."
        )
        server.set_signalling_header("X-Timed-Peer-Token", timed_peer_token)

    server.run()
    try:
        await sleep_forever()
    except asyncio.CancelledError:
        print("Shutting down Action Engine server.")
        server.cancel()
    finally:
        await asyncio.to_thread(server.join)


def sync_main(args: argparse.Namespace):
    setup_action_engine()
    asyncio.run(main(args), debug=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Run the Action Engine text-to-image server."
    )

    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host address to bind the server to.",
    )
    parser.add_argument(
        "--webrtc-signalling-server",
        type=str,
        default="actionengine.dev",
        help=(
            "WebRTC signalling server address. You may use actionengine.dev "
            "or your own server, but if you use actionengine.dev, please "
            "also set the identity to something unique."
        ),
    )
    parser.add_argument(
        "--webrtc-signalling-port",
        type=int,
        default=19001,
        help="WebRTC signalling server port.",
    )
    parser.add_argument(
        "--webrtc-identity",
        type=str,
        default=os.environ.get("WEBRTC_SIGNALLING_IDENTITY", "demoserver"),
        help="Our ID for the WebRTC signalling server.",
    )
    print(
        "Using WebRTC identity:",
        os.environ.get("WEBRTC_SIGNALLING_IDENTITY", "demoserver"),
    )

    sync_main(parser.parse_args())
