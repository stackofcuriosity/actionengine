# Copyright 2026 The Action Engine Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import logging
import os

from actionengine.actions import Action
from actionengine.sdk.anthropic.generate_content_claude import (
    GENERATE_CONTENT_CLAUDE_SCHEMA,
)
from actionengine.sdk.google.generate_content_gemini import (
    GENERATE_CONTENT_GEMINI_SCHEMA,
)
from actionengine.sdk.llm.generate_content import (
    is_provider_enabled,
)
from actionengine.sdk.ollama.generate_content_ollama import (
    GENERATE_CONTENT_OLLAMA_SCHEMA,
)

_LOGGER = logging.getLogger(__name__)


async def generate_content(action: Action):
    _LOGGER.debug("Running generate_content.")

    api_key = await action["api_key"].consume(timeout=3.0)
    if not api_key:
        await action["output"].put_and_finalize("API key is required.")
        return

    provider = ""
    if api_key == "ollama":
        provider = "ollama"

    if api_key == "alpha-demos" or api_key == "gemini":
        api_key = os.environ.get("GEMINI_API_KEY")
        provider = "gemini"

    if api_key == "claude":
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        provider = "claude"

    if not provider:
        raise ValueError(f"Could not resolve LLM provider for API key.")

    if not is_provider_enabled(provider):
        raise ValueError(
            f"The specified provider {provider} is not enabled or does not exist. "
        )

    schema = None
    match provider:
        case "gemini":
            schema = GENERATE_CONTENT_GEMINI_SCHEMA
        case "claude":
            schema = GENERATE_CONTENT_CLAUDE_SCHEMA
        case "ollama":
            schema = GENERATE_CONTENT_OLLAMA_SCHEMA

    handler = None
    match provider:
        case "gemini":
            from actionengine.sdk.google.generate_content_gemini_handler import (
                generate_content_gemini,
            )

            handler = generate_content_gemini
        case "claude":
            from actionengine.sdk.anthropic.generate_content_claude_handler import (
                generate_content_claude,
            )

            handler = generate_content_claude
        case "ollama":
            from actionengine.sdk.ollama.generate_content_ollama_handler import (
                generate_content_ollama,
            )

            handler = generate_content_ollama

    if handler is None:
        raise RuntimeError(f"Could not resolve handler for {provider}.")

    generate = (
        Action.from_schema(schema)
        .bind_handler(handler)
        .bind_registry(action.get_registry())
        .run()
    )
    await generate["api_key"].put_and_finalize(api_key)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(action["output"].copy_from(generate["output"]))
        tg.create_task(action["thoughts"].copy_from(generate["thoughts"]))
        tg.create_task(
            action["new_interaction_token"].copy_from(
                generate["new_interaction_token"]
            )
        )

        tg.create_task(generate["chat_input"].copy_from(action["chat_input"]))
        tg.create_task(
            generate["system_instructions"].copy_from(
                action["system_instructions"]
            )
        )
        tg.create_task(
            generate["interaction_token"].copy_from(action["interaction_token"])
        )
        tg.create_task(generate["config"].copy_from(action["config"]))
        tg.create_task(generate["tools"].copy_from(action["tools"]))

    await generate.wait_until_complete()
