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
import base64
import os
import traceback

from actionengine.actions import Action
from actionengine.sdk import interaction
from actionengine.sdk.google.client import get_gemini_client
from actionengine.sdk.rehydrate_interaction import REHYDRATE_INTERACTION_SCHEMA
from google.genai import types


async def generate_content_gemini(action: Action):
    input_timeout = 10.0

    interaction_token, api_key = await asyncio.gather(
        action["interaction_token"].consume(timeout=input_timeout),
        action["api_key"].consume(timeout=input_timeout),
    )

    interaction_id, next_output_seq, next_thought_seq = (
        await interaction.resolve_token_to_id_and_seqs(interaction_token)
    )
    if interaction_id is None:
        interaction_id = base64.urlsafe_b64encode(os.urandom(6)).decode("utf-8")

    rehydrate_action = (
        Action.from_schema(REHYDRATE_INTERACTION_SCHEMA)
        .bind_handler(interaction.rehydrate_interaction)
        .run()
    )
    await rehydrate_action["interaction_token"].put_and_finalize(
        interaction_token
    )

    contents = []
    message_idx = 0
    async for message in rehydrate_action["previous_messages"]:
        contents.append(
            types.Content(
                parts=[types.Part(text=message)],
                role="user" if len(contents) % 2 == 0 else "model",
            )
        )
        message_idx += 1

    gemini_client = get_gemini_client(api_key)

    chat_input = await action["chat_input"].consume()

    contents.append(
        types.Content(parts=[types.Part(text=chat_input)], role="user")
    )

    system_instructions = []
    async for instruction in action["system_instructions"]:
        system_instructions.append(instruction)

    retries_left = 3
    while retries_left > 0:
        try:
            config = types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(
                    include_thoughts=True,
                    thinking_budget=4096,
                ),
                tools=[types.Tool(google_search=types.GoogleSearch())],
            )
            if system_instructions:
                config.system_instruction = system_instructions

            stream = await gemini_client.models.generate_content_stream(
                model="gemini-3-flash-preview",
                contents=contents,
                config=config,
            )

            output = ""
            thought = ""

            thoughts_finalised = False

            try:
                async for chunk in stream:
                    for candidate in chunk.candidates:
                        if not candidate.content:
                            continue
                        if not candidate.content.parts:
                            continue

                        for part in candidate.content.parts:
                            if not part.thought:
                                if not thoughts_finalised:
                                    await action["thoughts"].finalize()
                                    thoughts_finalised = True
                                await action["output"].put(part.text)
                                output += part.text
                            else:
                                await action["thoughts"].put(part.text)
                                thought += part.text
                await action["output"].finalize()
            finally:
                if not thoughts_finalised:
                    await action["thoughts"].finalize()

            interaction_token = await interaction.save_turn(
                interaction_id,
                chat_input,
                output,
                thought,
                next_output_seq,
                next_thought_seq,
            )
            await action["new_interaction_token"].put_and_finalize(
                interaction_token
            )
            break
        except Exception:
            retries_left -= 1
            await action["output"].put(
                f"Retrying due to an internal error... {retries_left} retries left."
            )
            if retries_left == 0:
                await action["output"].put("Failed to connect to Gemini API.")
                traceback.print_exc()
                return
