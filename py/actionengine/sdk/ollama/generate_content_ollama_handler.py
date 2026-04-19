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

import base64
import os
import random
import traceback

from ollama import AsyncClient, Options

from actionengine.actions import Action
from actionengine.sdk import interaction
from actionengine.sdk.rehydrate_interaction import REHYDRATE_INTERACTION_SCHEMA


async def generate_content_ollama(action: Action):
    interaction_token = await action["interaction_token"].consume()
    interaction_id, next_output_seq, next_thought_seq = (
        await interaction.resolve_token_to_id_and_seqs(interaction_token)
    )
    if interaction_id is None:
        interaction_id = base64.urlsafe_b64encode(os.urandom(6)).decode("utf-8")

    rehydrate = (
        Action.from_schema(REHYDRATE_INTERACTION_SCHEMA)
        .bind_handler(interaction.rehydrate_interaction)
        .run()
    )
    await rehydrate["interaction_token"].put_and_finalize(interaction_token)

    system_instructions = []
    async for instruction in action["system_instructions"]:
        system_instructions.append(instruction)

    joined_instruction = (
        "\n".join(system_instructions)
        if system_instructions
        else "You are a helpful assistant called DeepSeek. "
        "You are running on Helena's local server using Ollama. "
    )

    messages = [
        {"role": "system", "content": joined_instruction},
    ]
    message_idx = 0
    async for message in rehydrate["previous_messages"]:
        if message_idx % 2 == 0:
            messages.append({"role": "user", "content": message})
        else:
            messages.append({"role": "assistant", "content": message})
        message_idx += 1

    thoughts = []
    async for thought in rehydrate["previous_thoughts"]:
        thoughts.append(thought)

    chat_input = await action["chat_input"].consume()
    messages.append(
        {
            "role": "user",
            "content": f"{'\n'.join(system_instructions)}\n"
            f"The following text is the user's input:\n\n {chat_input}",
        }
    )

    retries_left = 3
    while retries_left > 0:
        try:
            ollama_client = AsyncClient()

            stream = await ollama_client.chat(
                model="deepseek-r1:8b",
                messages=messages,
                stream=True,
                think=True,
                options=Options(seed=random.randint(0, 2**31 - 1)),
            )

            output = ""
            thought = ""

            async for chunk in stream:
                if "content" in chunk["message"]:
                    await action["output"].put(chunk["message"]["content"])
                    output += chunk["message"]["content"]

                if "thinking" in chunk["message"]:
                    await action["thoughts"].put(chunk["message"]["thinking"])
                    thought += chunk["message"]["thinking"]
            await action["output"].finalize()
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
                await action["output"].put("Failed to connect to Ollama API.")
                traceback.print_exc()
                return
