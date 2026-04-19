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
import json
import os

import anthropic
from actionengine.actions import Action
from actionengine.node_map import NodeMap
from actionengine.sdk.anthropic.client import get_anthropic_client
from actionengine.sdk.anthropic.generate_content_claude import (
    CreateMessageConfig,
)
from actionengine.sdk import interaction
from actionengine.sdk.llm_tool_runner import TOOL_RUNNER_SCHEMA
from actionengine.sdk.rehydrate_interaction import REHYDRATE_INTERACTION_SCHEMA


async def generate_content_claude(
    action: Action,
):
    input_timeout = 10.0

    config: CreateMessageConfig = await action["config"].consume(
        timeout=input_timeout, allow_none=True
    )
    config = config or CreateMessageConfig()

    interaction_token, api_key = await asyncio.gather(
        action["interaction_token"].consume(timeout=input_timeout),
        action["api_key"].consume(timeout=input_timeout),
    )

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

    messages = []
    message_idx = 0
    async for message in rehydrate["previous_messages"]:
        messages.append(
            {
                "role": "user" if message_idx % 2 == 0 else "assistant",
                "content": message,
            }
        )
        message_idx += 1
    await rehydrate.wait_until_complete()

    chat_input = await action["chat_input"].consume(timeout=input_timeout)
    messages.append({"role": "user", "content": chat_input})

    system_prompt = None
    async for instruction in action["system_instructions"]:
        if system_prompt is None:
            system_prompt = instruction
            continue
        else:
            system_prompt += instruction

    client = get_anthropic_client()

    tools = []
    async for tool in action["tools"]:
        tools.append(tool)

    new_message = ""
    new_thought = ""

    thoughts_finalised = False

    try:
        while True:
            stream = await client.messages.create(
                max_tokens=config.max_tokens,
                messages=messages,
                model=config.model,
                system=system_prompt or anthropic.Omit(),
                stream=True,
                tool_choice={"type": "auto"},
                tools=tools or anthropic.Omit(),
                thinking=(
                    {"type": "adaptive"} if not tools else anthropic.Omit()
                ),
                output_config={"effort": "medium"},
            )

            tool_calls = []
            tool_names = dict()
            tool_use_ids = dict()
            tool_inputs = dict()

            async for event in stream:
                if event.type == "content_block_start":
                    if event.content_block.type == "tool_use":
                        tool_names[event.index] = event.content_block.name
                        tool_use_ids[event.index] = event.content_block.id
                        tool_inputs[event.index] = ""

                if event.type == "content_block_delta":
                    delta = event.delta

                    if delta.type == "thinking_delta":
                        await action["thoughts"].put(delta.thinking)
                        new_thought += delta.thinking

                    if delta.type == "text_delta":
                        # if not thoughts_finalised:
                        #     await action["thoughts"].finalize()
                        #     thoughts_finalised = True

                        await action["output"].put(delta.text)
                        new_message += delta.text

                    if delta.type == "input_json_delta":
                        tool_inputs[event.index] += delta.partial_json

                if (
                    event.type == "content_block_stop"
                    and event.index in tool_inputs
                ):
                    parsed_tool_input = await asyncio.to_thread(
                        json.loads, tool_inputs[event.index]
                    )
                    tool_calls.append(
                        {
                            "name": tool_names[event.index],
                            "id": tool_use_ids[event.index],
                            "params": parsed_tool_input,
                        }
                    )
                    tool_names.pop(event.index)
                    tool_use_ids.pop(event.index)
                    tool_inputs.pop(event.index)

            if not tool_calls:
                break

            if not action.get_registry().is_registered(TOOL_RUNNER_SCHEMA.name):
                raise RuntimeError(
                    f"Tool runner not registered for action {action['id']}"
                )

            assistant_message = {
                "role": "assistant",
                "content": [],
            }
            if new_message:
                assistant_message["content"].append(
                    {"type": "text", "text": new_message}
                )
            for tool_call in tool_calls:
                assistant_message["content"].append(
                    {
                        "type": "tool_use",
                        "id": tool_call["id"],
                        "name": tool_call["name"],
                        "input": tool_call["params"],
                    }
                )
            messages.append(assistant_message)

            run_tools = (
                action.get_registry()
                .make_action(
                    TOOL_RUNNER_SCHEMA.name,
                    stream=None,
                    node_map=NodeMap(),
                )
                .run()
            )

            for tool_call in tool_calls:
                await run_tools["calls"].put(tool_call)
            await run_tools["calls"].finalize()

            tool_result_message = {
                "role": "user",
                "content": [],
            }

            call_idx = 0
            async for output in run_tools["outputs"]:
                print(
                    f"tool call: \x1b[33;20m{tool_calls[call_idx]['name']}\x1b[0m"
                )
                if tool_calls[call_idx]["name"] == "sqlite_select":
                    print(
                        f"\x1b[32;20m{tool_calls[call_idx]["params"]["query"]}\x1b[0m"
                    )
                    print(f"\x1b[38;5;242m", end="")
                    for row in output:
                        if isinstance(row, (tuple, list)):
                            print(",".join((str(element) for element in row)))
                        else:
                            print(str(row))
                    print("\x1b[0m\n")

                tool_result_message["content"].append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_calls[call_idx]["id"],
                        "content": [
                            {
                                "type": "text",
                                "text": await asyncio.to_thread(
                                    json.dumps, output
                                ),
                            }
                        ],
                    }
                )
                call_idx += 1

            messages.append(tool_result_message)

    except Exception as e:
        await action["output"].put(f"Error generating content: {e}")
        await action["thoughts"].put(f"Error generating content: {e}")
        raise

    finally:
        await action["output"].finalize()
        if not thoughts_finalised:
            await action["thoughts"].finalize()
            thoughts_finalised = True

    # TODO: save tool calls instead
    if new_message:
        interaction_token = await interaction.save_turn(
            interaction_id,
            chat_input,
            new_message,
            new_thought,
            next_output_seq,
            next_thought_seq,
        )
    await action["new_interaction_token"].put_and_finalize(interaction_token)
