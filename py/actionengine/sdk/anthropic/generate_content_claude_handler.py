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
import logging
import os

import anthropic
from actionengine.actions import Action
from actionengine.logging import get_logger
from actionengine.node_map import NodeMap
from actionengine.sdk.anthropic.client import get_anthropic_client
from actionengine.sdk.anthropic.generate_content_claude import (
    CreateMessageConfig,
)
from actionengine.sdk import interaction
from actionengine.sdk.llm_tool_runner import TOOL_RUNNER_SCHEMA
from actionengine.sdk.rehydrate_interaction import REHYDRATE_INTERACTION_SCHEMA

_LOGGER = get_logger().getChild("sdk.anthropic")


async def generate_content_claude(
    action: Action,
):
    input_timeout = 60.0

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

    rehydrate = Action.from_schema(REHYDRATE_INTERACTION_SCHEMA).bind_handler(
        interaction.rehydrate_interaction
    )
    await rehydrate["interaction_token"].put_and_finalize(interaction_token)
    rehydrate.run()

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
            try:
                thinking = anthropic.Omit()
                if "haiku" not in config.model and not tools:
                    thinking = {"type": "adaptive"}

                output_config = {"effort": "medium"}
                if "haiku" in config.model:
                    output_config = anthropic.Omit()

                stream = await client.messages.create(
                    max_tokens=config.max_tokens,
                    messages=messages,
                    model=config.model,
                    cache_control={"type": "ephemeral", "ttl": "1h"},
                    system=system_prompt or anthropic.Omit(),
                    stream=True,
                    tool_choice=(
                        {"type": "auto"} if tools else anthropic.Omit()
                    ),
                    tools=tools or anthropic.Omit(),
                    thinking=thinking,
                    output_config=output_config,
                )
            except anthropic.APIError as e:
                raise

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
                    parsed_tool_input = {}
                    if tool_inputs[event.index]:
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

            registry = action.get_registry()
            if not registry or not registry.is_registered(
                TOOL_RUNNER_SCHEMA.name
            ):
                raise RuntimeError(
                    f"Tool runner not registered for action {action['id']}"
                )

            assistant_message = {
                "role": "assistant",
                "content": [],
            }
            if new_message and not tool_calls:
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

            if tool_calls:
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
                    log_line = f"tool call: \x1b[33;20m{tool_call['name']} {tool_call['id']}\x1b[0m"
                    if tool_call["name"] == "sqlite_select":
                        log_line += f"\n\x1b[32;20m{tool_call["params"]["query"]}\x1b[0m"
                    elif "params" in tool_call:
                        log_line += (
                            f" \x1b[38;5;242m{tool_call['params']}\x1b[0m"
                        )
                    _LOGGER.debug(log_line)
                    await run_tools["calls"].put(tool_call)
                await run_tools["calls"].finalize()

                tool_result_message = {
                    "role": "user",
                    "content": [],
                }

                call_idx = 0
                async for output in run_tools["outputs"]:
                    _LOGGER.debug(
                        f"\x1b[33;20m{tool_calls[call_idx]['name']} {tool_calls[call_idx]['id']}\x1b[0m"
                    )

                    is_error = isinstance(output, dict) and output.get(
                        "__error__"
                    )

                    tool_output_display = (
                        ""
                        if not is_error
                        else ("ERROR: " + output.get("error", ""))
                    )
                    if not tool_output_display:
                        if tool_calls[call_idx]["name"] == "sqlite_select":
                            for row in output:
                                # if isinstance(row, (tuple, list)):
                                #     tool_output_display += (
                                #         ",".join(
                                #             (str(element) for element in row)
                                #         )
                                #         + "\n"
                                #     )
                                # else:
                                tool_output_display += str(row) + "\n"
                        else:
                            tool_output_display += str(output)
                        if len(tool_output_display) > 2000:
                            tool_output_display = (
                                tool_output_display[:2000] + "... <truncated>"
                            )
                    tool_output_display = (
                        f"\x1b[38;5;242m{tool_output_display}\x1b[0m\n"
                    )
                    _LOGGER.debug(tool_output_display)

                    if is_error:
                        tool_result_content = output.get("error", "")
                    else:
                        tool_result_content = [
                            {
                                "type": "text",
                                "text": await asyncio.to_thread(
                                    json.dumps, output
                                ),
                            }
                        ]

                    tool_result_message_content_item = {
                        "type": "tool_result",
                        "tool_use_id": tool_calls[call_idx]["id"],
                        "content": tool_result_content,
                    }
                    if is_error:
                        tool_result_message_content_item["is_error"] = True

                    tool_result_message["content"].append(
                        tool_result_message_content_item
                    )
                    call_idx += 1

                await run_tools.wait_until_complete()
                messages.append(tool_result_message)

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
        await action["new_interaction_token"].put_and_finalize(
            interaction_token
        )
