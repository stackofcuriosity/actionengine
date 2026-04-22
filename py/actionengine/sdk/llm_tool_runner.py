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

from actionengine import actions
from actionengine import async_node
from actionengine import data
from actionengine.logging import get_logger
from actionengine.sdk import llm_tool

_LOGGER = get_logger()

TOOL_RUNNER_SCHEMA = actions.ActionSchema(
    name="__handle_tool_calls",
    description="Runs tools from input_dicts.",
    inputs=[
        (
            "calls",
            "application/json",
            "Tool calls in the form of LLM-supplied dictionaries.",
        ),
    ],
    outputs=[
        (
            "outputs",
            "application/json",
            "The results of the tool calls.",
        ),
    ],
)


async def _run_tool(
    tools: dict[str, llm_tool.LLMTool],
    input_dict_chunk: data.Chunk,
    output_node: async_node.AsyncNode,
    result_idx: int,
):
    try:
        input_dict = await asyncio.to_thread(
            input_dict_chunk.deserialize, "application/json"
        )
        if not isinstance(input_dict, dict):
            raise ValueError("Input dict chunk did not contain a valid JSON.")

        tool = tools[input_dict["name"]]
        _LOGGER.info(f"{input_dict["id"]} {input_dict['name']}")

        result = await tool.run(input_dict["params"])
        if result is None:
            raise ValueError(f"Tool {input_dict['name']} returned None.")
    except Exception as exc:
        await output_node.put(
            {
                "__error__": True,
                "error": str(exc.with_traceback(None)).splitlines()[0],
            },
            seq=result_idx,
            mimetype="application/json",
        )
    else:
        await output_node.put(
            result,
            seq=result_idx,
            mimetype="application/json",
        )


def make_llm_tool_runner(tools: dict[str, llm_tool.LLMTool]):
    # TODO: this tool runner should later use the action registry to determine
    #       handlers, while llm_tool.LLMTool should become just a
    #       "stream/unstream" helper

    async def _runner(action: actions.Action):
        try:
            tasks = []

            # start all tools as soon as possible, but preserve call order
            tool_call_idx = 0
            while True:
                chunk: data.Chunk | None = await action["calls"].next_chunk()
                if chunk is None:
                    break
                tasks.append(
                    asyncio.create_task(
                        _run_tool(
                            tools, chunk, action["outputs"], tool_call_idx
                        )
                    )
                )
                if tool_call_idx == 0:
                    await asyncio.sleep(0.05)
                tool_call_idx += 1

            await asyncio.gather(*tasks)
        finally:
            await action["outputs"].finalize()

    return _runner


def enable_llm_tool_runner(
    registry: actions.ActionRegistry,
    tools: dict[str, llm_tool.LLMTool],
):
    registry.register(
        TOOL_RUNNER_SCHEMA.name, TOOL_RUNNER_SCHEMA, make_llm_tool_runner(tools)
    )
