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

import time
from collections import defaultdict
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

import actionengine.data  # noqa
from actionengine.actions import Action, ActionSchema, ActionHandler
from actionengine.async_node import AsyncNode


class LLMToolInputProperty(BaseModel):
    type: str = Field(description="The type of the property.")
    description: str = Field(
        default="", description="A description of the property."
    )
    enum: list[str] | None = Field(
        default=None,
        description="A list of allowed values for the property.",
        exclude_if=lambda x: x is None,
    )
    properties: dict[str, "LLMToolInputProperty"] | None = Field(
        default=None,
        description="A dictionary of nested properties.",
        exclude_if=lambda x: x is None,
    )


class LLMToolInputSchema(BaseModel):
    type: Literal["object"] = Field(
        default="object", description="The type of the input schema."
    )
    properties: dict[str, LLMToolInputProperty] = Field(
        description="A dictionary of properties and their types.",
        default_factory=dict,
    )


def _ae_type_to_json_schema_type(ae_type: str | type) -> str:
    if ae_type is dict:
        return "object"

    if ae_type is list or ae_type is tuple:
        return "array"

    if ae_type is str:
        return "string"

    if ae_type is int:
        return "integer"

    if ae_type is float:
        return "number"

    if ae_type is bool:
        return "boolean"

    if ae_type is None:
        return "null"

    if isinstance(ae_type, type) and issubclass(ae_type, BaseModel):
        return "object"

    if not isinstance(ae_type, str):
        return "object"

    if ae_type.startswith("text/"):
        return "string"
    if ae_type.startswith("image/"):
        return "string"
    if ae_type.startswith("application/json"):
        return "object"

    return "object"


class LLMToolSchema(BaseModel):
    @staticmethod
    def from_action_schema(action_schema: ActionSchema):
        name = action_schema.name
        description = action_schema.description
        input_schema = LLMToolInputSchema()
        required = []

        for input_name in action_schema.inputs():
            python_type = action_schema.get_python_type_for_port(input_name)
            json_schema_type = _ae_type_to_json_schema_type(python_type)

            nested_props = None
            if isinstance(python_type, type) and issubclass(
                python_type, BaseModel
            ):
                nested_props = python_type.model_json_schema()["properties"]
                nested_props_with_descriptions = dict()
                for prop_name, prop in nested_props.items():
                    prop_with_desc = {"type": prop["type"]}
                    prop_with_desc["description"] = python_type.model_fields[
                        prop_name
                    ].description
                    nested_props_with_descriptions[prop_name] = prop_with_desc

            input_schema.properties[input_name] = LLMToolInputProperty(
                type=json_schema_type,
                description=action_schema.input(input_name).description,
                properties=nested_props,
            )

        return LLMToolSchema(
            name=name,
            description=description,
            input_schema=input_schema,
            required=required,
        )

    name: str = Field(description="The name of the tool.")
    description: str = Field(description="A description of the tool.")
    eager_input_streaming: bool = Field(
        default=True,
        description="Whether the tool supports eager input streaming.",
    )
    input_schema: LLMToolInputSchema = Field(
        description="The input schema for the tool."
    )
    required: list[str] = Field(
        default_factory=list,
        description="A list of required fields in the input schema.",
        exclude_if=lambda x: not x,
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)


class LLMTool:
    _action_schema: ActionSchema
    _tool_schema: LLMToolSchema
    _autofills: dict[str, Any]
    _output_to_field: dict[str, str]
    _handler: ActionHandler
    _excluded_inputs: set[str]

    # TODO: input exclusion logic and autofilling should be refactored
    #       into (C++) Action to enable that functionality when using native
    #       Actions.

    def __init__(
        self,
        schema: ActionSchema,
        handler: ActionHandler,
        excluded_inputs: list[str] | None = None,
    ):
        self._action_schema = schema
        self._tool_schema = LLMToolSchema.from_action_schema(schema)
        self._autofills = dict()
        self._output_to_field = dict()
        self._handler = handler

        self._excluded_inputs = set()
        excluded_inputs = excluded_inputs or []
        for name in excluded_inputs:
            if name not in self._action_schema.inputs():
                raise ValueError(f"Input {name} not found in action schema.")
            self._excluded_inputs.add(name)

    def get_schema(self) -> LLMToolSchema:
        schema = self._tool_schema.model_copy(deep=True)
        for input_name in self._excluded_inputs:
            schema.input_schema.properties.pop(input_name)
        return schema

    def map_output_to_field(self, output_name: str, field_name: str):
        if output_name not in self._action_schema.outputs():
            raise ValueError(
                f"Output {output_name} not found in action schema."
            )
        self._output_to_field[output_name] = field_name

    def _reduce_chunked_output(
        self, output_name: str, chunked_output: list[Any]
    ):
        if output_name not in self._action_schema.outputs():
            raise ValueError(
                f"Output {output_name} not found in action schema."
            )

        if len(chunked_output) == 0:
            return []

        if len(chunked_output) == 1:
            return chunked_output[0]

        output_type = self._action_schema.get_python_type_for_port(output_name)
        if (
            output_type is str
            or isinstance(output_type, str)
            and output_type.startswith("text/")
        ):
            return "".join(chunked_output)
        if (
            output_type is bytes
            or isinstance(output_type, str)
            and output_type.startswith("application/octet-stream")
        ):
            return b"".join(chunked_output)
        return chunked_output

    @staticmethod
    async def _read_output(
        node: AsyncNode,
        destination: list[Any],
        timeout: float = -1.0,
    ):
        node.set_reader_options(timeout=timeout)
        async for piece in node:
            destination.append(piece)

    async def run(
        self,
        input_dict: dict[str, Any] | None = None,
        timeout: float = -1.0,
    ):
        input_dict = input_dict or dict()
        try:
            return await self._run(input_dict, timeout)
        finally:
            pass

    def clear_autofills(self):
        self._autofills = dict()

    async def _run(
        self,
        input_dict: dict[str, Any],
        timeout: float = -1.0,
    ):
        started_at = time.perf_counter()

        if self._excluded_inputs:
            for name in self._excluded_inputs:
                if name not in self._autofills:
                    raise ValueError(
                        f"Input {name} is excluded from exposed tool schema, "
                        f"but not autofilled: this would freeze the tool."
                    )

        # autofills take priority over LLM-supplied input
        input_dict = input_dict | self._autofills
        for name, autofill in self._autofills.items():
            assert (
                input_dict[name] == autofill
            ), f"Input {name} does not match autofill, this might be a security issue."

        required_input_names = list(self._action_schema.inputs())
        if len(input_dict) > len(required_input_names):
            raise ValueError(
                f"Too many input fields. Expected {required_input_names}, got {input_dict.keys()}"
            )

        if len(input_dict) < len(required_input_names):
            raise ValueError(
                f"Too few input fields. Expected {required_input_names}, got {input_dict.keys()}"
            )

        for name, value in input_dict.items():
            if name not in required_input_names:
                raise ValueError(
                    f"Input field {name} not found in action schema."
                )

        if not self._output_to_field:
            outputs = list(self._action_schema.outputs())
            if outputs:
                if len(outputs) == 1:
                    self.map_output_to_field(list(outputs)[0], "$")
                else:
                    for output_name in outputs:
                        self.map_output_to_field(output_name, output_name)

        n_whole_response_maps = 0
        for output_name, field_name in self._output_to_field.items():
            if field_name == "$":
                n_whole_response_maps += 1
                continue
            if n_whole_response_maps > 1:
                raise ValueError(
                    "Multiple output fields mapped to whole response. Only one is allowed."
                )

        action = (
            Action.from_schema(self._action_schema)
            .bind_handler(self._handler)
            .run()
        )
        for name, value in input_dict.items():
            await action[name].put_and_finalize(value)

        output_lists = defaultdict(list)

        timeout_left = (
            -1.0
            if timeout == -1.0
            else timeout - (time.perf_counter() - started_at)
        )
        await action.wait_until_complete(timeout_left)
        for output_name, field_name in self._output_to_field.items():
            timeout_left = (
                -1.0
                if timeout == -1.0
                else timeout - (time.perf_counter() - started_at)
            )
            await LLMTool._read_output(
                action[output_name],
                output_lists[field_name],
                timeout_left,
            )

        output_values = dict()
        for output_name, field_name in self._output_to_field.items():
            if field_name == "$":
                return self._reduce_chunked_output(
                    output_name, output_lists[field_name]
                )
            output_values[field_name] = self._reduce_chunked_output(
                output_name, output_lists[field_name]
            )

        return output_values

    def autofill(self, name: str, value: Any):
        self._autofills[name] = value
