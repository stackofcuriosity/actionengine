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

"""A Pythonic wrapper for the raw pybind11 Actions bindings."""

import asyncio
import inspect
from typing import Any
from typing import Awaitable
from typing import Callable

from actionengine import _C
from actionengine import async_node
from actionengine import node_map as eg_node_map
from actionengine import data
from actionengine import utils
from pydantic import BaseModel

AsyncNode = async_node.AsyncNode
NodeMap = eg_node_map.NodeMap

AsyncActionHandler = Callable[["Action"], Awaitable[None]]
SyncActionHandler = Callable[["Action"], None]
ActionHandler = SyncActionHandler | AsyncActionHandler


async def do_nothing(_: "Action") -> None:
    pass


def wrap_handler(handler: ActionHandler) -> ActionHandler:
    is_coroutine_fn = inspect.iscoroutinefunction(handler)

    if not is_coroutine_fn:

        def inner(action: "Action") -> None:
            """Inner function to wrap the handler."""
            return handler(utils.wrap_pybind_object(Action, action))

        return inner
    else:

        async def inner(action: "Action") -> None:
            """Inner function to wrap the async handler."""
            py_action = utils.wrap_pybind_object(Action, action)
            await handler(py_action)

        return inner


class ActionSchemaPort(_C.actions.ActionSchemaPort):
    def __init__(
        self,
        name: str,
        port_type: str | type[BaseModel],
        description: str = "",
        required: bool = False,
    ):
        """Constructor for ActionSchemaPort.

        Args:
          name: The name of the action port.
          port_type: The type of the action port.
          description: The description of the action port.
          required: Whether the action port is required.
        """
        self._py_type = port_type
        if isinstance(port_type, type) and issubclass(port_type, BaseModel):
            self._py_type = port_type
            port_type = "__BaseModel__"

        super().__init__(name, port_type, description, required)


class ActionSchema(_C.actions.ActionSchema):
    """A schema of an ActionEngine Action."""

    # pylint: disable-next=[useless-parent-delegation]
    def __init__(
        self,
        *,
        name: str = "",
        inputs: list[tuple[str, str | type[BaseModel]]],
        outputs: list[tuple[str, str | type[BaseModel]]],
        description: str = "",
    ):
        """Constructor for ActionSchema.

        Args:
          name: The name of the action definition.
          inputs: The inputs of the action definition.
          outputs: The outputs of the action definition.
        """
        self._py_inputs = dict()
        self._py_outputs = dict()

        input_ports = []
        output_ports = []

        for input_port_or_tuple in inputs:
            if isinstance(input_port_or_tuple, tuple):
                input_name, input_type = input_port_or_tuple
                self._py_inputs[input_name] = input_type
                input_port = ActionSchemaPort(input_name, input_type)
            else:
                input_port: ActionSchemaPort = input_port_or_tuple
                self._py_inputs[input_port.name] = input_port._py_type
            input_ports.append(input_port)

        for output_port_or_tuple in outputs:
            if isinstance(output_port_or_tuple, tuple):
                output_name, output_type = output_port_or_tuple
                self._py_outputs[output_name] = output_type
                output_port = ActionSchemaPort(output_name, output_type)
            else:
                output_port: ActionSchemaPort = output_port_or_tuple
                self._py_outputs[output_port.name] = output_port._py_type
            output_ports.append(output_port)

        super().__init__(
            name=name,
            inputs=input_ports,
            outputs=output_ports,
            description=description,
        )

    def get_python_type_for_port(self, name: str):
        if name in self._py_inputs:
            return self._py_inputs[name]

        if name in self._py_outputs:
            return self._py_outputs[name]

        raise ValueError(f"Unknown port name: {name}")


class ActionRegistry(_C.actions.ActionRegistry):
    """A Pythonic wrapper for the raw pybind11 ActionRegistry bindings."""

    def __init__(self):
        self._add_python_specific_attributes()
        super().__init__()

    def _add_python_specific_attributes(self):
        """Adds Python-specific attributes to the action."""
        if not hasattr(self, "_raw_handlers"):
            self._raw_handlers = dict()

    def register(
        self,
        name: str,
        schema: _C.actions.ActionSchema,
        handler: Any = do_nothing,
    ) -> None:
        """Registers an action schema and handler."""

        if not schema.name:
            schema.name = name

        self._raw_handlers[name] = handler
        super().register(name, schema, wrap_handler(handler))

    # pylint: disable-next=[useless-parent-delegation]
    def make_action_message(
        self, name: str, action_id: str
    ) -> data.ActionMessage:
        """Creates an action message.

        Args:
          name: The name of the action. Must be registered by the time of the call.
          action_id: The id of the action.

        Returns:
          The action message.
        """
        return super().make_action_message(name, action_id)

    def get_handler(self, name: str):
        return self._raw_handlers.get(name, None)

    def make_action(
        self,
        name: str,
        action_id: str = "",
        *,
        node_map: NodeMap | None = None,
        stream: _C.service.WireStream | None = None,
        session: _C.service.Session | None = None,
    ) -> "Action":
        """Creates an action."""

        action = utils.wrap_pybind_object(
            Action,
            super().make_action(
                name,
                action_id,
                node_map,
                stream,
                session,
            ),
        )

        action._node_map = node_map
        action._stream = stream
        action._session = session

        return action


class Action(_C.actions.Action):
    """A Pythonic wrapper for the raw pybind11 Action bindings."""

    @staticmethod
    def from_schema(
        schema: ActionSchema,
        action_id: str = "",
    ):
        """Creates an action from a schema."""
        action = utils.wrap_pybind_object(
            Action,
            _C.actions.Action(schema, action_id),
        )
        action.bind_node_map(NodeMap())
        return action

    def _add_python_specific_attributes(self):
        """Adds Python-specific attributes to the action."""
        if not hasattr(self, "_schema"):
            self._schema = self.get_schema()

    async def wait_until_complete(self, timeout: float = -1.0):
        """Waits for the action to complete with an optional timeout."""
        return await asyncio.to_thread(super().wait_until_complete, timeout)

    async def call(
        self, wire_message_headers: dict[str, bytes] | None = None
    ) -> None:
        """Calls the action by sending the action message to the stream."""
        await asyncio.to_thread(self.call_sync, wire_message_headers)

    async def call_and_wait_for_dispatch_status(
        self, wire_message_headers: dict[str, bytes] | None = None
    ) -> None:
        """Calls the action and waits for the dispatch status."""
        await asyncio.to_thread(
            self.call_and_wait_for_dispatch_status_sync,
            wire_message_headers,
        )

    def call_sync(
        self,
        wire_message_headers: dict[str, bytes] | None = None,
    ) -> None:
        """Calls the action by sending the action message to the stream."""
        super().call(wire_message_headers)

    def call_and_wait_for_dispatch_status_sync(
        self,
        wire_message_headers: dict[str, bytes] | None = None,
    ) -> None:
        """Calls the action and waits for the dispatch status synchronously."""
        super().call_and_wait_for_dispatch_status(wire_message_headers)

    def get_registry(self) -> ActionRegistry | None:
        """Returns the action registry from attached session."""
        registry = super().get_registry()
        if registry is None:
            return None
        return utils.wrap_pybind_object(ActionRegistry, registry)

    # pylint: disable-next=[useless-parent-delegation]
    def get_stream(self) -> _C.service.WireStream:
        """Returns attached stream."""
        return super().get_stream()

    def get_node_map(self) -> NodeMap:
        """Returns the NodeMap of the action."""
        if hasattr(self, "_node_map") and self._node_map is not None:
            return self._node_map
        return utils.wrap_pybind_object(NodeMap, super().get_node_map())

    def get_input(
        self, name: str, bind_stream: bool | None = None
    ) -> AsyncNode:
        """Returns the input node with the given name.

        Args:
          name: The name of the input node.
          bind_stream: Whether to bind the stream to the input node. Binding the
            stream to the input node means that in addition to writing chunks to the
            ChunkStore, the input node will also write them to the stream. If None,
            the default behavior is used, which is to bind streams to input nodes if
            the action is called (client-side), and to output nodes if the action is
            run (server-side).
        """
        return utils.wrap_pybind_object(
            AsyncNode,
            super().get_input(name, bind_stream),
        )

    def get_output(
        self, name: str, bind_stream: bool | None = None
    ) -> AsyncNode:
        """Returns the output node with the given name.

        Args:
          name: The name of the output node.
          bind_stream: Whether to bind the stream to the output node. Binding the
            stream to the output node means that in addition to writing chunks to
            the ChunkStore, the output node will also write them to the stream. If
            None, the default behavior is used, which is to bind streams to output
            nodes if the action is run (server-side), and to input nodes if the
            action is called (client-side).
        """
        return utils.wrap_pybind_object(
            AsyncNode,
            super().get_output(name, bind_stream),
        )

    def __getitem__(self, name: str) -> AsyncNode:
        """Returns the node with the given name."""
        node = None

        schema = self.get_schema()
        for param in schema.inputs:
            if param == name:
                node = self.get_input(name)
                break
        for param in schema.outputs:
            if param == name:
                node = self.get_output(name)
                break

        if node is None:
            raise KeyError(f"Node with name {name} not found.")

        return utils.wrap_pybind_object(AsyncNode, node)

    def make_action_in_same_session(
        self, name: str, action_id: str = ""
    ) -> "Action":
        """Creates an action in the same session."""
        return utils.wrap_pybind_object(
            Action,
            super().make_action_in_same_session(name),
        )

    def run(self) -> "Action":
        """Runs the action."""
        # _C.save_first_encountered_event_loop()
        super().run_in_background()
        return self

    def bind_handler(self, handler: ActionHandler) -> "Action":
        """Binds a handler to the action."""
        super().bind_handler(wrap_handler(handler))
        return self

    def bind_node_map(self, node_map: NodeMap) -> "Action":
        """Binds a NodeMap to the action."""
        super().bind_node_map(node_map)
        self._node_map = node_map
        return self

    def bind_registry(self, registry: ActionRegistry) -> "Action":
        """Binds an ActionRegistry to the action."""
        super().bind_registry(registry)
        self._registry = registry
        return self
