// Copyright 2026 The Action Engine Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "actionengine/actions/actions_pybind11.h"

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <pybind11/attr.h>
#include <pybind11/cast.h>
#include <pybind11/eval.h>
#include <pybind11/functional.h>
#include <pybind11/gil.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <pybind11_abseil/absl_casters.h>
#include <pybind11_abseil/import_status_module.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "actionengine/actions/action.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/service/session.h"
#include "actionengine/util/random.h"
#include "actionengine/util/status_macros.h"
#include "actionengine/util/utils_pybind11.h"

namespace act::pybindings {

namespace py = ::pybind11;

struct PyUserData {
  ~PyUserData() {
    py::gil_scoped_acquire gil;

    // no deletion unless fully run or cancelled
    if (action->HasBeenRun()) {
      DCHECK(future.attr("done")().cast<bool>());
    }

    py::object empty1;
    std::swap(future, empty1);
  }

  Action* absl_nonnull action = nullptr;
  thread::PermanentEvent done;
  py::object future = py::none();
};

static std::shared_ptr<PyUserData> EnsurePyUserData(
    Action* absl_nonnull action) {
  py::gil_scoped_acquire gil;
  if (absl::StatusOr<std::shared_ptr<void>> future_ptr =
          action->GetUserData("_py_user_data");
      future_ptr.ok()) {
    return std::static_pointer_cast<PyUserData>(future_ptr.value());
  }
  auto user_data = std::make_shared<PyUserData>();
  user_data->action = action;
  user_data->future = GetGloballySavedEventLoop().attr("create_future")();
  action->SetUserData("_py_user_data", user_data);
  return user_data;
}

absl::StatusOr<ActionHandler> MakeSimpleActionHandler(py::handle py_handler) {
  py_handler = py_handler.inc_ref();

  try {
    const py::object inspect = py::module::import("inspect");
    if (const bool is_coroutine_function =
            inspect.attr("iscoroutinefunction")(py_handler).cast<bool>();
        !is_coroutine_function) {
      return absl::InvalidArgumentError(
          "Handler must be a coroutine function.");
    }
  } catch (const py::error_already_set& e) {
    return absl::InternalError(e.what());
  }

  return [py_handler](const std::shared_ptr<Action>& action) -> absl::Status {
    std::shared_ptr<PyUserData> user_data = EnsurePyUserData(action.get());

    absl::Status status;
    py::object future;

    {
      py::gil_scoped_acquire gil;
      absl::StatusOr<py::object> future_or = RunThreadsafeIfCoroutine(
          py_handler(action), GetGloballySavedEventLoop(),
          /*return_future=*/true);
      if (!future_or.ok()) {
        status = future_or.status();
      } else {
        future = std::move(*future_or);
      }
      user_data->future = future;
    }

    if (!status.ok()) {
      return status;
    }

    // If an action is cancelled from C++ side,
    action->SetOnCancelled([user_data]() {
      py::gil_scoped_acquire gil;
      auto _ = user_data->future.attr("cancel")();
    });

    {
      py::gil_scoped_acquire gil;
      // future.attr("add_done_callback")(
      //     py::cpp_function([action](py::handle future) {
      //       py::gil_scoped_acquire gil;
      //
      //       if (const bool cancelled = future.attr("cancelled")().cast<bool>();
      //           !cancelled) {
      //         return;
      //       }
      //
      //       if (action->HasBeenCancelled()) {
      //         return;
      //       }
      //       {
      //         py::gil_scoped_release release;
      //         action->Cancel().IgnoreError();
      //       }
      //     }));

      future.attr("add_done_callback")(
          py::handle(py::cpp_function([user_data](py::handle) {
            py::gil_scoped_acquire gil;
            if (!user_data->done.HasBeenNotified()) {
              user_data->done.Notify();
            }
          })));
    }

    thread::Select({user_data->done.OnEvent()});

    {
      py::gil_scoped_acquire gil;
      try {
        auto _ = future.attr("result")();
        status = absl::OkStatus();
      } catch (py::error_already_set& e) {
        status = absl::InternalError(absl::StrCat(e.what()));
      }
      future = py::object();
    }

    return status;
  };
}

void BindActionSchemaPort(py::handle scope, std::string_view name) {
  py::bind_map<absl::flat_hash_map<std::string, ActionSchemaPort>>(
      scope, "ActionSchemaPortMap");
  py::class_<ActionSchemaPort>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::string_view name, std::string_view mimetype,
                       std::string_view description = "",
                       bool required = false) {
             return ActionSchemaPort{std::string(name), std::string(mimetype),
                                     std::string(description), required};
           }),
           py::arg("name"), py::arg("mimetype"), py::arg_v("description", ""),
           py::arg_v("required", false))
      .def_readwrite("name", &ActionSchemaPort::name)
      .def_readwrite("type", &ActionSchemaPort::type)
      .def_readwrite("description", &ActionSchemaPort::description)
      .def_readwrite("extra_type_info", &ActionSchemaPort::extra_type_info)
      .def_readwrite("required", &ActionSchemaPort::required)
      // .def("__repr__",
      //      [](const ActionSchemaPort& def) { return absl::StrCat(def); })
      .doc() = "A port of an action schema.";
}

void BindActionSchema(py::handle scope, std::string_view name) {
  py::class_<ActionSchema>(scope, std::string(name).c_str())
      .def(py::init<>())
      // .def(MakeSameObjectRefConstructor<ActionSchema>())
      .def(py::init([](std::string_view action_name,
                       const std::vector<NameAndMimetype>& inputs,
                       const std::vector<NameAndMimetype>& outputs,
                       std::string_view description = "") {
             absl::flat_hash_map<std::string, ActionSchemaPort> input_ports;
             input_ports.reserve(inputs.size());
             for (auto& [input_name, mimetype] : inputs) {
               input_ports[input_name] = ActionSchemaPort{input_name, mimetype};
             }
             absl::flat_hash_map<std::string, ActionSchemaPort> output_ports;
             output_ports.reserve(outputs.size());
             for (auto& [output_name, mimetype] : outputs) {
               output_ports[output_name] =
                   ActionSchemaPort{output_name, mimetype};
             }
             return ActionSchema(
                 std::string(action_name), std::move(input_ports),
                 std::move(output_ports), std::string(description));
           }),
           py::kw_only(), py::arg("name"),
           py::arg_v("inputs", std::vector<NameAndMimetype>()),
           py::arg_v("outputs", std::vector<NameAndMimetype>()),
           py::arg_v("description", ""))
      .def(py::init([](std::string_view action_name,
                       const std::vector<ActionSchemaPort>& inputs,
                       const std::vector<ActionSchemaPort>& outputs,
                       std::string_view description = "") {
             absl::flat_hash_map<std::string, ActionSchemaPort> input_ports;
             input_ports.reserve(inputs.size());
             for (const auto& port : inputs) {
               input_ports[port.name] = port;
             }
             absl::flat_hash_map<std::string, ActionSchemaPort> output_ports;
             output_ports.reserve(outputs.size());
             for (const auto& port : outputs) {
               output_ports[port.name] = port;
             }
             return ActionSchema(
                 std::string(action_name), std::move(input_ports),
                 std::move(output_ports), std::string(description));
           }),
           py::kw_only(), py::arg("name"), py::arg_v("inputs", py::none()),
           py::arg_v("outputs", py::none()), py::arg_v("description", ""))
      .def_readwrite("name", &ActionSchema::name)
      .def(
          "input",
          [](ActionSchema& self, std::string_view name)
              -> absl::StatusOr<std::reference_wrapper<ActionSchemaPort>> {
            const auto it = self.inputs.find(name);
            if (it == self.inputs.end()) {
              return absl::NotFoundError(
                  absl::StrCat("Input port '", name, "' not found."));
            }
            return std::ref(it->second);
          },
          py::return_value_policy::reference_internal)
      .def("inputs",
           [](const ActionSchema& self) -> py::list {
             py::list list;
             for (const auto& [key, value] : self.inputs) {
               list.append(py::str(key));
             }
             return list;
           })
      .def(
          "output",
          [](ActionSchema& self, std::string_view name)
              -> absl::StatusOr<std::reference_wrapper<ActionSchemaPort>> {
            const auto it = self.outputs.find(name);
            if (it == self.outputs.end()) {
              return absl::NotFoundError(
                  absl::StrCat("Output port '", name, "' not found."));
            }
            return std::ref(it->second);
          },
          py::return_value_policy::reference_internal)
      .def("outputs",
           [](const ActionSchema& self) -> py::list {
             py::list list;
             for (const auto& [key, value] : self.outputs) {
               list.append(py::str(key));
             }
             return list;
           })
      .def_readwrite("description", &ActionSchema::description)
      .def("__repr__",
           [](const ActionSchema& def) { return absl::StrCat(def); })
      .doc() = "An action schema.";
}

void BindActionRegistry(py::handle scope, std::string_view name) {
  py::classh<ActionRegistry>(scope, std::string(name).c_str())
      .def(py::init([]() { return std::make_shared<ActionRegistry>(); }))
      .def(MakeSameObjectRefConstructor<ActionRegistry>())
      .def(
          "register",
          [](const std::shared_ptr<ActionRegistry>& self, std::string_view name,
             const ActionSchema& def, py::function handler) -> absl::Status {
            ASSIGN_OR_RETURN(ActionHandler cpp_handler,
                             MakeSimpleActionHandler(handler));
            self->Register(name, def, std::move(cpp_handler));
            return absl::OkStatus();
          },
          py::arg("name"), py::arg("definition"), py::arg("handler"))
      .def(
          "make_action_message",
          [](const std::shared_ptr<ActionRegistry>& self, std::string_view name,
             std::string_view action_id) -> absl::StatusOr<ActionMessage> {
            return self->MakeActionMessage(name, action_id);
          },
          py::arg("name"), py::arg("action_id"),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "make_action",
          [](const std::shared_ptr<ActionRegistry>& self, std::string_view name,
             std::string_view id, const std::shared_ptr<NodeMap>& node_map,
             const std::shared_ptr<WireStream>& stream,
             const std::shared_ptr<Session>& session)
              -> absl::StatusOr<std::shared_ptr<Action>> {
            ASSIGN_OR_RETURN(std::unique_ptr<Action> action,
                             self->MakeAction(name, id));
            action->mutable_bound_resources()->set_node_map(node_map);
            action->mutable_bound_resources()->set_stream(stream);
            action->mutable_bound_resources()->set_session(session);
            return std::shared_ptr(std::move(action));
          },
          py::arg("name"), py::arg_v("action_id", ""),
          py::arg_v("node_map", nullptr), py::arg_v("stream", nullptr),
          py::arg_v("session", nullptr), py::keep_alive<0, 4>(),
          py::keep_alive<0, 5>(), py::keep_alive<0, 6>(),
          pybindings::keep_event_loop_memo())
      .def(
          "is_registered",
          [](const std::shared_ptr<ActionRegistry>& self,
             std::string_view action_name) {
            return self->IsRegistered(action_name);
          },
          py::arg("name"))
      .def(
          "get_schema",
          [](const std::shared_ptr<ActionRegistry>& self,
             std::string_view action_name) -> absl::StatusOr<ActionSchema> {
            ASSIGN_OR_RETURN(const ActionSchema& schema,
                             self->GetSchema(action_name));
            return schema;
          },
          py::arg("name"), py::return_value_policy::copy)
      .def("list_registered_actions",
           [](const std::shared_ptr<ActionRegistry>& self) {
             return self->ListRegisteredActions();
           })
      .doc() = "Registry for action schemas and handlers.";
}

void BindAction(py::handle scope, std::string_view name) {
  auto cls =
      py::classh<Action>(scope, std::string(name).c_str(),
                         py::release_gil_before_calling_cpp_dtor())
          .def(py::init(
                   [](const std::shared_ptr<Action>& other) { return other; }),
               py::keep_alive<0, 1>())
          .def(py::init([](ActionSchema schema, std::string_view id = "") {
                 auto action = std::make_shared<Action>(
                     !id.empty() ? id : GenerateUUID4());
                 action->set_schema(std::move(schema));
                 return action;
               }),
               py::arg(), py::arg_v("id", ""));
  cls.def(
         "run",
         [](const std::shared_ptr<Action>& action,
            double timeout = -1.0) -> absl::StatusOr<std::shared_ptr<Action>> {
           absl::Duration timeout_duration = absl::InfiniteDuration();
           if (timeout >= 0.0) {
             timeout_duration = absl::Seconds(timeout);
           }
           RETURN_IF_ERROR(action->Run(timeout_duration));
           return action;
         },
         pybindings::keep_event_loop_memo(),
         py::call_guard<py::gil_scoped_release>())
      .def(
          "run_in_background",
          [](const std::shared_ptr<Action>& action)
              -> absl::StatusOr<std::shared_ptr<Action>> {
            RETURN_IF_ERROR(action->RunInBackground(/*detach=*/true));
            return action;
          },
          pybindings::keep_event_loop_memo(),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "call",
          [](const std::shared_ptr<Action>& action, py::handle headers_obj) {
            absl::flat_hash_map<std::string, std::string> headers;
            if (!headers_obj.is_none()) {
              try {
                headers =
                    py::cast<absl::flat_hash_map<std::string, std::string>>(
                        headers_obj);
              } catch (const py::cast_error& e) {
                return absl::InvalidArgumentError(
                    absl::StrCat("Failed to cast headers to "
                                 "Dict[str, str]: ",
                                 e.what()));
              }
            }
            {
              py::gil_scoped_release release;
              return action->Call(std::move(headers));
            }
          },
          py::arg("headers") = py::none())
      .def(
          "call_and_wait_for_dispatch_status",
          [](const std::shared_ptr<Action>& action,
             py::handle headers_obj) -> absl::StatusOr<absl::Status> {
            return absl::UnimplementedError("not implemented.");
            absl::flat_hash_map<std::string, std::string> headers;
            if (!headers_obj.is_none()) {
              try {
                headers =
                    py::cast<absl::flat_hash_map<std::string, std::string>>(
                        headers_obj);
              } catch (const py::cast_error& e) {
                return absl::InvalidArgumentError(
                    absl::StrCat("Failed to cast headers to "
                                 "Dict[str, str]: ",
                                 e.what()));
              }
            }
            absl::StatusOr<absl::Status> dispatch_status_or;
            {
              py::gil_scoped_release release;
              // dispatch_status_or =
              //     action->CallAndWaitForDispatchStatus(std::move(headers));
            }
            if (!dispatch_status_or.ok()) {
              return dispatch_status_or.status();
            }
            return *dispatch_status_or;
          },
          py::arg("headers") = py::none())
      .def(
          "wait_until_complete",
          [](const std::shared_ptr<Action>& action, double timeout = -1.0) {
            absl::Duration timeout_duration = absl::InfiniteDuration();
            if (timeout >= 0.0) {
              timeout_duration = absl::Seconds(timeout);
            }
            bool timeout_occurred = false;
            return action->Await(timeout_duration, &timeout_occurred);
          },
          py::call_guard<py::gil_scoped_release>(), py::arg_v("timeout", -1.0))
      .def(
          "clear_inputs_after_run",
          [](const std::shared_ptr<Action>& self, bool clear) {
            self->mutable_settings()->clear_inputs_after_run = clear;
          },
          py::arg_v("clear", true))
      .def(
          "clear_outputs_after_run",
          [](const std::shared_ptr<Action>& self, bool clear) {
            self->mutable_settings()->clear_outputs_after_run = clear;
          },
          py::arg_v("clear", true))
      .def(
          "cancel",
          [](const std::shared_ptr<Action>& self) {
            const auto user_data = EnsurePyUserData(self.get());
            auto _ = user_data->future.attr("cancel")();
            return self->Cancel();
          },
          py::call_guard<py::gil_scoped_release>())
      .def("cancelled", &Action::HasBeenCancelled,
           py::call_guard<py::gil_scoped_release>())
      .def(
          "get_action_message",
          [](const std::shared_ptr<Action>& action)
              -> absl::StatusOr<ActionMessage> {
            return action->GetActionMessage();
          },
          py::call_guard<py::gil_scoped_release>())
      .def("get_registry",
           [](const std::shared_ptr<Action>& action) {
             return action->bound_resources().borrow_registry();
           })
      .def("get_session",
           [](const std::shared_ptr<Action>& action) {
             return action->bound_resources().borrow_session();
           })
      .def("get_node_map",
           [](const std::shared_ptr<Action>& action) {
             return action->bound_resources().borrow_node_map();
           })
      .def("get_stream",
           [](const std::shared_ptr<Action>& action) {
             return action->bound_resources().borrow_stream();
           })
      .def("get_id", &Action::id)
      .def(
          "get_schema",
          [](const std::shared_ptr<Action>& action) -> ActionSchema {
            return action->schema();
          },
          py::return_value_policy::copy)
      .def(
          "get_node",
          [](const std::shared_ptr<Action>& action, std::string_view id) {
            return ShareWithNoDeleter<AsyncNode>(nullptr);
          },
          py::arg("node_id"), py::call_guard<py::gil_scoped_release>())
      .def(
          "get_input",
          [](const std::shared_ptr<Action>& action, std::string_view id,
             const std::optional<bool>& bind_stream) {
            return ShareWithNoDeleter(action->GetInput(id, bind_stream));
          },
          py::arg("name"), py::arg_v("bind_stream", py::none()),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get_output",
          [](const std::shared_ptr<Action>& action, std::string_view id,
             const std::optional<bool>& bind_stream) {
            return ShareWithNoDeleter(action->GetOutput(id, bind_stream));
          },
          py::arg("name"), py::arg_v("bind_stream", py::none()),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "make_action_in_same_session",
          [](const std::shared_ptr<Action>& action, std::string_view name,
             std::string_view id) -> absl::StatusOr<std::unique_ptr<Action>> {
            return action->MakeActionInSameSession(name, id);
          },
          py::arg("name"), py::arg_v("action_id", ""),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "bind_handler",
          [](const std::shared_ptr<Action>& self,
             py::function handler) -> absl::Status {
            ASSIGN_OR_RETURN(auto cpp_handler,
                             MakeSimpleActionHandler(std::move(handler)));
            self->set_handler(std::move(cpp_handler));
            return absl::OkStatus();
          },
          py::arg("handler"))
      .def(
          "bind_streams_on_inputs_by_default",
          [](const std::shared_ptr<Action>& self, bool bind) {
            self->mutable_settings()->bind_streams_on_inputs_by_default = bind;
          },
          py::arg("bind"))
      .def(
          "bind_streams_on_outputs_by_default",
          [](const std::shared_ptr<Action>& self, bool bind) {
            self->mutable_settings()->bind_streams_on_outputs_by_default = bind;
          },
          py::arg("bind"))
      .def(
          "bind_node_map",
          [](const std::shared_ptr<Action>& self,
             std::shared_ptr<NodeMap> node_map) {
            self->mutable_bound_resources()->set_node_map(std::move(node_map));
          },
          py::arg("node_map"))
      .def("bind_registry",
           [](const std::shared_ptr<Action>& self,
              const std::shared_ptr<ActionRegistry>& registry) {
             self->mutable_bound_resources()->set_registry(registry);
           })
      .def("headers",
           [](const std::shared_ptr<Action>& self) {
             return py::make_key_iterator(self->headers().begin(),
                                          self->headers().end());
           })
      .def(
          "get_header",
          [](const std::shared_ptr<Action>& self, std::string_view key,
             bool decode = false) -> std::optional<py::object> {
            const std::optional<std::string> header = self->get_header(key);
            if (!header) {
              return std::nullopt;
            }

            py::object value = py::bytes(std::string(*header));
            if (decode) {
              value =
                  py::cast<py::str>(value.attr("decode")("utf-8", "strict"));
            }
            return value;
          },
          py::arg("key"), py::arg_v("decode", false))
      .def(
          "set_header",
          [](const std::shared_ptr<Action>& self, py::handle py_key,
             py::handle py_value) -> absl::Status {
            std::string key, value;
            try {
              const auto py_key_str = py::cast<py::str>(py_key);
              key = py_key_str.attr("encode")("utf-8").cast<std::string>();
            } catch (const py::cast_error& e) {
              return absl::InvalidArgumentError(
                  absl::StrCat("Failed to cast header key to str: ", e.what()));
            }

            py::bytes py_value_bytes;
            if (py::isinstance<py::bytes>(py_value)) {
              py_value_bytes = py::cast<py::bytes>(py_value);
            } else if (py::isinstance<py::str>(py_value)) {
              const auto py_value_str = py::cast<py::str>(py_value);
              py_value_bytes = py::bytes(
                  py_value_str.attr("encode")("utf-8").cast<std::string>());
            } else {
              return absl::InvalidArgumentError(
                  "Header value must be either bytes or str.");
            }
            value = std::string(py_value_bytes);

            self->set_header(key, value);
            return absl::OkStatus();
          },
          py::arg("key"), py::arg("value"))
      .def(
          "remove_header",
          [](const std::shared_ptr<Action>& self, std::string_view key) {
            return self->remove_header(key);
          },
          py::arg("key"));
}

py::module_ MakeActionsModule(py::module_ scope, std::string_view module_name) {
  py::module_ actions = scope.def_submodule(std::string(module_name).c_str(),
                                            "ActionEngine Actions interface.");

  BindActionSchemaPort(actions, "ActionSchemaPort");
  BindActionSchema(actions, "ActionSchema");
  BindActionRegistry(actions, "ActionRegistry");
  BindAction(actions, "Action");

  return actions;
}

}  // namespace act::pybindings
