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
#include <pybind11_abseil/absl_casters.h>
#include <pybind11_abseil/import_status_module.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "actionengine/actions/action.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/service/session.h"
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

    py::object empty2;
    std::swap(lock, empty2);
    obj_refs.clear();
  }

  Action* absl_nonnull action = nullptr;
  thread::PermanentEvent done;
  py::object future = py::none();
  py::object lock = py::none();
  std::vector<py::object> obj_refs;
};

class PyRaiiLock {
 public:
  static absl::StatusOr<PyRaiiLock> FromExisting(py::handle lock) {
    py::gil_scoped_acquire gil;
    // if (!py::isinstance(lock, py::module::import("threading").attr("Lock"))) {
    //   return absl::InvalidArgumentError("Expected a threading.Lock object.");
    // }
    return PyRaiiLock(lock);
  }

  PyRaiiLock() {
    py::gil_scoped_acquire gil;
    lock_ = py::module::import("threading").attr("Lock")();
    CHECK_OK(lock());
  }

  ~PyRaiiLock() {
    py::gil_scoped_acquire gil;
    unlock().IgnoreError();
    lock_ = py::none();
  }

  absl::Status lock() const {
    py::gil_scoped_acquire gil;
    try {
      auto _ = lock_.attr("acquire")();
    } catch (py::error_already_set& e) {
      return absl::InternalError(absl::StrCat(e.what()));
    }
    return absl::OkStatus();
  }

  absl::Status unlock() const {
    py::gil_scoped_acquire gil;
    try {
      auto _ = lock_.attr("release")();
    } catch (py::error_already_set& e) {
      return absl::InternalError(absl::StrCat(e.what()));
    }
    return absl::OkStatus();
  }

 private:
  explicit PyRaiiLock(py::handle lock_obj) {
    py::gil_scoped_acquire gil;
    lock_ = py::cast<py::object>(lock_obj);
    CHECK_OK(lock());
  }

  py::object lock_;
};

static PyUserData& EnsurePyUserData(Action* absl_nonnull action) {
  py::gil_scoped_acquire gil;
  if (const absl::StatusOr<void*> future_ptr =
          action->GetUserData("_py_user_data");
      future_ptr.ok()) {
    return *static_cast<PyUserData*>(*future_ptr);
  }
  auto user_data = std::make_shared<PyUserData>();
  user_data->action = action;
  user_data->future = GetGloballySavedEventLoop().attr("create_future")();
  user_data->lock = py::module::import("threading").attr("Lock")();

  PyUserData* user_data_ptr = user_data.get();
  action->SetUserData("_py_user_data", std::move(user_data));
  return *user_data_ptr;
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
    PyUserData& user_data = EnsurePyUserData(action.get());
    py::gil_scoped_acquire gil;

    absl::Status status;
    try {
      absl::StatusOr<py::object> future = RunThreadsafeIfCoroutine(
          py_handler(action), GetGloballySavedEventLoop(),
          /*return_future=*/true);

      if (!future.ok()) {
        status = future.status();
      } else {
        std::swap(*future, user_data.future);
        future->release().dec_ref();
        py::handle future_handle = user_data.future;
        action->SetOnCancelled([future_handle]() {
          py::gil_scoped_acquire gil;
          auto _ = future_handle.attr("cancel")();
        });
        auto on_python_cancel =
            py::cpp_function([user_data = &user_data](py::handle) {
              py::gil_scoped_acquire gil;
              auto lock = *PyRaiiLock::FromExisting(user_data->lock);

              if (const bool cancelled =
                      user_data->future.attr("cancelled")().cast<bool>();
                  !cancelled) {
                return;
              }

              if (user_data->action->HasBeenCancelled()) {
                return;
              }
              {
                py::gil_scoped_release release;
                user_data->action->Cancel().IgnoreError();
              }
            });
        user_data.obj_refs.push_back(on_python_cancel);
        user_data.future.attr("add_done_callback")(
            py::handle(on_python_cancel));
        on_python_cancel.release().dec_ref();

        auto on_python_done =
            py::cpp_function([action = action.get()](py::handle) {
              py::gil_scoped_acquire gil;
              if (PyUserData& user_data = EnsurePyUserData(action);
                  !user_data.done.HasBeenNotified()) {
                user_data.done.Notify();
              }
            });
        user_data.obj_refs.push_back(on_python_done);
        user_data.future.attr("add_done_callback")(py::handle(on_python_done));
        on_python_done.release().dec_ref();

        {
          py::gil_scoped_release release;
          thread::Select({user_data.done.OnEvent()});
        }
      }
    } catch (py::error_already_set& e) {
      status = absl::InternalError(absl::StrCat(e.what()));
    }

    RETURN_IF_ERROR(status);

    try {
      auto _ = user_data.future.attr("result")();
      return absl::OkStatus();
    } catch (py::error_already_set& e) {
      return absl::InternalError(absl::StrCat(e.what()));
    }
  };
}

void BindActionSchema(py::handle scope, std::string_view name) {
  py::classh<ActionSchema>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(MakeSameObjectRefConstructor<ActionSchema>())
      .def(py::init([](std::string_view action_name,
                       const std::vector<NameAndMimetype>& inputs,
                       const std::vector<NameAndMimetype>& outputs,
                       std::string_view description = "") {
             NameToMimetype input_map(inputs.begin(), inputs.end());
             NameToMimetype output_map(outputs.begin(), outputs.end());
             return std::make_shared<ActionSchema>(std::string(action_name),
                                                   input_map, output_map,
                                                   std::string(description));
           }),
           py::kw_only(), py::arg("name"),
           py::arg_v("inputs", std::vector<NameAndMimetype>()),
           py::arg_v("outputs", std::vector<NameAndMimetype>()),
           py::arg_v("description", ""))
      .def_readwrite("name", &ActionSchema::name)
      .def_readwrite("inputs", &ActionSchema::inputs)
      .def_readwrite("outputs", &ActionSchema::outputs)
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
             std::string_view id, NodeMap* node_map,
             const std::shared_ptr<WireStream>& stream,
             Session* session) -> absl::StatusOr<std::shared_ptr<Action>> {
            ASSIGN_OR_RETURN(std::unique_ptr<Action> action,
                             self->MakeAction(name, id));
            action->mutable_bound_resources()->set_node_map_non_owning(
                node_map);
            action->mutable_bound_resources()->set_stream(stream);
            action->mutable_bound_resources()->set_session_non_owning(session);
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
             std::string_view action_name)
              -> absl::StatusOr<std::shared_ptr<const ActionSchema>> {
            ASSIGN_OR_RETURN(const ActionSchema& schema,
                             self->GetSchema(action_name));
            return std::shared_ptr<const ActionSchema>(
                &schema, [](const ActionSchema*) {});
          },
          py::arg("name"), py::return_value_policy::reference)
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
            auto action = std::make_shared<Action>(id);
            action->set_schema(std::move(schema));
            return action;
          }));
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
            RETURN_IF_ERROR(action->RunInBackground());
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
          [](const std::shared_ptr<Action>& action) { return action->Await(); },
          py::call_guard<py::gil_scoped_release>())
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
            const PyUserData& user_data = EnsurePyUserData(self.get());
            auto _ = user_data.future.attr("cancel")();
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
      .def("get_schema", &Action::schema, py::return_value_policy::reference)
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

  BindActionSchema(actions, "ActionSchema");
  BindActionRegistry(actions, "ActionRegistry");
  BindAction(actions, "Action");

  return actions;
}

}  // namespace act::pybindings
