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

#include "actionengine/service/service_pybind11.h"

#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include <Python.h>
#include <absl/log/check.h>
#include <absl/strings/str_cat.h>
#include <pybind11/attr.h>
#include <pybind11/cast.h>
#include <pybind11/detail/common.h>
#include <pybind11/detail/descr.h>
#include <pybind11/detail/internals.h>
#include <pybind11/gil.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11_abseil/absl_casters.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "actionengine/actions/registry.h"
#include "actionengine/net/stream.h"
#include "actionengine/nodes/async_node.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/service/service.h"
#include "actionengine/service/session.h"
#include "actionengine/stores/chunk_store.h"
#include "actionengine/util/utils_pybind11.h"

namespace act::pybindings {

namespace py = ::pybind11;

void BindStream(py::handle scope, std::string_view name) {
  const std::string name_str(name);

  py::classh<net::MergeWireMessagesWhileInScope>(
      scope, "MergeWireMessagesWhileInScope",
      py::release_gil_before_calling_cpp_dtor())
      .def(
          py::init<>([](WireStream* stream) {
            return std::make_shared<net::MergeWireMessagesWhileInScope>(stream);
          }),
          py::arg("stream"), pybindings::keep_event_loop_memo())
      .def("no_more_sends", &net::MergeWireMessagesWhileInScope::NoMoreSends)
      .def("send", &net::MergeWireMessagesWhileInScope::Send,
           py::arg("message"), py::call_guard<py::gil_scoped_release>())
      .def("force_flush", &net::MergeWireMessagesWhileInScope::ForceFlush,
           py::call_guard<py::gil_scoped_release>())
      .def("get_status", &net::MergeWireMessagesWhileInScope::GetStatus)
      .def("finalize", &net::MergeWireMessagesWhileInScope::Finalize,
           py::call_guard<py::gil_scoped_release>())
      .def(
          "buffered_message",
          [](net::MergeWireMessagesWhileInScope& self) -> WireMessage& {
            return *self.buffered_message();
          },
          py::return_value_policy::reference)
      .def(
          "__enter__",
          [](net::MergeWireMessagesWhileInScope* self)
              -> absl::StatusOr<net::MergeWireMessagesWhileInScope*> {
            RETURN_IF_ERROR(self->Attach());
            return self;
          },
          py::return_value_policy::reference)
      .def(
          "__exit__",
          [](net::MergeWireMessagesWhileInScope& self, py::handle, py::handle,
             py::handle) -> absl::Status { return self.Finalize(); },
          py::call_guard<py::gil_scoped_release>());

  py::classh<WireStream>(scope, absl::StrCat(name, "VirtualBase").c_str(),
                         py::release_gil_before_calling_cpp_dtor())
      .def("send", &WireStream::Send, py::call_guard<py::gil_scoped_release>())
      .def(
          "receive",
          [](const std::shared_ptr<WireStream>& self,
             double timeout) -> absl::StatusOr<std::optional<WireMessage>> {
            const absl::Duration timeout_duration =
                timeout < 0 ? absl::InfiniteDuration() : absl::Seconds(timeout);
            return self->Receive(timeout_duration);
          },
          py::arg_v("timeout", -1.0), py::call_guard<py::gil_scoped_release>())
      .def("accept", &WireStream::Accept)
      .def("start", &WireStream::Start)
      .def("half_close", &WireStream::HalfClose)
      .def("abort", &WireStream::Abort,
           py::call_guard<py::gil_scoped_release>())
      .def("get_status", &WireStream::GetStatus)
      .def("get_id", &WireStream::GetId);

  py::classh<PyWireStream, WireStream>(scope, name_str.c_str())
      .def(py::init<>(), pybindings::keep_event_loop_memo())
      .def(MakeSameObjectRefConstructor<PyWireStream>())
      .def("send", &PyWireStream::Send, py::arg("message"),
           py::call_guard<py::gil_scoped_release>())
      .def(
          "receive",
          [](const std::shared_ptr<PyWireStream>& self,
             double timeout) -> absl::StatusOr<std::optional<WireMessage>> {
            const absl::Duration timeout_duration =
                timeout < 0 ? absl::InfiniteDuration() : absl::Seconds(timeout);
            return self->Receive(timeout_duration);
          },
          py::arg_v("timeout", -1.0), py::call_guard<py::gil_scoped_release>())
      .def(
          "accept",
          [](const std::shared_ptr<PyWireStream>& self) {
            return self->Accept();
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "start",
          [](const std::shared_ptr<PyWireStream>& self) {
            return self->Start();
          },
          py::call_guard<py::gil_scoped_release>())
      .def("half_close", &PyWireStream::HalfClose)
      .def("abort", &PyWireStream::Abort,
           py::call_guard<py::gil_scoped_release>())
      .def("get_status", &PyWireStream::GetStatus)
      .def("get_id", &PyWireStream::GetId);
}

void BindSession(py::handle scope, std::string_view name) {
  py::classh<Session>(scope, std::string(name).c_str(),
                      py::release_gil_before_calling_cpp_dtor())
      .def(py::init([](NodeMap* node_map = nullptr,
                       ActionRegistry* action_registry = nullptr) {
             auto session = std::make_shared<Session>();
             session->set_node_map(node_map);
             session->set_action_registry(*action_registry);
             return session;
           }),
           py::arg("node_map"), py::arg_v("action_registry", nullptr))
      .def(MakeSameObjectRefConstructor<Session>())
      .def(
          "get_node",
          [](const std::shared_ptr<Session>& self, const std::string_view id,
             const ChunkStoreFactory& chunk_store_factory = {}) {
            return ShareWithNoDeleter(self->GetNode(id, chunk_store_factory));
          },
          py::arg_v("id", ""), py::arg_v("chunk_store_factory", py::none()),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "dispatch_from",
          [](const std::shared_ptr<Session>& self,
             const std::shared_ptr<WireStream>& stream) {
            self->StartStreamHandler(stream->GetId(), stream);
          },
          py::keep_alive<1, 2>(), py::arg("stream"),
          py::call_guard<py::gil_scoped_release>())
      .def("dispatch_wire_message", &Session::DispatchWireMessage,
           py::call_guard<py::gil_scoped_release>())
      .def(
          "get_node_map",
          [](const std::shared_ptr<Session>& self) {
            return ShareWithNoDeleter(self->node_map());
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get_action_registry",
          [](const std::shared_ptr<Session>& self) {
            return ShareWithNoDeleter(self->action_registry());
          },
          py::call_guard<py::gil_scoped_release>())
      .def("set_action_registry", &Session::set_action_registry,
           py::arg("action_registry"),
           py::call_guard<py::gil_scoped_release>());
}

StreamHandler MakeCppConnectionHandler(py::handle py_connection_handler) {
  py_connection_handler = py_connection_handler.inc_ref();
  if (py_connection_handler.is_none()) {
    return nullptr;
  }

  return [py_connection_handler](std::shared_ptr<WireStream> stream,
                                 Session* absl_nonnull session,
                                 absl::Duration recv_timeout) -> absl::Status {
    py::gil_scoped_acquire acquire;
    const py::object result =
        py_connection_handler(std::move(stream), ShareWithNoDeleter(session),
                              recv_timeout == absl::InfiniteDuration()
                                  ? -1.0
                                  : absl::ToDoubleSeconds(recv_timeout));
    return result.cast<absl::Status>();
  };
}

void BindService(py::handle scope, std::string_view name) {
  py::classh<Service>(scope, std::string(name).c_str(),
                      py::release_gil_before_calling_cpp_dtor())
      .def(py::init([](ActionRegistry* action_registry = nullptr,
                       py::handle py_connection_handler = py::none()) {
             return std::make_shared<Service>(
                 action_registry,
                 py_connection_handler.is_none()
                     ? internal::DefaultStreamHandler
                     : MakeCppConnectionHandler(py_connection_handler));
           }),
           py::arg("action_registry"),
           py::arg_v("connection_handler", py::none()))
      .def(
          "get_stream",
          [](const std::shared_ptr<Service>& self,
             const std::string& stream_id) {
            return ShareWithNoDeleter(self->GetStream(stream_id));
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get_session",
          [](const std::shared_ptr<Service>& self,
             const std::string& session_id) {
            return ShareWithNoDeleter(self->GetSession(session_id));
          },
          py::call_guard<py::gil_scoped_release>())
      .def("get_session_keys", &Service::GetSessionKeys,
           py::call_guard<py::gil_scoped_release>())
      .def("set_action_registry", &Service::SetActionRegistry,
           py::arg("action_registry"));
}

pybind11::module_ MakeServiceModule(pybind11::module_ scope,
                                    std::string_view module_name) {
  pybind11::module_ service = scope.def_submodule(
      std::string(module_name).c_str(), "ActionEngine Service interface.");

  BindStream(service, "WireStream");
  BindSession(service, "Session");
  BindService(service, "Service");

  return service;
}

}  // namespace act::pybindings
