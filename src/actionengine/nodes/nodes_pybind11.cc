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

#include "actionengine/nodes/nodes_pybind11.h"

#include <memory>
#include <string>
#include <string_view>

#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "actionengine/nodes/node_map.h"
#include "actionengine/stores/chunk_store.h"
#include "actionengine/stores/chunk_store_pybind11.h"
#include "actionengine/stores/chunk_store_reader.h"
#include "actionengine/util/utils_pybind11.h"

namespace act {}  // namespace act

namespace act::pybindings {

void BindNodeMap(py::handle scope, std::string_view name) {
  py::classh<NodeMap>(scope, std::string(name).c_str(),
                      py::release_gil_before_calling_cpp_dtor())
      .def(MakeSameObjectRefConstructor<NodeMap>())
      .def(py::init([](const ChunkStoreFactory& factory = {}) {
             return std::make_shared<NodeMap>(factory);
           }),
           py::arg_v("chunk_store_factory", py::none()))
      .def(
          "get",
          [](const std::shared_ptr<NodeMap>& self, std::string_view id) {
            return ShareWithNoDeleter(self->Get(id));
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "borrow",
          [](const std::shared_ptr<NodeMap>& self, std::string_view id) {
            return self->Borrow(id);
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "extract",
          [](const std::shared_ptr<NodeMap>& self,
             std::string_view id) -> std::optional<std::shared_ptr<AsyncNode>> {
            std::shared_ptr<AsyncNode> node = self->Extract(id);
            if (node == nullptr) {
              return std::nullopt;
            }
            return node;
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "contains",
          [](const std::shared_ptr<NodeMap>& self, std::string_view id) {
            return self->contains(id);
          },
          py::call_guard<py::gil_scoped_release>());
}

void BindAsyncNode(py::handle scope, std::string_view name) {
  py::classh<AsyncNode>(scope, std::string(name).c_str(),
                        py::release_gil_before_calling_cpp_dtor())
      .def(py::init<>(), py::call_guard<py::gil_scoped_release>())
      .def(MakeSameObjectRefConstructor<AsyncNode>())
      .def(py::init([](const std::string& id, NodeMap* node_map,
                       std::unique_ptr<PyChunkStore> chunk_store = nullptr) {
             return std::make_shared<AsyncNode>(id, node_map,
                                                std::move(chunk_store));
           }),
           py::arg_v("id", ""), py::arg_v("node_map", nullptr),
           py::arg_v("chunk_store", nullptr),
           py::call_guard<py::gil_scoped_release>())
      // it is not possible to pass a std::unique_ptr to pybind11, so we pass
      // the factory function instead.
      .def(py::init([](const std::string& id, NodeMap* node_map,
                       const ChunkStoreFactory& chunk_store_factory = {}) {
             std::unique_ptr<ChunkStore> chunk_store(nullptr);
             if (chunk_store_factory) {
               chunk_store = chunk_store_factory(id);
             }
             return std::make_shared<AsyncNode>(id, node_map,
                                                std::move(chunk_store));
           }),
           py::arg_v("id", ""), py::arg_v("node_map", nullptr),
           py::arg_v("chunk_store_factory", py::none()))
      .def(
          "put_fragment",
          [](const std::shared_ptr<AsyncNode>& self, NodeFragment fragment,
             int seq = -1) { return self->Put(std::move(fragment), seq); },
          py::arg_v("fragment", NodeFragment()), py::arg_v("seq", -1),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "put_chunk",
          [](const std::shared_ptr<AsyncNode>& self, Chunk chunk, int seq = -1,
             bool final = false) {
            return self->Put(std::move(chunk), seq, final);
          },
          py::arg_v("chunk", Chunk()), py::arg_v("seq", -1),
          py::arg_v("final", false), py::call_guard<py::gil_scoped_release>())
      .def(
          "bind_stream",
          [](const std::shared_ptr<AsyncNode>& self,
             const std::shared_ptr<WireStream>& stream) {
            absl::flat_hash_map<std::string, WireStream*> peers;
            peers[stream->GetId()] = stream.get();
            self->GetWriter().BindPeers(std::move(peers));
          },
          py::arg("stream"), py::call_guard<py::gil_scoped_release>())
      .def(
          "next_fragment",
          [](const std::shared_ptr<AsyncNode>& self, double timeout = -1.0,
             const py::object& future =
                 py::none()) -> absl::StatusOr<std::optional<NodeFragment>> {
            absl::Duration timeout_duration =
                timeout < 0 ? absl::InfiniteDuration() : absl::Seconds(timeout);

            if (future.is_none()) {
              py::gil_scoped_release release_gil;
              absl::StatusOr<std::optional<NodeFragment>> result =
                  self->Next<NodeFragment>(timeout_duration);
              if (!result.ok()) {
                absl::Status status(
                    result.status().code(),
                    absl::StrCat(absl::StrFormat("[%s]: ", self->GetId()),
                                 result.status().message()));
                return status;
              }
              return *std::move(result);
            }
            // TODO: this work is not worthy of its own fibers and should
            //       be pooled.
            thread::Detach({}, [future_handle = py::handle(future),
                                timeout_duration, node = self]() mutable {
              py::gil_scoped_acquire gil;
              py::object future = py::cast<py::object>(future_handle);
              thread::Fiber* current_fiber = thread::Fiber::Current();
              auto on_python_cancel =
                  py::cpp_function([current_fiber](py::handle future) {
                    py::gil_scoped_acquire gil;
                    if (!future.attr("cancelled")().cast<bool>()) {
                      return;
                    }
                    current_fiber->Cancel();
                  });
              future.attr("add_done_callback")(py::handle(on_python_cancel));

              absl::StatusOr<std::optional<NodeFragment>> result;
              {
                py::gil_scoped_release release_gil;
                result = node->Next<NodeFragment>(timeout_duration);
              }
              absl::Status status;
              if (!result.ok()) {
                status = absl::Status(
                    result.status().code(),
                    absl::StrCat(absl::StrFormat("[%s]: ", node->GetId()),
                                 result.status().message()));
              }
              if (!status.ok()) {
                if (!future.attr("done")().cast<bool>()) {
                  py::object loop = future.attr("get_loop")();
                  loop.attr("call_soon_threadsafe")(
                      future.attr("set_exception"),
                      PyObject_CallFunction(PyExc_RuntimeError, "s",
                                            status.message()));
                }
                return;
              }
              py::object loop = future.attr("get_loop")();
              loop.attr("call_soon_threadsafe")(future.attr("set_result"),
                                                *std::move(result));
            });
            return std::nullopt;
          },
          py::arg_v("timeout", -1.0), py::arg_v("future", py::none()))
      .def(
          "next_chunk",
          [](const std::shared_ptr<AsyncNode>& self, double timeout = -1.0,
             const py::object& future =
                 py::none()) -> absl::StatusOr<std::optional<Chunk>> {
            absl::Duration timeout_duration =
                timeout < 0 ? absl::InfiniteDuration() : absl::Seconds(timeout);

            if (future.is_none()) {
              py::gil_scoped_release release_gil;
              absl::StatusOr<std::optional<Chunk>> result =
                  self->Next<Chunk>(timeout_duration);
              if (!result.ok()) {
                absl::Status status(
                    result.status().code(),
                    absl::StrCat(absl::StrFormat("[%s]: ", self->GetId()),
                                 result.status().message()));
                return status;
              }
              return *std::move(result);
            }
            // TODO: this work is not worthy of its own fibers and should
            //       be pooled.
            thread::Detach({}, [future_handle = py::handle(future),
                                timeout_duration, node = self]() mutable {
              py::gil_scoped_acquire gil;
              py::object future = py::cast<py::object>(future_handle);
              thread::Fiber* current_fiber = thread::Fiber::Current();
              auto on_python_cancel =
                  py::cpp_function([current_fiber](py::handle future) {
                    py::gil_scoped_acquire gil;
                    if (!future.attr("cancelled")().cast<bool>()) {
                      return;
                    }
                    current_fiber->Cancel();
                  });
              future.attr("add_done_callback")(py::handle(on_python_cancel));

              absl::StatusOr<std::optional<Chunk>> result;
              {
                py::gil_scoped_release release_gil;
                result = node->Next<Chunk>(timeout_duration);
              }
              absl::Status status;
              if (!result.ok()) {
                status = absl::Status(
                    result.status().code(),
                    absl::StrCat(absl::StrFormat("[%s]: ", node->GetId()),
                                 result.status().message()));
              }
              if (!status.ok()) {
                if (!future.attr("done")().cast<bool>()) {
                  py::object loop = future.attr("get_loop")();
                  loop.attr("call_soon_threadsafe")(
                      future.attr("set_exception"),
                      PyObject_CallFunction(PyExc_RuntimeError, "s",
                                            status.message()));
                }
                return;
              }
              py::object loop = future.attr("get_loop")();
              loop.attr("call_soon_threadsafe")(future.attr("set_result"),
                                                *std::move(result));
            });
            return std::nullopt;
          },
          py::arg_v("timeout", -1.0), py::arg_v("future", py::none()))
      // .def(
      //     "next_chunk_future",
      //     [](const std::shared_ptr<AsyncNode>& self, double timeout = -1.0,
      //        const py::object& future =
      //            py::none()) -> py::object {
      //       absl::Duration timeout_duration =
      //           timeout < 0 ? absl::InfiniteDuration() : absl::Seconds(timeout);
      //
      //       thread::Detach({}, [future, timeout_duration,
      //                           node = self]() mutable {
      //         thread::Fiber* current_fiber = thread::Fiber::Current();
      //         auto on_python_cancel =
      //             py::cpp_function([future, current_fiber](py::handle) {
      //               if (!future.attr("cancelled").cast<bool>()) {
      //                 return;
      //               }
      //               current_fiber->Cancel();
      //             });
      //         future.attr("add_done_callback")(py::handle(on_python_cancel));
      //
      //         absl::StatusOr<std::optional<Chunk>> result;
      //         {
      //           py::gil_scoped_release release_gil;
      //           result = node->Next<Chunk>(timeout_duration);
      //         }
      //         absl::Status status;
      //         if (!result.ok()) {
      //           status = absl::Status(
      //               result.status().code(),
      //               absl::StrCat(absl::StrFormat("[%s]: ", node->GetId()),
      //                            result.status().message()));
      //         }
      //         if (!status.ok()) {
      //           future.attr("set_exception")(
      //               std::runtime_error(std::string(status.message())));
      //         }
      //         future.attr("set_result")(*std::move(result));
      //       });
      //     },
      //     py::arg_v("timeout", -1.0), py::arg_v("future", py::none()))
      .def("get_id",
           [](const std::shared_ptr<AsyncNode>& self) { return self->GetId(); })
      .def(
          "make_reader",
          [](const std::shared_ptr<AsyncNode>& self,
             const ChunkStoreReaderOptions& options) {
            return std::shared_ptr(self->MakeReader(options));
          },
          py::arg_v("options", ChunkStoreReaderOptions()),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "set_reader_options",
          [](const std::shared_ptr<AsyncNode>& self,
             std::optional<bool> ordered = std::nullopt,
             std::optional<bool> remove_chunks = std::nullopt,
             std::optional<int> n_chunks_to_buffer = std::nullopt,
             std::optional<double> timeout = std::nullopt,
             int start_seq_or_offset = -1) {
            ChunkStoreReaderOptions options;
            options.ordered = ordered;
            options.remove_chunks = remove_chunks;
            options.n_chunks_to_buffer = n_chunks_to_buffer;
            options.start_seq_or_offset = start_seq_or_offset;

            std::optional<absl::Duration> timeout_duration = std::nullopt;
            if (timeout.has_value()) {
              timeout_duration = *timeout < 0 ? absl::InfiniteDuration()
                                              : absl::Seconds(*timeout);
            }
            options.timeout = timeout_duration;
            self->SetReaderOptions(options);
            return self;
          },
          py::arg_v("ordered", py::none()),
          py::arg_v("remove_chunks", py::none()),
          py::arg_v("n_chunks_to_buffer", py::none()),
          py::arg_v("timeout", py::none()), py::arg_v("start_seq_or_offset", 0),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "set_reader_options",
          [](const std::shared_ptr<AsyncNode>& self,
             const ChunkStoreReaderOptions& options) {
            self->SetReaderOptions(options);
            return self;
          },
          py::arg_v("options", ChunkStoreReaderOptions()),
          py::call_guard<py::gil_scoped_release>())
      .def("get_chunk_store", [](const std::shared_ptr<AsyncNode>& self) {
        return ShareWithNoDeleter(self->GetChunkStore());
      });
}

}  // namespace act::pybindings
