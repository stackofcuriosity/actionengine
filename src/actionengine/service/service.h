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

#ifndef ACTIONENGINE_SERVICE_SERVICE_H_
#define ACTIONENGINE_SERVICE_SERVICE_H_

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>

#include "actionengine/actions/action.h"
#include "actionengine/actions/registry.h"
#include "actionengine/concurrency/concurrency.h"
#include "actionengine/net/stream.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/service/session.h"
#include "actionengine/stores/chunk_store.h"

namespace act {

/**
 * The ActionEngine service class. Manages sessions, streams, and connections.
 *
 * This class provides methods to establish and join connections, as well as
 * to set the action registry.
 *
 * The service can be instantiated with an optional action registry and
 * connection handler. If the action registry is not provided, it will be
 * initialized with an empty registry. If the connection handler is not
 * provided, it will be initialized with RunSimpleSession. The chunk
 * store factory is used to create chunk stores for new sessions. By default,
 * `LocalChunkStore`s are created.
 *
 * This class is thread-safe. Whenever a connection is joined, it is moved out
 * of the Service object under a lock, and the Service object is no longer
 * responsible for managing it. Action registry is set under a lock, and
 * getters use read locks.
 *
 * The Service object can be destroyed at any time, and it will take care of
 * joining all the connections and cleaning up all the fibers. However, the
 * Service object itself will not be destroyed until all the connections have
 * been joined.
 *
 * @headerfile actionengine/service/service.h
 */
class Service : public std::enable_shared_from_this<Service> {
 public:
  explicit Service(
      ActionRegistry* absl_nullable action_registry = nullptr,
      StreamHandler connection_handler = internal::DefaultStreamHandler,
      ChunkStoreFactory chunk_store_factory = {});

  ~Service();

  // This class is not copyable or movable.
  Service(const Service& other) = delete;
  Service& operator=(const Service& other) = delete;

  /**
   * Returns the stream with the given ID managed by this service.
   *
   * @param stream_id
   *   The ID of the stream to retrieve.
   * @return
   *   A pointer to the WireStream if it exists, or nullptr if it does not.
   */
  auto GetStream(std::string_view stream_id) const -> WireStream* absl_nullable;
  /**
   * Returns the session with the given ID managed by this service.
   *
   * @param session_id
   *   The ID of the session to retrieve.
   * @return
   *   A pointer to the Session if it exists, or nullptr if it does not.
   */
  auto GetSession(std::string_view session_id) const -> Session* absl_nullable;
  auto GetSessionKeys() const -> std::vector<std::string>;

  /**
   * Establishes a connection with the given stream and returns a
   * StreamToSessionConnection object representing the connection.
   *
   * The stream must be a valid WireStream instance that is already connected
   * and ready to send and receive messages. The connection handler is used to
   * handle the connection, and if it is not provided, the default handler
   * RunSimpleSession is used.
   *
   * The lifetime of the passed WireStream is guaranteed to be at least
   * as long as the connection, so it is safe to use the stream anywhere
   * in the connection handler.
   *
   * @param stream
   *   The stream to establish the connection with.
   * @param connection_handler
   *   The connection handler to use for the connection. If not provided,
   *   RunSimpleSession is used.
   * @return
   *   A StreamToSessionConnection object representing the established
   *   connection, or an error status if the connection could not be established.
   */
  absl::Status StartStreamHandler(std::shared_ptr<WireStream> stream,
                                  StreamHandler connection_handler = {});

  /**
   * Sets the action registry for the service.
   *
   * This method allows you to set a custom action registry for the service.
   * The action registry is used to resolve ActionMessages into Actions that
   * can be executed in the context of a session.
   *
   * This method does not introduce a race with connection handlers, and
   * any ActionMessage received after this call succeeds will be resolved
   * using the new action registry. This can be used to dynamically change the
   * behavior of the service at runtime, for example, to add, remove or modify
   * actions that can be executed in the context of a session.
   *
   * No stream is interrupted by this call, so interesting applications are
   * available, such as rapid prototyping of new actions in a Jupyter notebook.
   *
   * @param action_registry
   *   The ActionRegistry to set for the service.
   */
  auto SetActionRegistry(const ActionRegistry& action_registry) const -> void;

  void DisallowNewConnections() {
    act::MutexLock lock(&mu_);
    allow_new_connections_ = false;
  }

 private:
  StreamHandler EnsureCleanupOnDone(StreamHandler handler);

  std::vector<std::unique_ptr<thread::Fiber>> GatherConnectionFibers()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    std::vector<std::unique_ptr<thread::Fiber>> fibers;
    fibers.reserve(streams_.size());
    for (auto& [session_id, session] : sessions_) {
      absl::flat_hash_map<std::string, std::unique_ptr<internal::ConnectionCtx>>
          ctxs = session->ExtractAllStreamHandlers();
      for (auto& [stream_id, ctx] : ctxs) {
        fibers.push_back(ctx->ExtractHandlerFiber());
      }
    }
    return fibers;
  }

  std::unique_ptr<ActionRegistry> action_registry_;
  StreamHandler connection_handler_;
  ChunkStoreFactory chunk_store_factory_;

  mutable act::Mutex mu_;
  absl::flat_hash_map<std::string, std::shared_ptr<WireStream>> streams_
      ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<std::string, std::unique_ptr<NodeMap>> node_maps_
      ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<std::string, std::unique_ptr<Session>> sessions_
      ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<std::string, std::string> stream_to_session_
      ABSL_GUARDED_BY(mu_);

  bool allow_new_connections_ ABSL_GUARDED_BY(mu_) = true;
};

}  // namespace act

#endif  // ACTIONENGINE_SERVICE_SERVICE_H_
