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

#ifndef ACTIONENGINE_SERVICE_SESSION_H_
#define ACTIONENGINE_SERVICE_SESSION_H_

#include <memory>
#include <string_view>

#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/status/status.h>
#include <absl/time/time.h>

#include "actionengine/actions/action.h"
#include "actionengine/actions/registry.h"
#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/types.h"
#include "actionengine/net/stream.h"
#include "actionengine/nodes/async_node.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/stores/chunk_store.h"

namespace act {

struct StreamDispatchTask {
  std::shared_ptr<WireStream> stream;
  std::unique_ptr<thread::Fiber> fiber;
  absl::Status status;
  thread::PermanentEvent done;
};

/**
 * A session for handling ActionEngine actions.
 *
 * This class is used to manage the lifecycle of a session, including dispatching
 * messages and managing nodes and actions.
 *
 * @headerfile actionengine/service/session.h
 */
bool IsReservedActionName(std::string_view name);

class Session;

/**
 * A function type that handles a connection between a stream and a session.
 *
 * Most use cases will use the default handler, `DefaultStreamHandler`, which
 * simply receives messages from the stream and dispatches them to the session,
 * putting NodeFragments into the attached NodeMap and executing Actions
 * resolved from the ActionRegistry according to the ActionMessages
 * received from the stream.
 *
 * However, this type can be used to define custom connection handlers
 * that can implement different logic for handling the connection, such as
 * handling multiple streams in a single session, or implementing
 * custom logic for handling ActionMessages and NodeFragments. It is allowed
 * and safe to use multiple handlers for different connections in a single
 * application.
 */
using StreamHandler = std::function<absl::Status(
    const std::shared_ptr<WireStream>& stream, Session* absl_nonnull session,
    absl::Duration recv_timeout)>;

namespace internal {

StreamHandler EnsureHalfClosesOrAbortsStream(StreamHandler handler);

absl::Status DefaultStreamHandler(
    std::shared_ptr<WireStream> stream, Session* absl_nonnull session,
    absl::Duration recv_timeout = absl::InfiniteDuration());

class ConnectionCtx {
 public:
  ConnectionCtx(Session* absl_nonnull session,
                std::shared_ptr<WireStream> stream, StreamHandler handler,
                absl::Duration recv_timeout);

  ~ConnectionCtx();

  /**
   * Cancel the connection handler fiber.
   *
   * Only cancels the handler fiber, does not release any resources,
   * does not communicate with the stream in any way, and does not
   * set a cancelled status.
   */
  void CancelHandler() const;

  /**
   * Join the connection handler fiber, release resources, including
   * the shared reference to the wire stream.
   *
   * Returns the status of the connection handler.
   */
  absl::Status Join();

  std::unique_ptr<thread::Fiber> ExtractHandlerFiber();

 private:
  Session* absl_nonnull session_;
  std::shared_ptr<WireStream> stream_;
  std::unique_ptr<thread::Fiber> fiber_;
  std::shared_ptr<absl::Status> status_;
};

}  // namespace internal

class Session {
 public:
  friend class ActionExecutionContext;
  friend class Action;

  Session() = default;
  ~Session();

  // This class is not copyable or movable.
  Session(const Session& other) = delete;
  Session& operator=(const Session& other) = delete;

  /**
   * Start a stream handler for the given stream.
   *
   * The session will share ownership of the stream and will start
   * an exclusively owned fiber to handle the stream using the provided handler.
   */
  void StartStreamHandler(
      std::string_view id, std::shared_ptr<WireStream> stream,
      StreamHandler handler = internal::DefaultStreamHandler,
      absl::Duration recv_timeout = absl::InfiniteDuration());

  /**
   * Returns the context of the given connection, without cancelling or
   * joining the fiber. After the call, the session will not own the
   * connection, and the caller is responsible for managing the connection's
   * lifecycle.
   *
   * @param id The id of the connection.
   * @return The context of the given connection, or nullptr.
   */
  std::unique_ptr<internal::ConnectionCtx> ExtractStreamHandler(
      std::string_view id);

  absl::flat_hash_map<std::string, std::unique_ptr<internal::ConnectionCtx>>
  ExtractAllStreamHandlers();

  absl::Status DispatchNodeFragment(NodeFragment node_fragment);

  absl::Status DispatchActionMessage(
      ActionMessage action_message,
      WireStream* absl_nullable origin_stream = nullptr);

  absl::Status DispatchWireMessage(
      WireMessage message, WireStream* absl_nullable origin_stream = nullptr);

  absl::Status AwaitAllActions(
      absl::Duration timeout = absl::InfiniteDuration());

  void CancelAllActions();
  void CancelAllConnections();

  size_t GetNumActiveConnections() const;

  AsyncNode* absl_nullable GetNode(std::string_view id,
                                   const ChunkStoreFactory& factory = {}) const;

  NodeMap* absl_nullable node_map() const;
  void set_node_map(NodeMap* absl_nullable node_map);

  ActionRegistry* absl_nullable action_registry();
  void set_action_registry(std::optional<ActionRegistry> action_registry);

 private:
  mutable act::Mutex mu_;

  absl::Status RecordActionCall(std::string_view id,
                                std::shared_ptr<Action> action);
  absl::StatusOr<std::shared_ptr<Action>> ExtractAction(std::string_view id);
  absl::StatusOr<std::shared_ptr<Action>> ExtractAction_(std::string_view id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status AwaitAllActions_(
      absl::Duration timeout = absl::InfiniteDuration())
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status DispatchNodeFragmentInternal(NodeFragment node_fragment)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status DispatchReservedActionInternal(
      ActionMessage action_message,
      WireStream* absl_nullable origin_stream = nullptr)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status DispatchActionMessageInternal(
      ActionMessage action_message,
      WireStream* absl_nullable origin_stream = nullptr)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status DispatchWireMessageInternal(
      WireMessage message, WireStream* absl_nullable origin_stream = nullptr)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  bool finalizing_ ABSL_GUARDED_BY(mu_) = false;

  absl::flat_hash_map<std::string, std::unique_ptr<internal::ConnectionCtx>>
      connections_ ABSL_GUARDED_BY(mu_);

  std::optional<ActionRegistry> action_registry_ ABSL_GUARDED_BY(mu_);
  NodeMap* absl_nullable node_map_ ABSL_GUARDED_BY(mu_) = nullptr;

  absl::flat_hash_map<std::string, std::shared_ptr<Action>> actions_
      ABSL_GUARDED_BY(mu_);
};

}  // namespace act

#endif  // ACTIONENGINE_SERVICE_SESSION_H_
