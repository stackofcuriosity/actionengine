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

#ifndef ACTIONENGINE_ACTIONS_ACTION_H_
#define ACTIONENGINE_ACTIONS_ACTION_H_

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/status/status.h>
#include <absl/time/time.h>

#include "actionengine/actions/registry.h"
#include "actionengine/actions/schema.h"
#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/types.h"
#include "actionengine/net/stream.h"
#include "actionengine/nodes/async_node.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/stores/chunk_store_reader.h"

namespace act {

class Session;
class ActionExecutionContext;
class Action;

struct ActionRunState {
  std::unique_ptr<thread::Fiber> fiber;
  std::optional<absl::Status> run_result;
};

struct ActionCallState {
  std::optional<absl::Status> dispatch_status;
  std::unique_ptr<thread::PermanentEvent> dispatched{
      std::make_unique<thread::PermanentEvent>()};

  std::optional<absl::Status> completion_status;
  std::unique_ptr<thread::PermanentEvent> completed{
      std::make_unique<thread::PermanentEvent>()};

  bool successfully_cancelled_on_remote = false;
};

using RunOrCallState = std::variant<ActionRunState, ActionCallState>;

class ActionBoundResources {
 public:
  static ActionBoundResources SameAs(const ActionBoundResources& other);
  /**
   * Destructor simply ensures the correct order of destruction in case
   * of owning resources:
   *
   * node_map_ -> stream_ -> session_ -> registry_.
   */
  ~ActionBoundResources();

  [[nodiscard]] NodeMap* absl_nullable node_map() const;
  [[nodiscard]] std::shared_ptr<NodeMap> borrow_node_map() const;
  void set_node_map(std::shared_ptr<NodeMap> node_map);
  void set_node_map_non_owning(NodeMap* absl_nullable node_map);

  [[nodiscard]] WireStream* absl_nullable stream() const;
  [[nodiscard]] std::shared_ptr<WireStream> borrow_stream() const;
  void set_stream(std::shared_ptr<WireStream> stream);
  void set_stream_non_owning(WireStream* absl_nullable stream);

  [[nodiscard]] Session* absl_nullable session() const;
  [[nodiscard]] std::shared_ptr<Session> borrow_session() const;
  void set_session(std::shared_ptr<Session> session);
  void set_session_non_owning(Session* absl_nullable session);

  [[nodiscard]] ActionRegistry* absl_nullable registry() const;
  [[nodiscard]] std::shared_ptr<ActionRegistry> borrow_registry() const;
  void set_registry(std::shared_ptr<ActionRegistry> registry);
  void set_registry_non_owning(ActionRegistry* absl_nullable registry);

 private:
  std::shared_ptr<NodeMap> node_map_;
  std::shared_ptr<WireStream> stream_;
  std::shared_ptr<Session> session_;
  std::shared_ptr<ActionRegistry> registry_;
};

class ActionExecutionContext {
 public:
  friend class Action;

  ActionExecutionContext() = default;
  ~ActionExecutionContext();

  // non-copyable, non-moveable
  ActionExecutionContext& operator=(const ActionExecutionContext&) = delete;
  ActionExecutionContext(const ActionExecutionContext&) = delete;

  /**
   * Initialise action run state
   *
   * @param state Action state that's exclusive to being run: run status,
   *   running fiber, etc.
   * @return absl::OkStatus() if the action is successfully recorded as running,
   *   an error status otherwise.
   */
  absl::Status RecordRun(ActionRunState state);

  absl::Status RecordCall(ActionCallState state);

  /**
   * Cancel the action if it is still in progress.
   *
   * For non-started or completed actions, this function will set cancellation
   * events and make HasBeenCancelled() return true, but will not prevent
   * the handler from running.
   *
   * For actions that are called (so run remotely), this function will notify
   * the local cancellation event immediately, then make a remote cancellation
   * request. HasBeenCancelled() will only return true after the remote
   * cancellation request is completed successfully.
   *
   * For actions that have already been cancelled, this function depends on
   * whether they are run or called. For unstarted or locally run actions,
   * it will return immediately with success. For called actions, it will
   * first notify the local cancellation event (if not already notified),
   * then make a remote cancellation request, and return its status.
   *
   * Successful cancellations are idempotent, local OR remote.
   *
   * @return absl::Status indicating success or failure
   */
  absl::Status Cancel();

  // Status is an output parameter to be able to distinguish a DeadlineError
  // coming from awaiting too long and a DeadlineError as an execution result.
  absl::Status Await(absl::Status* absl_nonnull status_out,
                     absl::Time deadline = absl::InfiniteFuture());

  absl::StatusOr<thread::Case> OnDone() const;
  [[nodiscard]] bool IsDone() const;
  [[nodiscard]] bool HasBeenCancelled() const;
  [[nodiscard]] bool HasBeenRun() const;
  [[nodiscard]] bool HasBeenCalled() const;

  absl::Status SetDispatchStatus(absl::Status status);
  absl::Status SetCompletionStatus(absl::Status status);

  ActionRunState* absl_nullable run_state();
  ActionCallState* absl_nullable call_state();

 private:
  // Ensures that the action has been done, failed or cancelled, and the state
  // is fully clear of any blocking waits.
  void Finalize_() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void RunHandlerWithPreparationAndCleanup(
      ActionHandler handler, const std::shared_ptr<Action>& action)
      ABSL_LOCKS_EXCLUDED(mu_);

  void CommunicateHandlerStatus_(const std::shared_ptr<Action>& action,
                                 absl::Status status)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void ReleaseResourcesAfterRun_(const std::shared_ptr<Action>& action)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  [[nodiscard]] bool HasBeenRun_() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  [[nodiscard]] bool HasBeenCalled_() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void CancelLocally_() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  absl::Status CancelRemotely_() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  [[nodiscard]] bool CancelledLocally_() const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  [[nodiscard]] bool CancelledSuccessfullyOnRemote_() const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status AwaitRun_(absl::Time deadline,
                         absl::Status* absl_nonnull status_out)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  absl::Status AwaitCall_(absl::Time deadline,
                          absl::Status* absl_nonnull status_out)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void MarkAsOutput_(std::shared_ptr<AsyncNode> node)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  void MarkAsInput_(std::shared_ptr<AsyncNode> node)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void SetOnCancelled(absl::AnyInvocable<void()> callback)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  mutable act::Mutex mu_;
  bool cancelled_ ABSL_GUARDED_BY(mu_) = false;
  bool joined_ ABSL_GUARDED_BY(mu_) = false;
  bool detached_ ABSL_GUARDED_BY(mu_) = false;
  std::optional<RunOrCallState> progress_state_ ABSL_GUARDED_BY(mu_);

  absl::flat_hash_map<std::string, std::string> input_name_to_id_;
  absl::flat_hash_map<std::string, std::string> output_name_to_id_;

  // Maintains a collection of nodes that have been registered as outputs.
  // On an unsuccessful run, these nodes will be notified of the failure.
  absl::flat_hash_set<std::shared_ptr<AsyncNode>> output_nodes_
      ABSL_GUARDED_BY(mu_);

  // Maintains a collection of nodes that have been registered as inputs.
  // On failure or cancellation, the readers of these nodes will be cancelled.
  absl::flat_hash_set<std::shared_ptr<AsyncNode>> input_nodes_
      ABSL_GUARDED_BY(mu_);

  absl::AnyInvocable<void()> on_cancelled_;
};

struct ActionSettings {
  // the default settings are for callers, so that if inputs are set
  // before calling the action, they are still streamed correctly to the
  // other party.
  bool bind_streams_on_inputs_by_default = true;
  bool bind_streams_on_outputs_by_default = false;

  bool clear_inputs_after_run = false;
  bool clear_outputs_after_run = false;
};

class Action : public std::enable_shared_from_this<Action> {
 public:
  // This class is not strictly thread-compatible, as it does nothing to prevent
  // races between changing action settings or headers and calling/running it.
  // If changed definitely after execution, a warning is logged.

  friend class Session;

  static std::string MakeNodeId(std::string_view action_id,
                                std::string_view node_name);

  explicit Action(std::string_view id = "");
  ~Action();

  absl::Status Run(absl::Duration timeout = absl::InfiniteDuration());
  absl::Status RunInBackground(bool detach = false);

  absl::Status Call(
      absl::flat_hash_map<std::string, std::string> wire_message_headers = {});

  absl::StatusOr<thread::Case> OnDone() const;
  bool IsDone() const;
  bool HasBeenRun() const;
  bool HasBeenCalled() const;
  bool HasBeenCancelled() const;

  absl::Status SetDispatchStatus(absl::Status status);
  absl::Status SetCompletionStatus(absl::Status status);

  absl::Status Cancel();
  absl::Status Await(absl::Duration timeout = absl::InfiniteDuration(),
                     bool* absl_nullable timeout_exceeded = nullptr);

  [[nodiscard]] absl::StatusOr<ActionMessage> GetActionMessage() const
      ABSL_LOCKS_EXCLUDED(ctx_.mu_);
  absl::Status MapPortsFromMessage(const ActionMessage& message);

  absl::StatusOr<std::unique_ptr<Action>> MakeActionInSameSession(
      std::string_view name, std::string_view action_id = "") const;

  AsyncNode* absl_nullable GetInput(
      std::string_view name, std::optional<bool> bind_stream = std::nullopt);
  AsyncNode* absl_nullable GetOutput(
      std::string_view name, std::optional<bool> bind_stream = std::nullopt);

  absl::StatusOr<void*> GetUserData(std::string_view key) const;
  void SetUserData(std::string_view key, std::shared_ptr<void> value);

  void SetOnCancelled(absl::AnyInvocable<void()> callback);

  ActionSchema schema() const;
  bool has_schema() const;
  void set_schema(ActionSchema schema);

  ActionHandler handler() const;
  bool has_handler() const;
  void set_handler(ActionHandler handler);

  const std::string& id() const;
  void set_id(std::string_view id);

  const absl::flat_hash_map<std::string, std::string>& headers() const;
  std::optional<std::string> get_header(std::string_view key) const;
  bool has_header(std::string_view key) const;
  void set_header(std::string_view key, std::string_view value);
  void remove_header(std::string_view key);

  const ActionBoundResources& bound_resources() const;
  ActionBoundResources* absl_nonnull mutable_bound_resources();

  const ActionSettings& settings() const;
  ActionSettings* absl_nonnull mutable_settings();

 private:
  static std::string GetNoChangeAfterExecWarningMessage(std::string_view field);
  void LogWarningIfChangedAfterExec(std::string_view field) const;
  void UnbindSessionInternal();

  std::optional<ActionSchema> schema_;
  ActionHandler handler_;
  std::string id_;

  absl::flat_hash_map<std::string, std::string> headers_;

  ActionBoundResources bound_resources_;
  ActionSettings settings_;

  ActionExecutionContext ctx_;
  absl::flat_hash_map<std::string, std::shared_ptr<void>> user_data_;
};

inline absl::StatusOr<void*> Action::GetUserData(std::string_view key) const {
  const auto it = user_data_.find(key);
  if (it == user_data_.end()) {
    return absl::NotFoundError(absl::StrCat("Key not found: ", key));
  }
  return it->second.get();
}

inline void Action::SetUserData(std::string_view key,
                                std::shared_ptr<void> value) {
  user_data_[key] = std::move(value);
}

}  // namespace act

#endif  // ACTIONENGINE_ACTIONS_ACTION_H_