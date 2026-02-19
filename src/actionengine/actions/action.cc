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

#include "actionengine/actions/action.h"

#include "actionengine/service/session.h"
#include "actionengine/util/random.h"

namespace act {

ActionBoundResources ActionBoundResources::SameAs(
    const ActionBoundResources& other) {
  ActionBoundResources result;
  result.set_node_map(other.node_map_);
  result.set_stream(other.stream_);
  result.set_session(other.session_);
  result.set_registry(other.registry_);
  return result;
}

ActionBoundResources::~ActionBoundResources() {
  node_map_.reset();
  stream_.reset();
  session_.reset();
  registry_.reset();
}

NodeMap* ActionBoundResources::node_map() const {
  return node_map_.get();
}

std::shared_ptr<NodeMap> ActionBoundResources::borrow_node_map() const {
  return node_map_;
}

void ActionBoundResources::set_node_map(std::shared_ptr<NodeMap> node_map) {
  node_map_ = std::move(node_map);
}

void ActionBoundResources::set_node_map_non_owning(NodeMap* node_map) {
  node_map_ = std::shared_ptr<NodeMap>(node_map, [](NodeMap*) {});
}

WireStream* ActionBoundResources::stream() const {
  return stream_.get();
}

std::shared_ptr<WireStream> ActionBoundResources::borrow_stream() const {
  return stream_;
}

void ActionBoundResources::set_stream(std::shared_ptr<WireStream> stream) {
  stream_ = std::move(stream);
}

void ActionBoundResources::set_stream_non_owning(WireStream* stream) {
  stream_ = std::shared_ptr<WireStream>(stream, [](WireStream*) {});
}

Session* ActionBoundResources::session() const {
  return session_.get();
}

std::shared_ptr<Session> ActionBoundResources::borrow_session() const {
  return session_;
}

void ActionBoundResources::set_session(std::shared_ptr<Session> session) {
  session_ = std::move(session);
}

void ActionBoundResources::set_session_non_owning(Session* session) {
  session_ = std::shared_ptr<Session>(session, [](Session*) {});
}

ActionRegistry* ActionBoundResources::registry() const {
  return registry_.get();
}

std::shared_ptr<ActionRegistry> ActionBoundResources::borrow_registry() const {
  return registry_;
}

void ActionBoundResources::set_registry(
    std::shared_ptr<ActionRegistry> registry) {
  registry_ = std::move(registry);
}

void ActionBoundResources::set_registry_non_owning(ActionRegistry* registry) {
  registry_ = std::shared_ptr<ActionRegistry>(registry, [](ActionRegistry*) {});
}

ActionExecutionContext::~ActionExecutionContext() {
  act::MutexLock lock(&mu_);
  Finalize_();
}

absl::Status ActionExecutionContext::RecordRun(ActionRunState state) {
  act::MutexLock lock(&mu_);

  if (HasBeenRun_()) {
    return absl::FailedPreconditionError("Action has already been run.");
  }

  if (HasBeenCalled_()) {
    return absl::FailedPreconditionError(
        "Action has been called remotely, cannot run.");
  }

  progress_state_ = std::move(state);
  return absl::OkStatus();
}

absl::Status ActionExecutionContext::RecordCall(ActionCallState state) {
  act::MutexLock lock(&mu_);

  if (HasBeenCalled_()) {
    return absl::FailedPreconditionError("Action has already been called.");
  }

  if (HasBeenRun_()) {
    return absl::FailedPreconditionError(
        "Action has already been run, cannot be called remotely.");
  }

  progress_state_ = std::move(state);
  return absl::OkStatus();
}

absl::Status ActionExecutionContext::Cancel() {
  act::MutexLock lock(&mu_);
  if (!cancelled_) {
    cancelled_ = true;
    if (on_cancelled_) {
      mu_.unlock();
      on_cancelled_();
      mu_.lock();
    }
  }
  if (!HasBeenRun_() && !HasBeenCalled_()) {
    return absl::OkStatus();
  }
  if (HasBeenRun_()) {
    CancelLocally_();
    return absl::OkStatus();
  }
  if (HasBeenCalled_()) {
    return CancelRemotely_();
  }
  return absl::OkStatus();
}

absl::Status ActionExecutionContext::Await(
    absl::Status* absl_nonnull status_out, absl::Time deadline) {
  act::MutexLock lock(&mu_);
  if (HasBeenRun_()) {
    return AwaitRun_(deadline, status_out);
  }
  if (HasBeenCalled_()) {
    absl::Status await_status = AwaitCall_(deadline, status_out);
    return await_status;
  }
  return absl::FailedPreconditionError("Action has not been run or called.");
}

absl::StatusOr<thread::Case> ActionExecutionContext::OnDone() const {
  act::MutexLock lock(&mu_);
  if (!HasBeenRun_() && !HasBeenCalled_()) {
    return absl::FailedPreconditionError(
        "No run or call state is present at the moment. Cannot create a "
        "thread::Case.");
  }
  if (HasBeenCalled_()) {
    auto& call_state = std::get<ActionCallState>(*progress_state_);
    if (call_state.completion_status) {
      return thread::AlwaysSelectableCase();
    }
    return call_state.dispatched->OnEvent();
  }
  auto& run_state = std::get<ActionRunState>(*progress_state_);
  return run_state.fiber->OnJoinable();
}

bool ActionExecutionContext::IsDone() const {
  act::MutexLock lock(&mu_);
  if (HasBeenRun_()) {
    return std::get<ActionRunState>(*progress_state_).run_result.has_value();
  }
  if (HasBeenCalled_()) {
    return std::get<ActionCallState>(*progress_state_)
        .completion_status.has_value();
  }
  return false;
}

bool ActionExecutionContext::HasBeenCancelled() const {
  act::MutexLock lock(&mu_);
  if (!HasBeenRun_() && !HasBeenCalled_()) {
    return cancelled_;
  }
  if (HasBeenRun_()) {
    return CancelledLocally_();
  }
  if (HasBeenCalled_()) {
    return CancelledSuccessfullyOnRemote_();
  }
  return false;
}

bool ActionExecutionContext::HasBeenRun() const {
  act::MutexLock lock(&mu_);
  return HasBeenRun_();
}

bool ActionExecutionContext::HasBeenCalled() const {
  act::MutexLock lock(&mu_);
  return HasBeenCalled_();
}

absl::Status ActionExecutionContext::SetDispatchStatus(absl::Status status) {
  act::MutexLock lock(&mu_);
  if (!HasBeenCalled_()) {
    return absl::FailedPreconditionError(
        "Cannot set dispatch status: the action has not been called.");
  }
  auto& call_state = std::get<ActionCallState>(*progress_state_);

  if (call_state.successfully_cancelled_on_remote) {
    return absl::FailedPreconditionError(
        "Cannot set dispatch status after remote cancelled the action.");
  }

  if (call_state.completion_status) {
    return absl::FailedPreconditionError(
        "Cannot set dispatch status after final call status has been set.");
  }

  if (call_state.dispatch_status) {
    return absl::FailedPreconditionError("Cannot set dispatch status twice.");
  }

  if (call_state.dispatched->HasBeenNotified()) {
    return absl::FailedPreconditionError(
        "Dispatch status has already been set.");
  }
  call_state.dispatch_status = status;
  call_state.dispatched->Notify();

  if (!status.ok() && !call_state.completed->HasBeenNotified()) {
    call_state.completion_status = status;
    call_state.completed->Notify();
  }

  return absl::OkStatus();
}

absl::Status ActionExecutionContext::SetCompletionStatus(absl::Status status) {
  act::MutexLock lock(&mu_);
  if (!HasBeenCalled_()) {
    return absl::FailedPreconditionError(
        "Cannot set completion status: the action has not been called.");
  }
  auto& call_state = std::get<ActionCallState>(*progress_state_);

  if (!call_state.dispatch_status) {
    return absl::FailedPreconditionError(
        "Cannot set completion status before dispatch status is set.");
  }
  if (call_state.completion_status) {
    return absl::FailedPreconditionError("Cannot set completion status twice.");
  }

  call_state.completion_status = std::move(status);
  call_state.completed->Notify();
  return absl::OkStatus();
}

ActionRunState* ActionExecutionContext::run_state() {
  act::MutexLock lock(&mu_);
  if (!progress_state_) {
    return nullptr;
  }
  if (std::holds_alternative<ActionRunState>(*progress_state_)) {
    return &std::get<ActionRunState>(*progress_state_);
  }
  return nullptr;
}

ActionCallState* ActionExecutionContext::call_state() {
  act::MutexLock lock(&mu_);
  if (!progress_state_) {
    return nullptr;
  }
  if (std::holds_alternative<ActionCallState>(*progress_state_)) {
    return &std::get<ActionCallState>(*progress_state_);
  }
  return nullptr;
}

void ActionExecutionContext::Finalize_() {
  if (!HasBeenRun_() && !HasBeenCalled_()) {
    return;
  }
  if (HasBeenRun_()) {
    auto& run_state = std::get<ActionRunState>(*progress_state_);
    DCHECK(run_state.run_result)
        << "Failed invariant check: run result must be set by the time of the "
           "finalization of an action whose handler was run.";
    if (detached_) {
      const bool done =
          thread::SelectUntil(absl::InfinitePast(),
                              {run_state.fiber->OnJoinable()}) == 0;
      CHECK(done || thread::Fiber::Current() == run_state.fiber.get())
          << "Action is detached, but not finalizing from its own fiber.";
      thread::Detach(std::move(run_state.fiber));
      return;
    }

    CHECK(thread::Fiber::Current() != run_state.fiber.get())
        << "Action is trying to join itself without a previous detach. "
           "You should either detach or await the action in your calling "
           "code.";

    if (run_state.run_result) {
      if (!joined_) {
        mu_.unlock();
        run_state.fiber->Join();
        mu_.lock();
        joined_ = true;
      }
      return;
    }
    mu_.unlock();
    const bool fiber_cancelled =
        thread::SelectUntil(absl::InfinitePast(),
                            {run_state.fiber->OnCancel()}) == 0;
    mu_.lock();
    if (!fiber_cancelled) {
      LOG(WARNING) << "Action fiber is not done and has not been cancelled "
                      "before finalizing the Action object. Will cancel, try "
                      "to join for 5 seconds, and then detach.";
      run_state.fiber->Cancel();
    }
    mu_.unlock();
    const bool timeout_exceeded =
        thread::SelectUntil(absl::Now() + absl::Seconds(5),
                            {run_state.fiber->OnJoinable()}) == -1;
    mu_.lock();
    if (!timeout_exceeded) {
      if (!joined_) {
        mu_.unlock();
        run_state.fiber->Join();
        mu_.lock();
        joined_ = true;
      }
      return;
    }
    LOG(WARNING) << "Action fiber did not join in time, detaching.";
    thread::Detach(std::move(run_state.fiber));
    return;
  }
  // DCHECK(!HasBeenCalled_())
  //     << "Finalization not yet implemented for called actions.";
}

void ActionExecutionContext::ReleaseResourcesAfterRun_(
    const std::shared_ptr<Action>& action) {
  DCHECK(HasBeenRun_());
  DCHECK(action != nullptr);

  const auto& settings = action->settings();

  mu_.unlock();

  // Release input and output nodes in bound node map
  if (NodeMap* node_map = action->bound_resources().node_map();
      node_map != nullptr) {
    if (settings.clear_inputs_after_run) {
      for (const auto& [input_name, input_id] : input_name_to_id_) {
        std::shared_ptr<AsyncNode> node = node_map->Extract(input_id);
        // In case any nodes are still read from, cancel readers:
        node->GetReader().Cancel();
        node.reset();
      }
    }
    if (settings.clear_outputs_after_run) {
      for (const auto& [output_name, output_id] : output_name_to_id_) {
        std::shared_ptr<AsyncNode> node = node_map->Extract(output_id);
        // In case any data still have to be written out, flush writers:
        node->GetWriter().FlushCurrentBuffer();
        node.reset();
      }
    }
  }
  mu_.lock();

  // Flush any other node that has been explicitly marked as an output:
  for (const auto& node : output_nodes_) {
    mu_.unlock();
    node->GetWriter().FlushCurrentBuffer();
    mu_.lock();
  }
  output_nodes_.clear();

  if (Session* session = action->bound_resources().session();
      session != nullptr) {
    session->ExtractAction(action->id()).IgnoreError();
  }
}

bool ActionExecutionContext::HasBeenRun_() const {
  return progress_state_ &&
         std::holds_alternative<ActionRunState>(*progress_state_);
}

bool ActionExecutionContext::HasBeenCalled_() const {
  return progress_state_ &&
         std::holds_alternative<ActionCallState>(*progress_state_);
}

void ActionExecutionContext::CancelLocally_() const {
  DCHECK(!HasBeenCalled_())
      << "Invariant violation: local cancellation must not be called when "
         "remote execution is requested.";
  // For inputs which are still waiting for data, report a cancelled status.
  for (const std::shared_ptr<AsyncNode>& node : input_nodes_) {
    ChunkStoreWriter& writer = node->GetWriter();
    writer.FlushCurrentBuffer();
    if (writer.AcceptsPuts()) {
      node->Put(absl::CancelledError("Action was cancelled."), -1, true)
          .IgnoreError();
      writer.FlushCurrentBuffer();
    }
  }
}

absl::Status ActionExecutionContext::CancelRemotely_() {
  // TODO: implement this properly
  if (!HasBeenCalled_()) {
    return absl::FailedPreconditionError(
        "Cannot cancel an action that has not been called yet.");
  }
  auto& call_state = std::get<ActionCallState>(*progress_state_);
  if (!call_state.completed->HasBeenNotified()) {
    call_state.completion_status =
        absl::CancelledError("Cancelled by our end.");
    call_state.completed->Notify();
  }
  return absl::OkStatus();
}

bool ActionExecutionContext::CancelledLocally_() const {
  if (!HasBeenRun_()) {
    return cancelled_;
  }
  return std::get<ActionRunState>(*progress_state_).fiber->Cancelled();
}

bool ActionExecutionContext::CancelledSuccessfullyOnRemote_() const {
  if (!HasBeenCalled_()) {
    return false;
  }
  return std::get<ActionCallState>(*progress_state_)
      .successfully_cancelled_on_remote;
}

absl::Status ActionExecutionContext::AwaitRun_(
    absl::Time deadline, absl::Status* absl_nonnull status_out) {
  DCHECK(HasBeenRun_());
  const auto& run_state = std::get<ActionRunState>(*progress_state_);
  if (thread::Fiber::Current() == run_state.fiber.get()) {
    return absl::FailedPreconditionError(
        "Trying to await a running action from its own fiber.");
  }

  DCHECK(run_state.fiber != nullptr);
  mu_.unlock();
  thread::SelectUntil(deadline, {run_state.fiber->OnJoinable()});
  mu_.lock();

  // check status before checking expiration to return results optimistically
  // in case of races between completion and the passage of the deadline:
  if (run_state.run_result) {
    mu_.unlock();
    const bool fiber_completed =
        thread::SelectUntil(absl::InfinitePast(),
                            {run_state.fiber->OnJoinable()}) == 0;
    mu_.lock();
    DCHECK(fiber_completed)
        << "Failed invariant check: if run status is set, action fiber MUST "
           "have completed.";

    if (joined_) {
      *status_out = *run_state.run_result;
      return absl::OkStatus();
    }
    mu_.unlock();
    run_state.fiber->Join();
    mu_.lock();
    joined_ = true;
    *status_out = *run_state.run_result;
    return absl::OkStatus();
  }

  return absl::DeadlineExceededError("Timed out waiting for action run.");
}

absl::Status ActionExecutionContext::AwaitCall_(
    absl::Time deadline, absl::Status* absl_nonnull status_out) {
  DCHECK(HasBeenCalled_());
  auto& call_state = std::get<ActionCallState>(*progress_state_);

  mu_.unlock();
  thread::SelectUntil(deadline, {call_state.dispatched->OnEvent()});
  mu_.lock();

  if (!call_state.dispatched->HasBeenNotified()) {
    return absl::DeadlineExceededError(
        "Timed out waiting for action call to be dispatched.");
  }

  DCHECK(call_state.dispatch_status);
  if (!call_state.dispatch_status->ok()) {
    *status_out = *call_state.dispatch_status;
    return absl::OkStatus();
  }

  mu_.unlock();
  thread::SelectUntil(deadline, {call_state.completed->OnEvent()});
  mu_.lock();

  if (call_state.completion_status) {
    DCHECK(call_state.completed->HasBeenNotified())
        << "Failed invariant check: if call has a completion status, the "
           "completion event MUST be notified.";
    if (call_state.successfully_cancelled_on_remote) {
      DCHECK(absl::IsCancelled(*call_state.completion_status))
          << "Failed invariant check: if cancelled on remote, local status "
             "must be reported as cancelled. Actual: "
          << *call_state.completion_status;
    }
    *status_out = *call_state.completion_status;
    return absl::OkStatus();
  }

  DCHECK(!call_state.completed->HasBeenNotified())
      << "Failed invariant check: "
      << "if call has no completion status, "
      << "the completion event MUST NOT be notified.";

  return absl::DeadlineExceededError(
      "Timed out waiting for action call to complete");
}

void ActionExecutionContext::MarkAsOutput_(std::shared_ptr<AsyncNode> node) {
  output_nodes_.insert(std::move(node));
}

void ActionExecutionContext::MarkAsInput_(std::shared_ptr<AsyncNode> node) {
  input_nodes_.insert(std::move(node));
}

void ActionExecutionContext::SetOnCancelled(
    absl::AnyInvocable<void()> callback) {
  on_cancelled_ = std::move(callback);
}

const ActionSettings& Action::settings() const {
  return settings_;
}

ActionSettings* Action::mutable_settings() {
  LogWarningIfChangedAfterExec("settings");
  return &settings_;
}

std::string Action::GetNoChangeAfterExecWarningMessage(std::string_view field) {
  return absl::StrCat(
      "Attempting to change or take a mutable reference to `", field,
      "`. The changes may not apply and will likely be ignored.");
}

void Action::LogWarningIfChangedAfterExec(std::string_view field) const {
  if (ctx_.HasBeenRun() || ctx_.HasBeenCalled()) {
    LOG(WARNING) << GetNoChangeAfterExecWarningMessage(field);
  }
}

void Action::UnbindSessionInternal() {
  act::MutexLock lock(&ctx_.mu_);
  bound_resources_.set_session(nullptr);
}

void ActionExecutionContext::RunHandlerWithPreparationAndCleanup(
    ActionHandler handler, const std::shared_ptr<Action>& action) {
  act::MutexLock lock(&mu_);
  DCHECK(handler != nullptr);
  DCHECK(action != nullptr);

  auto& run_state = std::get<ActionRunState>(*progress_state_);
  if (cancelled_) {
    ReleaseResourcesAfterRun_(action);
    run_state.run_result = absl::CancelledError(
        "Action was cancelled before its handler could run.");
    return;
  }

  if (action->bound_resources().node_map() != nullptr) {
    for (const auto& [input_name, input_id] : input_name_to_id_) {
      MarkAsInput_(action->bound_resources().node_map()->Borrow(input_id));
    }
    for (const auto& [output_name, output_id] : output_name_to_id_) {
      MarkAsOutput_(action->bound_resources().node_map()->Borrow(output_id));
    }
  }

  if (action->bound_resources().node_map() == nullptr &&
      (input_name_to_id_.empty() || output_name_to_id_.empty())) {
    ReleaseResourcesAfterRun_(action);
    run_state.run_result = absl::FailedPreconditionError(
        "Action has registered inputs or outputs, "
        "but no node map is bound.");
    return;
  }

  mu_.unlock();
  absl::Status handler_status = std::move(handler)(action);
  mu_.lock();

  // If any output nodes are not finalised by this point, report this in the
  // whole status:
  if (action->bound_resources().node_map() != nullptr) {
    // TODO: report blocked readers in the same way

    // for (const auto& [output_name, output_id] : output_name_to_id_) {
    //   if (output_name == "__status__" || output_name == "__dispatch_status__") {
    //     continue;
    //   }
    //   const std::shared_ptr<AsyncNode> node =
    //       action->bound_resources().node_map()->Borrow(output_id);
    //   if (node->GetWriter().AcceptsPuts()) {
    //     handler_status = absl::FailedPreconditionError(absl::StrCat(
    //         "Output node ", output_id,
    //         " was not finalised before the action handler finished."));
    //     break;
    //   }
    // }
  }

  // Populate special nodes in node map, send status to remote peers
  CommunicateHandlerStatus_(action, handler_status);
  // Release any resources such as borrowed nodes
  ReleaseResourcesAfterRun_(action);

  run_state.run_result = handler_status;
}

void ActionExecutionContext::CommunicateHandlerStatus_(
    const std::shared_ptr<Action>& action, absl::Status status) {
  const ActionBoundResources& resources = action->bound_resources();
  // 1. If a node map is bound, for every unfinalized mapped output (but not __status__):
  //   - if !status.ok(), the output gets this status, and gets flushed
  //   - if status.ok(), the output gets flushed and, if still unfinalized,
  //     it gets an OutOfRangeError telling that the action has finished,
  //     and is flushed again
  absl::flat_hash_set<std::string> mapped_output_ids;
  if (resources.node_map() != nullptr) {
    mapped_output_ids.reserve(output_name_to_id_.size());
    for (const auto& [output_name, output_id] : output_name_to_id_) {
      mapped_output_ids.insert(output_id);
      if (output_name == "__status__") {
        continue;
      }
      const std::shared_ptr<AsyncNode> node =
          resources.node_map()->Borrow(output_id);
      ChunkStoreWriter& writer = node->GetWriter();
      writer.FlushCurrentBuffer();
      if (!writer.AcceptsPuts()) {
        continue;  // this node has been finalised
      }
      if (!status.ok()) {
        node->Put(status, -1, true).IgnoreError();
      } else {
        node->Put(absl::OutOfRangeError(
                      absl::StrCat("The action has completed successfully, but "
                                   "did not finalise this output node: ",
                                   output_name)))
            .IgnoreError();
      }
      writer.FlushCurrentBuffer();
    }

    //   - every mapped input gets cancelled
    for (const std::shared_ptr<AsyncNode>& node : input_nodes_) {
      node->GetReader().Cancel();
    }
  }

  // 2. For marked nodes outside the map, the same happens, except OutOfRange
  //    is not communicated on success (as other consumers may be listening
  //    to the node)
  for (const std::shared_ptr<AsyncNode>& node : output_nodes_) {
    if (mapped_output_ids.contains(node->GetId())) {
      continue;  // we are only doing this for unmapped nodes in this loop
    }
    ChunkStoreWriter& writer = node->GetWriter();
    writer.FlushCurrentBuffer();
    if (!writer.AcceptsPuts()) {
      continue;  // this node has been finalised
    }
    if (!status.ok()) {
      node->Put(status, -1, true).IgnoreError();
    }
    writer.FlushCurrentBuffer();
  }

  // 3. status itself gets communicated:
  //   - if a node map is bound, status is written to __status__ and flushed,
  //     so it also gets to the stream,
  if (resources.node_map() != nullptr) {
    const std::shared_ptr<AsyncNode> node =
        resources.node_map()->Borrow(output_name_to_id_["__status__"]);
    ChunkStoreWriter& writer = node->GetWriter();
    writer.FlushCurrentBuffer();
    if (WireStream* stream = action->bound_resources().stream();
        stream != nullptr) {
      writer.BindPeers({{stream->GetId(), stream}});
    }
    if (!node->Put(status).ok()) {
      DLOG(WARNING) << "Failed to send action status to __status__ node.";
    }
    writer.FlushCurrentBuffer();
  } else if (resources.stream() != nullptr) {
    const std::string status_node_id = output_name_to_id_["__status__"];

    NodeFragment fragment;
    fragment.id = status_node_id;
    fragment.continued = false;
    fragment.data = *ConvertTo<Chunk>(status);
    fragment.seq = 0;

    resources.stream()
        ->SendWithoutBuffering(
            WireMessage{.node_fragments = {std::move(fragment)}})
        .IgnoreError();
  }
}

std::string Action::MakeNodeId(std::string_view action_id,
                               std::string_view node_name) {
  return absl::StrCat(action_id, "#", node_name);
}

Action::Action(std::string_view id) : id_(id) {}

Action::~Action() {
  if (Session* session = bound_resources_.session(); session != nullptr) {
    session->ExtractAction(id_).IgnoreError();
  }
}

absl::Status Action::Run(absl::Duration timeout) {
  RETURN_IF_ERROR(RunInBackground());
  RETURN_IF_ERROR(Await(timeout));
  return absl::OkStatus();
}

absl::Status Action::RunInBackground(bool detach) {
  if (ctx_.HasBeenCalled()) {
    return absl::FailedPreconditionError(
        "Cannot run action: it has already been called.");
  }
  if (ctx_.HasBeenRun()) {
    return absl::FailedPreconditionError(
        "Cannot run action: it has already been run.");
  }
  if (handler_ == nullptr) {
    return absl::FailedPreconditionError(
        "Cannot run action: no handler is set.");
  }

  if (id_.empty()) {
    return absl::FailedPreconditionError(
        "Cannot run action: no action id is set.");
  }

  settings_.bind_streams_on_inputs_by_default = false;
  settings_.bind_streams_on_outputs_by_default = true;
  {
    // hacky synchronization to ensure we don't run the fiber
    // if the context is already running or calling something.
    // Compared to the opportunistic checks above, these are STRICT.
    act::MutexLock lock(&ctx_.mu_);
    if (ctx_.HasBeenRun_()) {
      return absl::FailedPreconditionError(
          "Cannot run action: it has already been run.");
    }
    if (ctx_.HasBeenCalled_()) {
      return absl::FailedPreconditionError(
          "Cannot run action: it has already been called.");
    }
    ctx_.detached_ = detach;
    ctx_.progress_state_ = ActionRunState{
        .fiber = thread::NewTree({}, [this]() {
          const std::shared_ptr<Action> action = shared_from_this();
          ctx_.RunHandlerWithPreparationAndCleanup(std::move(handler_), action);
        })};
  }

  return absl::OkStatus();
}

absl::Status Action::Call(
    absl::flat_hash_map<std::string, std::string> wire_message_headers) {
  if (ctx_.HasBeenCalled()) {
    return absl::FailedPreconditionError(
        "Cannot call action: it has already been called.");
  }
  if (ctx_.HasBeenRun()) {
    return absl::FailedPreconditionError(
        "Cannot call action: it has already been run.");
  }

  if (bound_resources_.stream() == nullptr) {
    return absl::FailedPreconditionError(
        "Cannot call action: no wire stream is bound to the action.");
  }

  if (Session* absl_nullable session = bound_resources_.session();
      session != nullptr) {
    if (!weak_from_this().lock()) {
      return absl::FailedPreconditionError(
          "Cannot call action: the action has no shared pointer, which is "
          "required to safely bind it to a session.");
    }
  }

  settings_.bind_streams_on_inputs_by_default = true;
  settings_.bind_streams_on_outputs_by_default = false;

  ASSIGN_OR_RETURN(ActionMessage message, GetActionMessage());

  {
    act::MutexLock lock(&ctx_.mu_);
    if (ctx_.HasBeenCalled_()) {
      return absl::FailedPreconditionError(
          "Cannot call action: it has already been called.");
    }
    if (ctx_.HasBeenRun_()) {
      return absl::FailedPreconditionError(
          "Cannot call action: it has already been run.");
    }
    if (ctx_.cancelled_) {
      return absl::CancelledError("Cannot call action: it has been cancelled.");
    }
    if (Session* session = bound_resources_.session(); session != nullptr) {
      RETURN_IF_ERROR(session->RecordActionCall(id_, shared_from_this()));
    }
    RETURN_IF_ERROR(bound_resources_.stream()->Send(
        WireMessage{.actions = {std::move(message)},
                    .headers = std::move(wire_message_headers)}));
  }

  RETURN_IF_ERROR(ctx_.RecordCall(ActionCallState{}));
  return absl::OkStatus();
}

absl::StatusOr<thread::Case> Action::OnDone() const {
  return ctx_.OnDone();
}

bool Action::IsDone() const {
  return ctx_.IsDone();
}

bool Action::HasBeenRun() const {
  return ctx_.HasBeenRun();
}

bool Action::HasBeenCalled() const {
  return ctx_.HasBeenCalled();
}

bool Action::HasBeenCancelled() const {
  return ctx_.HasBeenCancelled();
}

absl::Status Action::SetDispatchStatus(absl::Status status) {
  RETURN_IF_ERROR(ctx_.SetDispatchStatus(status));
  return ctx_.SetDispatchStatus(status);
}

absl::Status Action::SetCompletionStatus(absl::Status status) {
  return ctx_.SetCompletionStatus(status);
}

absl::Status Action::Cancel() {
  return ctx_.Cancel();
}

absl::Status Action::Await(absl::Duration timeout,
                           bool* absl_nullable timeout_exceeded) {
  if (bound_resources_.session() == nullptr && ctx_.HasBeenCalled()) {
    return absl::FailedPreconditionError(
        "Awaiting called actions is only possible if they are called in a "
        "session. Consider manually reading action[\"__status__\"]");
  }
  absl::Status exec_status;

  if (absl::Status await_status =
          ctx_.Await(&exec_status, absl::Now() + timeout);
      !await_status.ok()) {
    if (timeout_exceeded != nullptr) {
      *timeout_exceeded = true;
    }
    return await_status;
  }

  if (timeout_exceeded != nullptr) {
    *timeout_exceeded = false;
  }
  return exec_status;
}

absl::StatusOr<ActionMessage> Action::GetActionMessage() const {
  if (!schema_) {
    return absl::FailedPreconditionError(
        "Cannot get action message: no schema is set.");
  }

  std::vector<Port> inputs;
  std::vector<Port> outputs;
  {
    act::MutexLock lock(&ctx_.mu_);
    inputs.reserve(ctx_.input_name_to_id_.size());
    for (const auto& [input_name, input_id] : ctx_.input_name_to_id_) {
      inputs.push_back({.name = input_name, .id = input_id});
    }
    outputs.reserve(ctx_.output_name_to_id_.size());
    for (const auto& [output_name, output_id] : ctx_.output_name_to_id_) {
      outputs.push_back({.name = output_name, .id = output_id});
    }
  }
  return ActionMessage{.id = id_,
                       .name = schema_->name,
                       .inputs = inputs,
                       .outputs = outputs,
                       .headers = headers_};
}

absl::Status Action::MapPortsFromMessage(const ActionMessage& message) {
  if (!schema_) {
    return absl::FailedPreconditionError(
        "Cannot map ports from message: no schema is set.");
  }
  for (const Port& input : message.inputs) {
    if (!schema_->HasInput(input.name)) {
      return absl::FailedPreconditionError(
          absl::StrCat("Cannot map ports from message: input port `",
                       input.name, "` not found in schema."));
    }
  }
  for (const Port& output : message.outputs) {
    if (!schema_->HasOutput(output.name)) {
      return absl::FailedPreconditionError(
          absl::StrCat("Cannot map ports from message: output port `",
                       output.name, "` not found in schema."));
    }
  }
  for (const Port& input : message.inputs) {
    ctx_.input_name_to_id_[input.name] = input.id;
  }
  for (const Port& output : message.outputs) {
    ctx_.output_name_to_id_[output.name] = output.id;
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<Action>> Action::MakeActionInSameSession(
    std::string_view name, std::string_view action_id) const {
  auto action =
      std::make_unique<Action>(action_id.empty() ? GenerateUUID4() : action_id);

  const ActionRegistry* absl_nullable registry = bound_resources_.registry();
  if (registry == nullptr) {
    return absl::FailedPreconditionError(absl::StrCat(
        "Cannot make action in same session: no registry is bound."));
  }
  if (!registry->IsRegistered(name)) {
    return absl::FailedPreconditionError(
        absl::StrCat("Cannot make action in same session: action `", name,
                     "` is not registered."));
  }

  *action->mutable_bound_resources() =
      ActionBoundResources::SameAs(bound_resources_);
  ASSIGN_OR_RETURN(const ActionSchema& schema, registry->GetSchema(name));
  ASSIGN_OR_RETURN(const ActionHandler& handler, registry->GetHandler(name));
  action->set_handler(handler);
  action->set_schema(schema);
  return action;
}

AsyncNode* Action::GetInput(std::string_view name,
                            std::optional<bool> bind_stream) {
  if (!bound_resources_.node_map()) {
    return nullptr;
  }
  const auto id_it = ctx_.input_name_to_id_.find(name);
  if (id_it == ctx_.input_name_to_id_.end()) {
    return nullptr;
  }
  AsyncNode* absl_nonnull node =
      bound_resources_.node_map()->Get(id_it->second);
  if (WireStream* stream = bound_resources_.stream();
      stream != nullptr &&
      bind_stream.value_or(settings_.bind_streams_on_inputs_by_default)) {
    node->GetWriter().BindPeers({{stream->GetId(), stream}});
  }
  return node;
}

AsyncNode* Action::GetOutput(std::string_view name,
                             const std::optional<bool> bind_stream) {
  // TODO: keep track of nodes with bound streams
  if (!bound_resources_.node_map()) {
    return nullptr;
  }
  const auto id_it = ctx_.output_name_to_id_.find(name);
  if (id_it == ctx_.output_name_to_id_.end()) {
    return nullptr;
  }

  AsyncNode* absl_nonnull node =
      bound_resources_.node_map()->Get(id_it->second);

  if (WireStream* stream = bound_resources_.stream();
      stream != nullptr &&
      bind_stream.value_or(settings_.bind_streams_on_outputs_by_default)) {
    node->GetWriter().BindPeers({{stream->GetId(), stream}});
  }
  return node;
}

void Action::SetOnCancelled(absl::AnyInvocable<void()> callback) {
  ctx_.on_cancelled_ = std::move(callback);
}

ActionSchema Action::schema() const {
  return schema_.value_or(ActionSchema{});
}

bool Action::has_schema() const {
  return schema_.has_value();
}

void Action::set_schema(ActionSchema schema) {
  LogWarningIfChangedAfterExec("schema");

  schema_ = std::move(schema);
  {
    act::MutexLock lock(&ctx_.mu_);
    DCHECK(!ctx_.HasBeenRun_() && !ctx_.HasBeenCalled_());

    ctx_.input_name_to_id_.clear();
    ctx_.output_name_to_id_.clear();
    if (!schema_) {
      return;
    }

    ctx_.input_name_to_id_.reserve(schema_->inputs.size());
    ctx_.output_name_to_id_.reserve(schema_->outputs.size());

    for (const auto& [input_name, _] : schema_->inputs) {
      ctx_.input_name_to_id_[input_name] = Action::MakeNodeId(id_, input_name);
    }
    for (const auto& [output_name, _] : schema_->outputs) {
      ctx_.output_name_to_id_[output_name] =
          Action::MakeNodeId(id_, output_name);
    }
    ctx_.output_name_to_id_["__status__"] =
        Action::MakeNodeId(id_, "__status__");
    ctx_.output_name_to_id_["__dispatch_status__"] =
        Action::MakeNodeId(id_, "__dispatch_status__");
  }
}

ActionHandler Action::handler() const {
  if (!has_handler()) {
    return nullptr;
  }
  return handler_;
}

bool Action::has_handler() const {
  return handler_ != nullptr;
}

void Action::set_handler(ActionHandler handler) {
  handler_ = std::move(handler);
}

const std::string& Action::id() const {
  return id_;
}

void Action::set_id(std::string_view id) {
  id_ = id;
}

const absl::flat_hash_map<std::string, std::string>& Action::headers() const {
  return headers_;
}

std::optional<std::string> Action::get_header(std::string_view key) const {
  if (const auto it = headers_.find(key); it != headers_.end()) {
    return it->second;
  }
  return std::nullopt;
}

bool Action::has_header(std::string_view key) const {
  return headers_.contains(key);
}

void Action::set_header(std::string_view key, std::string_view value) {
  LogWarningIfChangedAfterExec("headers");
  headers_[key] = value;
}

void Action::remove_header(std::string_view key) {
  LogWarningIfChangedAfterExec("headers");
  headers_.erase(key);
}

const ActionBoundResources& Action::bound_resources() const {
  return bound_resources_;
}

ActionBoundResources* Action::mutable_bound_resources() {
  LogWarningIfChangedAfterExec("bound_resources");
  return &bound_resources_;
}

}  // namespace act