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

#include "actionengine/service/session.h"

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/status/statusor.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/strip.h>
#include <absl/time/clock.h>

#include "actionengine/actions/action.h"
#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/conversion.h"
#include "actionengine/data/types.h"
#include "actionengine/net/stream.h"
#include "actionengine/nodes/async_node.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/stores/chunk_store.h"

namespace act {

internal::ConnectionCtx::ConnectionCtx(Session* session,
                                       std::shared_ptr<WireStream> stream,
                                       StreamHandler handler,
                                       absl::Duration recv_timeout)
    : session_(session),
      stream_(std::move(stream)),
      status_(std::make_shared<absl::Status>(absl::StatusCode::kOk, "")) {
  DCHECK(stream_ != nullptr);
  fiber_ = thread::NewTree(
      {}, [status = status_, session = session_, stream_ = stream_,
           handler = std::move(handler), recv_timeout]() noexcept {
        *status = handler(stream_, session, recv_timeout);
      });
}

internal::ConnectionCtx::~ConnectionCtx() {
  if (fiber_ != nullptr) {
    fiber_->Cancel();
    fiber_->Join();
  }
  fiber_.reset();
  stream_.reset();
}

void internal::ConnectionCtx::CancelHandler() const {
  if (fiber_ == nullptr) {
    return;
  }
  fiber_->Cancel();
}

absl::Status internal::ConnectionCtx::Join() {
  if (fiber_ == nullptr) {
    return *status_;
  }
  fiber_->Join();
  fiber_.reset();
  stream_.reset();
  return *status_;
}

std::unique_ptr<thread::Fiber> internal::ConnectionCtx::ExtractHandlerFiber() {
  std::unique_ptr<thread::Fiber> fiber = std::move(fiber_);
  fiber_ = nullptr;
  return fiber;
}

bool IsReservedActionName(std::string_view name) {
  static auto* reserved_names = new absl::flat_hash_set<std::string_view>{
      "__ping", "__list_running_actions", "__cancel_action"};
  return reserved_names->contains(name);
}

StreamHandler internal::EnsureHalfClosesOrAbortsStream(StreamHandler handler) {
  return [handler = std::move(handler)](std::shared_ptr<WireStream> stream,
                                        Session* absl_nonnull session,
                                        absl::Duration recv_timeout) {
    absl::Status status;
    try {
      status = handler(stream, session, recv_timeout);
    } catch (const std::exception& e) {
      // Even though the whole Action Engine is noexcept, handlers might
      // not be: external library calls, rogue Python handlers, etc.
      status = absl::InternalError(
          absl::StrCat("Unhandled exception in stream handler: ", e.what()));
    }

    // If already half-closed or aborted (as should be by the handler),
    // these are no-ops.
    DCHECK(stream != nullptr);
    if (status.ok()) {
      stream->HalfClose();
    } else {
      stream->Abort(status);
    }

    return status;
  };
}

absl::Status internal::DefaultStreamHandler(std::shared_ptr<WireStream> stream,
                                            Session* session,
                                            absl::Duration recv_timeout) {
  if (stream == nullptr) {
    return absl::InvalidArgumentError("Stream cannot be null");
  }

  absl::Status status;
  while (!thread::Cancelled()) {
    absl::StatusOr<std::optional<WireMessage>> message =
        stream->Receive(recv_timeout);
    if (!message.ok()) {
      status = message.status();
      stream->Abort(status);
      break;
    }
    if (!message->has_value()) {
      break;
    }
    status =
        session->DispatchWireMessage(std::move(message)->value(), stream.get());
  }

  if (thread::Cancelled()) {
    status = absl::CancelledError("Service is shutting down.");
  }

  return status;
}

static absl::Status ReturnDispatchStatusReportingToCallerStream(
    WireStream* stream, const std::string& action_id, absl::Status status) {
  if (stream == nullptr) {
    return status;
  }
  std::vector fragments = {NodeFragment{
      .id = Action::MakeNodeId(action_id, "__dispatch_status__"),
      .data = ConvertTo<Chunk>(status).value(),
      .seq = 0,
      .continued = false,
  }};
  fragments.reserve(2);

  // If there was an error during dispatch, also report it on the __status__
  // because the caller may already not be listening to __dispatch_status__.
  if (!status.ok()) {
    fragments.push_back(NodeFragment{
        .id = Action::MakeNodeId(action_id, "__status__"),
        .data = ConvertTo<Chunk>(status).value(),
        .seq = 0,
        .continued = false,
    });
  }

  status.Update(
      stream->Send(WireMessage{.node_fragments = std::move(fragments)}));
  return status;
}

Session::~Session() {
  act::MutexLock lock(&mu_);

  finalizing_ = true;

  // First, gracefully cancel all actions: they might still want to write
  // to nodes.
  for (auto& [_, action] : actions_) {
    mu_.unlock();
    if (absl::Status cancel_status = action->Cancel(); !cancel_status.ok()) {
      LOG(WARNING) << "Failed to cancel action " << action->id() << ": "
                   << cancel_status;
    }
    mu_.lock();
  }

  mu_.unlock();
  for (const auto& [_, action] : actions_) {
    action->UnbindSessionInternal();
  }
  mu_.lock();

  for (auto& [_, connection] : connections_) {
    connection->CancelHandler();
    mu_.unlock();
    connection->Join().IgnoreError();
    mu_.lock();
  }
  AwaitAllActions_(absl::Seconds(5)).IgnoreError();
  mu_.unlock();
  actions_.clear();
  mu_.lock();
}

void Session::StartStreamHandler(std::string_view id,
                                 std::shared_ptr<WireStream> stream,
                                 StreamHandler handler,
                                 absl::Duration recv_timeout) {
  act::MutexLock lock(&mu_);
  connections_.emplace(
      id, std::make_unique<internal::ConnectionCtx>(
              this, std::move(stream),
              internal::EnsureHalfClosesOrAbortsStream(std::move(handler)),
              recv_timeout));
}

std::unique_ptr<internal::ConnectionCtx> Session::ExtractStreamHandler(
    std::string_view id) {
  act::MutexLock lock(&mu_);
  const auto map_node = connections_.extract(id);
  if (map_node.empty()) {
    return nullptr;
  }
  return std::move(map_node.mapped());
}

absl::flat_hash_map<std::string, std::unique_ptr<internal::ConnectionCtx>>
Session::ExtractAllStreamHandlers() {
  act::MutexLock lock(&mu_);
  auto extracted_connections = std::move(connections_);
  connections_.clear();
  return extracted_connections;
}

absl::Status Session::DispatchNodeFragment(NodeFragment node_fragment) {
  act::MutexLock lock(&mu_);
  return DispatchNodeFragmentInternal(std::move(node_fragment));
}

absl::Status Session::DispatchActionMessage(ActionMessage action_message,
                                            WireStream* origin_stream) {
  act::MutexLock lock(&mu_);
  return DispatchActionMessageInternal(std::move(action_message),
                                       origin_stream);
}

absl::Status Session::DispatchWireMessage(WireMessage message,
                                          WireStream* origin_stream) {
  act::MutexLock lock(&mu_);
  return DispatchWireMessageInternal(std::move(message), origin_stream);
}

absl::Status Session::AwaitAllActions(absl::Duration timeout) {
  act::MutexLock lock(&mu_);
  return AwaitAllActions_(timeout);
}

void Session::CancelAllActions() {
  act::MutexLock lock(&mu_);
  for (auto& [_, action] : actions_) {
    action->Cancel().IgnoreError();
  }
}

void Session::CancelAllConnections() {
  act::MutexLock lock(&mu_);
  for (auto& [_, connection] : connections_) {
    connection->CancelHandler();
  }
}

absl::Status Session::RecordActionCall(std::string_view id,
                                       std::shared_ptr<Action> action) {
  act::MutexLock lock(&mu_);
  if (actions_.contains(id)) {
    return absl::AlreadyExistsError(absl::StrCat(
        "Action with id ", id, " already exists in this session."));
  }
  if (finalizing_) {
    return absl::FailedPreconditionError(
        "Session is being finalized, no more dispatch.");
  }
  actions_.emplace(id, std::move(action));
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<Action>> Session::ExtractAction(
    std::string_view id) {
  act::MutexLock lock(&mu_);
  return ExtractAction_(id);
}

absl::StatusOr<std::shared_ptr<Action>> Session::ExtractAction_(
    std::string_view id) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (finalizing_) {
    return absl::FailedPreconditionError(
        "Session is being finalized, actions are being purged, so reentrant "
        "access is not allowed.");
  }
  const auto action_it = actions_.extract(id);
  if (action_it.empty()) {
    return absl::NotFoundError(
        absl::StrCat("Action with id ", id, " not found."));
  }
  return std::move(action_it.mapped());
}

absl::Status Session::AwaitAllActions_(absl::Duration timeout) {
  std::vector<std::shared_ptr<Action>> actions;
  actions.reserve(actions_.size());
  for (auto& [_, action] : actions_) {
    actions.push_back(action);
  }
  thread::CaseArray done_cases;
  done_cases.reserve(actions.size());
  for (const auto& action : actions) {
    absl::StatusOr<thread::Case> done_case_or_status = action->OnDone();
    if (!done_case_or_status.ok()) {
      continue;
    }
    done_cases.push_back(*std::move(done_case_or_status));
  }
  size_t num_actions_done = 0;
  const absl::Time deadline = absl::Now() + timeout;
  while (absl::Now() < deadline && num_actions_done < actions.size()) {
    mu_.unlock();
    const int selected = thread::SelectUntil(deadline, done_cases);
    mu_.lock();
    if (selected == -1) {
      break;
    }
    // mu_.unlock();
    // if (absl::Status status = actions[selected]->Await(deadline - absl::Now());
    //     !status.ok()) {
    //   LOG(WARNING) << "Action " << actions[selected]->id()
    //                << " failed: " << status;
    // }
    // mu_.lock();
    done_cases[selected] = thread::NonSelectableCase();
    ++num_actions_done;
  }
  if (absl::Now() >= deadline && num_actions_done < done_cases.size()) {
    return absl::DeadlineExceededError(
        absl::StrCat("Timed out waiting for ",
                     actions.size() - num_actions_done, " actions to finish."));
  }
  return absl::OkStatus();
}

size_t Session::GetNumActiveConnections() const {
  act::MutexLock lock(&mu_);
  return connections_.size();
}

AsyncNode* Session::GetNode(std::string_view id,
                            const ChunkStoreFactory& factory) const {
  act::MutexLock lock(&mu_);
  if (!node_map_) {
    return nullptr;
  }
  return node_map_->Get(id, factory);
}

NodeMap* Session::node_map() const {
  act::MutexLock lock(&mu_);
  return node_map_;
}

void Session::set_node_map(NodeMap* absl_nullable node_map) {
  act::MutexLock lock(&mu_);
  if (node_map_ != nullptr) {
    node_map_->FlushAllWriters();
  }
  node_map_ = node_map;
}

ActionRegistry* Session::action_registry() {
  act::MutexLock lock(&mu_);
  if (!action_registry_) {
    return nullptr;
  }
  return &*action_registry_;
}

void Session::set_action_registry(
    std::optional<ActionRegistry> action_registry) {
  act::MutexLock lock(&mu_);
  action_registry_ = std::move(action_registry);
}

absl::Status Session::DispatchNodeFragmentInternal(NodeFragment node_fragment) {
  if (!node_map_) {
    return absl::FailedPreconditionError("Node map not set.");
  }

  if (finalizing_) {
    return absl::FailedPreconditionError(
        "Session is being finalized, no more dispatch.");
  }

  RETURN_IF_ERROR(node_map_->Get(node_fragment.id)->Put(node_fragment));

  if (absl::EndsWith(node_fragment.id, "#__dispatch_status__")) {
    const std::string_view action_id =
        absl::StripSuffix(node_fragment.id, "#__dispatch_status__");
    const auto action_it = actions_.find(action_id);
    if (action_it == actions_.end()) {
      return absl::NotFoundError(absl::StrCat(
          "Received dispatch status for unknown action ", action_id));
    }
    ASSIGN_OR_RETURN(Chunk & chunk, node_fragment.GetChunk());
    ASSIGN_OR_RETURN(const auto action_status, ConvertTo<absl::Status>(chunk));
    RETURN_IF_ERROR(action_it->second->SetDispatchStatus(action_status));
    if (!action_status.ok()) {
      ExtractAction_(action_id).IgnoreError();
    }
  }

  if (absl::EndsWith(node_fragment.id, "#__status__")) {
    const std::string_view action_id =
        absl::StripSuffix(node_fragment.id, "#__status__");
    const auto action_it = actions_.find(action_id);
    if (action_it == actions_.end()) {
      return absl::NotFoundError(
          absl::StrCat("Received status for unknown action ", action_id));
    }
    ASSIGN_OR_RETURN(Chunk & chunk, node_fragment.GetChunk());
    ASSIGN_OR_RETURN(const auto action_status, ConvertTo<absl::Status>(chunk));
    RETURN_IF_ERROR(action_it->second->SetCompletionStatus(action_status));
    if (!action_status.ok()) {
      ExtractAction_(action_id).IgnoreError();
    }
  }

  return absl::OkStatus();
}

absl::Status Session::DispatchReservedActionInternal(
    ActionMessage action_message, WireStream* origin_stream) {
  return absl::UnimplementedError(absl::StrCat(
      "Reserved actions are not implemented. Tried ", action_message.name));
}

absl::Status Session::DispatchActionMessageInternal(
    ActionMessage action_message, WireStream* origin_stream) {
  if (finalizing_) {
    return absl::FailedPreconditionError(
        "Session is being finalized, no more dispatch.");
  }
  if (IsReservedActionName(action_message.name)) {
    // Reserved actions might use custom logic dealing with dispatch statuses,
    // thus raw return:
    return DispatchReservedActionInternal(std::move(action_message));
  }
  if (actions_.contains(action_message.id)) {
    return ReturnDispatchStatusReportingToCallerStream(
        origin_stream, action_message.id,
        absl::AlreadyExistsError(
            absl::StrCat("Action with id ", action_message.id,
                         " already exists in this session.")));
  }

  if (!action_registry_) {
    return ReturnDispatchStatusReportingToCallerStream(
        origin_stream, action_message.id,
        absl::FailedPreconditionError("Action registry not set."));
  }

  if (!action_registry_->IsRegistered(action_message.name)) {
    return ReturnDispatchStatusReportingToCallerStream(
        origin_stream, action_message.id,
        absl::NotFoundError(absl::StrCat(
            "Action not found in registry: ", action_message.name, ".")));
  }

  absl::StatusOr<std::unique_ptr<Action>> action_or_status =
      action_registry_->MakeAction(action_message.name, action_message.id);
  if (!action_or_status.ok()) {
    return ReturnDispatchStatusReportingToCallerStream(
        origin_stream, action_message.id, action_or_status.status());
  }

  std::unique_ptr<Action> action = *std::move(action_or_status);
  action->mutable_bound_resources()->set_node_map_non_owning(node_map_);
  action->mutable_bound_resources()->set_session_non_owning(this);
  action->mutable_bound_resources()->set_stream_non_owning(origin_stream);

  // The session class is intended to represent a session where there is
  // another party involved. In this case, we want to clear inputs and outputs
  // after the action is run, because they will already have been sent to the
  // other party, and we don't want to keep them around locally.
  action->mutable_settings()->clear_inputs_after_run = true;
  action->mutable_settings()->clear_outputs_after_run = true;

  auto [it, inserted] = actions_.insert({action->id(), std::move(action)});
  DCHECK(inserted);
  const absl::Status run_in_background_status =
      it->second->RunInBackground(/*detach=*/true);
  if (!run_in_background_status.ok()) {
    actions_.erase(it);
    return ReturnDispatchStatusReportingToCallerStream(
        origin_stream, action_message.id, run_in_background_status);
  }

  return ReturnDispatchStatusReportingToCallerStream(
      origin_stream, action_message.id, absl::OkStatus());
}

absl::Status Session::DispatchWireMessageInternal(WireMessage message,
                                                  WireStream* origin_stream) {
  if (finalizing_) {
    return absl::FailedPreconditionError(
        "Session is being finalized, no more dispatch.");
  }

  std::vector<std::string> error_messages;

  for (auto& node_fragment : message.node_fragments) {
    std::string node_id = node_fragment.id;
    if (absl::Status status =
            DispatchNodeFragmentInternal(std::move(node_fragment));
        !status.ok()) {
      error_messages.push_back(
          absl::StrCat("node fragment ", node_id, ": ", status));
    }
  }

  for (auto& action_message : message.actions) {
    std::string action_name = action_message.name;
    if (absl::Status status = DispatchActionMessageInternal(
            std::move(action_message), origin_stream);
        !status.ok()) {
      error_messages.push_back(
          absl::StrCat("action ", action_name, ": ", status));
    }
  }

  if (!error_messages.empty()) {
    return absl::InvalidArgumentError(absl::StrJoin(error_messages, "\n"));
  }
  return absl::OkStatus();
}

}  // namespace act
