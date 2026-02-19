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

#include "actionengine/service/service.h"

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <absl/base/optimization.h>
#include <absl/log/log.h>
#include <absl/strings/str_cat.h>
#include <absl/time/time.h>

#include "actionengine/actions/action.h"
#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/types.h"
#include "actionengine/net/stream.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/service/session.h"
#include "actionengine/util/map_util.h"

namespace act {

Service::Service(ActionRegistry* absl_nullable action_registry,
                 StreamHandler connection_handler,
                 ChunkStoreFactory chunk_store_factory)
    : action_registry_(
          action_registry == nullptr
              ? nullptr
              : std::make_unique<ActionRegistry>(*action_registry)),
      connection_handler_(std::move(connection_handler)),
      chunk_store_factory_(std::move(chunk_store_factory)) {}

Service::~Service() {
  act::MutexLock lock(&mu_);
  allow_new_connections_ = false;
  DLOG(INFO) << "Shutting down service.";

  for (auto& [_, session] : sessions_) {
    session->CancelAllActions();
  }

  bool abort_streams = false;
  absl::Time deadline = absl::Now() + absl::Seconds(10);
  for (auto& [_, session] : sessions_) {
    if (!session->AwaitAllActions(deadline - absl::Now()).ok()) {
      abort_streams = true;
      break;
    }
  }

  if (abort_streams) {
    LOG(INFO) << "Aborting streams after 10 seconds of waiting for cancelled "
                 "connections to finish.";
    for (auto& [_, stream] : streams_) {
      stream->Abort(absl::DeadlineExceededError("Service shutting down."));
    }
  }
  mu_.unlock();
  sessions_.clear();
  node_maps_.clear();
  mu_.lock();
}

WireStream* absl_nullable Service::GetStream(std::string_view stream_id) const {
  act::MutexLock lock(&mu_);
  const auto it = streams_.find(stream_id);
  if (it == streams_.end()) {
    return nullptr;
  }
  return it->second.get();
}

Session* absl_nullable Service::GetSession(std::string_view session_id) const {
  act::MutexLock lock(&mu_);
  if (sessions_.contains(session_id)) {
    return sessions_.at(session_id).get();
  }
  return nullptr;
}

std::vector<std::string> Service::GetSessionKeys() const {
  act::MutexLock lock(&mu_);
  std::vector<std::string> keys;
  keys.reserve(sessions_.size());
  for (const auto& [key, _] : sessions_) {
    keys.push_back(key);
  }
  return keys;
}

absl::Status Service::StartStreamHandler(std::shared_ptr<WireStream> stream,
                                         StreamHandler connection_handler) {
  act::MutexLock lock(&mu_);

  if (stream == nullptr) {
    return absl::InvalidArgumentError("Provided stream is null.");
  }

  std::string stream_id = stream->GetId();
  if (stream_id.empty()) {
    return absl::InvalidArgumentError("Provided stream has no id.");
  }

  const auto& session_id = stream_id;
  if (session_id.empty()) {
    return absl::InvalidArgumentError("Provided stream has no session id.");
  }

  if (!allow_new_connections_) {
    return absl::FailedPreconditionError(
        "Service does not allow new connections.");
  }

  if (streams_.contains(stream_id)) {
    return absl::AlreadyExistsError(
        absl::StrCat("Stream ", stream_id, " is already connected."));
  }

  WireStream* absl_nonnull stream_ptr = stream.get();
  streams_.emplace(stream_id, std::move(stream));

  if (!sessions_.contains(session_id)) {
    node_maps_.emplace(session_id,
                       std::make_unique<NodeMap>(chunk_store_factory_));
    auto session = std::make_unique<Session>();
    session->set_node_map(node_maps_.at(session_id).get());
    session->set_action_registry(*action_registry_);
    sessions_.emplace(session_id, std::move(session));
  }

  stream_to_session_[stream_id] = session_id;

  StreamHandler resolved_handler = std::move(connection_handler);
  if (resolved_handler == nullptr) {
    resolved_handler = connection_handler_;
  }
  if (resolved_handler == nullptr) {
    LOG(FATAL)
        << "no connection handler provided, and no default handler is set.";
    ABSL_ASSUME(false);
  }
  resolved_handler = EnsureCleanupOnDone(std::move(resolved_handler));

  // for later: Stubby streams require Accept() to be called before returning
  // from StartSession. This might not be the ideal solution with other streams.
  RETURN_IF_ERROR(stream_ptr->Accept());

  sessions_.at(session_id)
      ->StartStreamHandler(stream_id, streams_.at(stream_id),
                           std::move(resolved_handler));

  return absl::OkStatus();
}

void Service::SetActionRegistry(const ActionRegistry& action_registry) const {
  act::MutexLock lock(&mu_);
  *action_registry_ = action_registry;
}

StreamHandler Service::EnsureCleanupOnDone(StreamHandler handler) {
  return [this, handler = std::move(handler)](
             std::shared_ptr<WireStream> stream, Session* absl_nonnull session,
             absl::Duration recv_timeout) {
    auto status = internal::EnsureHalfClosesOrAbortsStream(handler)(
        stream, session, recv_timeout);

    const std::string stream_id = stream->GetId();

    act::MutexLock lock(&mu_);

    const std::unique_ptr<internal::ConnectionCtx> ctx =
        session->ExtractStreamHandler(stream_id);
    if (ctx != nullptr) {
      std::unique_ptr<thread::Fiber> handler_fiber = ctx->ExtractHandlerFiber();
      if (handler_fiber != nullptr) {
        // handler_fiber should just be the current fiber
        DCHECK(handler_fiber.get() == thread::Fiber::Current());
        thread::Detach(std::move(handler_fiber));
      }
    }

    if (session->GetNumActiveConnections() == 0 && allow_new_connections_) {
      auto node_map_it = node_maps_.find(stream_to_session_.at(stream_id));
      DCHECK(node_map_it != node_maps_.end());
      node_map_it->second->FlushAllWriters();
      node_map_it->second->CancelAllReaders();
      sessions_.erase(stream_to_session_.at(stream_id));
      node_maps_.erase(stream_to_session_.at(stream_id));
    }

    stream_to_session_.erase(stream_id);
    streams_.erase(stream_id);

    return status;
  };
}

}  // namespace act
