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

#include "actionengine/net/stream.h"

namespace act {

absl::Status WireStream::SendWithoutBuffering(WireMessage message) {
  return absl::UnimplementedError(
      "This stream does not support sending without buffering.");
}

absl::Status WireStream::AttachBufferingBehaviour(
    net::WireMessageBufferingBehaviour* sender) {
  return absl::UnimplementedError(
      "This stream does not support attaching a reducing sender.");
}

bool WireStream::HasAttachedBufferingBehaviour() const {
  return false;
}

namespace net {

MergeWireMessagesWhileInScope::MergeWireMessagesWhileInScope(WireStream* stream)
    : stream_(stream) {}

MergeWireMessagesWhileInScope::~MergeWireMessagesWhileInScope() {
  Finalize().IgnoreError();
}

absl::Status MergeWireMessagesWhileInScope::Attach() {
  if (stream_ == nullptr) {
    return absl::OkStatus();
  }
  return stream_->AttachBufferingBehaviour(this);
}

void MergeWireMessagesWhileInScope::NoMoreSends() {
  act::MutexLock lock(&mu_);
  send_allowed_ = false;
}

absl::Status MergeWireMessagesWhileInScope::Send(WireMessage message) {
  act::MutexLock lock(&mu_);
  if (!send_allowed_) {
    return absl::FailedPreconditionError(
        "No more sends allowed on this ReducingWireMessageSender.");
  }
  // Merge the message into the buffer.
  for (auto& fragment : message.node_fragments) {
    buffered_message_.node_fragments.push_back(std::move(fragment));
  }
  for (auto& action : message.actions) {
    buffered_message_.actions.push_back(std::move(action));
  }
  for (auto& [key, value] : message.headers) {
    auto it = buffered_message_.headers.find(key);
    if (it != buffered_message_.headers.end()) {
      // Key already exists, it's not up to us to decide how to merge headers.
      return absl::FailedPreconditionError(
          absl::StrFormat("Header key conflict while merging: %s", key));
    }
    buffered_message_.headers[key] = std::move(value);
  }
  return absl::OkStatus();
}

absl::Status MergeWireMessagesWhileInScope::ForceFlush() {
  act::MutexLock lock(&mu_);
  return ForceFlushInternal();
}

absl::Status MergeWireMessagesWhileInScope::ForceFlushInternal() {
  if (buffered_message_.node_fragments.empty() &&
      buffered_message_.actions.empty()) {
    return absl::OkStatus();
  }
  if (stream_ == nullptr) {
    return absl::OkStatus();
  }
  absl::Status status =
      stream_->SendWithoutBuffering(std::move(buffered_message_));
  buffered_message_ = WireMessage{};
  return status;
}

absl::Status MergeWireMessagesWhileInScope::GetStatus() const {
  if (stream_ == nullptr) {
    return absl::OkStatus();
  }
  return stream_->GetStatus();
}

absl::Status MergeWireMessagesWhileInScope::Finalize() {
  act::MutexLock lock(&mu_);
  send_allowed_ = false;
  absl::Status status;
  status.Update(ForceFlushInternal());
  if (stream_ == nullptr) {
    return status;
  }
  status.Update(stream_->AttachBufferingBehaviour(nullptr));
  return status;
}

WireMessage* MergeWireMessagesWhileInScope::buffered_message() {
  return &buffered_message_;
}

}  // namespace net

}  // namespace act