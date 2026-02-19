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

#include "actionengine/stores/local_chunk_store.h"

#include <algorithm>
#include <functional>
#include <optional>
#include <utility>

#include <absl/log/check.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_cat.h>
#include <absl/time/time.h>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/types.h"
#include "actionengine/util/map_util.h"
#include "actionengine/util/metrics.h"

namespace act {

LocalChunkStore::LocalChunkStore(std::string_view id) : LocalChunkStore() {
  // setting ID in a _local_ store cannot fail.
  const absl::Status status = LocalChunkStore::SetId(id);
  DCHECK_OK(status);
}

LocalChunkStore::~LocalChunkStore() {
  act::MutexLock lock(&mu_);
  ClosePutsAndAwaitPendingOperations();

  uint64_t total_data_bytes_remaining = 0;
  for (const auto& [_, chunk] : chunks_) {
    total_data_bytes_remaining += chunk.data.size() + chunk.ref.size();
  }
  MetricStore& pmetrics = GetGlobalMetricStore();
  pmetrics
      .AddToIntegerGauge("gauge_local_chunk_store_data_bytes",
                         -static_cast<int64_t>(total_data_bytes_remaining))
      .IgnoreError();
}

void LocalChunkStore::Notify() {
  act::MutexLock lock(&mu_);
  cv_.SignalAll();
}

absl::StatusOr<std::reference_wrapper<const Chunk>> LocalChunkStore::GetRef(
    int64_t seq, absl::Duration timeout) {
  act::MutexLock lock(&mu_);
  if (chunks_.contains(seq)) {
    const Chunk& chunk = chunks_.at(seq);
    return chunk;
  }

  if (no_further_puts_) {
    return absl::FailedPreconditionError(
        "Cannot get chunks after the store has been closed for writes.");
  }

  absl::Status status;
  ++num_pending_ops_;
  while (!chunks_.contains(seq) && !no_further_puts_ && !thread::Cancelled()) {
    if (cv_.WaitWithTimeout(&mu_, timeout)) {
      status = absl::DeadlineExceededError(
          absl::StrCat("Timed out waiting for seq: ", seq));
      break;
    }
    if (thread::Cancelled()) {
      status = absl::CancelledError(
          absl::StrCat("Cancelled waiting for seq: ", seq));
      break;
    }
    if (no_further_puts_ && !chunks_.contains(seq)) {
      status = absl::FailedPreconditionError(
          "Cannot get chunks after the store has been closed.");
      break;
    }
  }
  --num_pending_ops_;

  if (!status.ok()) {
    return status;
  }

  const auto it = chunks_.find(seq);
  // The chunks_ map should always be in sync, so this DCHECK is a sanity check
  // for that invariant in debug builds.
  DCHECK(it != chunks_.end());
  if (it == chunks_.end()) {
    return absl::InternalError(absl::StrCat(
        "Invariant violation: chunks map is missing entry for seq ", seq,
        ", but no previous error, cancellation, timeout or finalisation "
        "happened."));
  }
  return it->second;
}

absl::StatusOr<std::reference_wrapper<const Chunk>>
LocalChunkStore::GetRefByArrivalOrder(int64_t arrival_offset,
                                      absl::Duration timeout) {
  act::MutexLock lock(&mu_);
  if (arrival_order_to_seq_.contains(arrival_offset)) {
    const int64_t seq = arrival_order_to_seq_.at(arrival_offset);

    const auto it = chunks_.find(seq);
    // The two maps should always be in sync, so this DCHECK is a sanity check
    // for that invariant in debug builds.
    DCHECK(it != chunks_.end());
    if (it == chunks_.end()) {
      return absl::InternalError(absl::StrCat(
          "Invariant violation: arrival order to seq map and chunks map are "
          "out of sync for arrival offset ",
          arrival_offset, " and seq ", seq));
    }
    return it->second;
  }

  if (no_further_puts_) {
    return absl::FailedPreconditionError(
        "Cannot get chunks after the store has been closed.");
  }

  absl::Status status;
  ++num_pending_ops_;
  while (!arrival_order_to_seq_.contains(arrival_offset) && !no_further_puts_ &&
         !thread::Cancelled()) {

    if (cv_.WaitWithTimeout(&mu_, timeout)) {
      status = absl::DeadlineExceededError(absl::StrCat(
          "Timed out waiting for arrival offset: ", arrival_offset));
      break;
    }
    if (thread::Cancelled()) {
      status = absl::CancelledError(absl::StrCat(
          "Cancelled waiting for arrival offset: ", arrival_offset));
      break;
    }
    if (no_further_puts_ && !arrival_order_to_seq_.contains(arrival_offset)) {
      status = absl::FailedPreconditionError(
          "Cannot get chunks after the store has been closed.");
      break;
    }
  }
  --num_pending_ops_;

  if (!status.ok()) {
    return status;
  }

  const auto seq_it = arrival_order_to_seq_.find(arrival_offset);
  DCHECK(seq_it != arrival_order_to_seq_.end());
  if (seq_it == arrival_order_to_seq_.end()) {
    return absl::InternalError(absl::StrCat(
        "Invariant violation: arrival order to seq map is missing entry, "
        "but it was just found earlier for arrival offset. ",
        arrival_offset));
  }
  const int64_t seq = seq_it->second;

  const auto chunk_it = chunks_.find(seq);
  DCHECK(chunk_it != chunks_.end());
  if (chunk_it == chunks_.end()) {
    return absl::InternalError(absl::StrCat(
        "Invariant violation: chunks map is missing entry, but it was just "
        "found earlier for seq ",
        seq));
  }
  return chunk_it->second;
}

absl::StatusOr<std::optional<Chunk>> LocalChunkStore::Pop(int64_t seq) {
  act::MutexLock lock(&mu_);
  MetricStore& pmetrics = GetGlobalMetricStore();

  if (const auto map_node = chunks_.extract(seq); map_node) {
    const int64_t arrival_order = seq_to_arrival_order_[seq];
    seq_to_arrival_order_.erase(seq);
    arrival_order_to_seq_.erase(arrival_order);

    pmetrics
        .AddToIntegerGauge("gauge_local_chunk_store_data_bytes",
                           -static_cast<int64_t>(map_node.mapped().data.size() +
                                                 map_node.mapped().ref.size()))
        .IgnoreError();

    return std::move(map_node.mapped());
  }

  return std::nullopt;
}

absl::Status LocalChunkStore::Put(int64_t seq, Chunk chunk, bool final) {
  act::MutexLock lock(&mu_);

  if (no_further_puts_) {
    return absl::FailedPreconditionError(
        "Cannot put chunks after the store has been closed.");
  }

  max_seq_ = std::max(max_seq_, seq);
  final_seq_ = final ? seq : final_seq_;

  arrival_order_to_seq_[total_chunks_put_] = seq;
  seq_to_arrival_order_[seq] = total_chunks_put_;
  chunks_[seq] = std::move(chunk);
  ++total_chunks_put_;

  MetricStore& pmetrics = GetGlobalMetricStore();
  pmetrics
      .AddToIntegerGauge("gauge_local_chunk_store_data_bytes",
                         static_cast<int64_t>(chunks_.at(seq).data.size() +
                                              chunks_.at(seq).ref.size()))
      .IgnoreError();

  cv_.SignalAll();
  return absl::OkStatus();
}

absl::Status LocalChunkStore::CloseWritesWithStatus(absl::Status) {
  act::MutexLock lock(&mu_);

  no_further_puts_ = true;
  if (max_seq_ != -1) {
    final_seq_ = std::min(final_seq_, max_seq_);
  }
  // Notify all waiters because they will not be able to get any more chunks.
  cv_.SignalAll();
  return absl::OkStatus();
}

absl::StatusOr<size_t> LocalChunkStore::Size() {
  act::MutexLock lock(&mu_);
  return chunks_.size();
}

absl::StatusOr<bool> LocalChunkStore::Contains(int64_t seq) {
  act::MutexLock lock(&mu_);
  return chunks_.contains(seq);
}

absl::Status LocalChunkStore::SetId(std::string_view id) {
  id_ = id;
  return absl::OkStatus();
}

std::string_view LocalChunkStore::GetId() const {
  return id_;
}

absl::StatusOr<int64_t> LocalChunkStore::GetSeqForArrivalOffset(
    int64_t arrival_offset) {
  act::MutexLock lock(&mu_);
  if (!arrival_order_to_seq_.contains(arrival_offset)) {
    return -1;
  }
  return arrival_order_to_seq_.at(arrival_offset);
}

absl::StatusOr<int64_t> LocalChunkStore::GetFinalSeq() {
  act::MutexLock lock(&mu_);
  return final_seq_;
}

void LocalChunkStore::ClosePutsAndAwaitPendingOperations() {
  no_further_puts_ = true;
  // Notify all waiters because they will not be able to get any more chunks.
  cv_.SignalAll();

  while (num_pending_ops_ > 0) {
    cv_.Wait(&mu_);
  }
}

}  // namespace act