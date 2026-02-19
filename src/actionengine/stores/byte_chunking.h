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

#ifndef ACTIONENGINE_STORES_BYTE_CHUNKING_H_
#define ACTIONENGINE_STORES_BYTE_CHUNKING_H_

#include <cstddef>
#include <cstdint>
#include <utility>
#include <variant>
#include <vector>

#include <absl/base/thread_annotations.h>
#include <absl/container/inlined_vector.h>
#include <absl/log/check.h>
#include <absl/status/statusor.h>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/msgpack.h"  // IWYU pragma: keep
#include "actionengine/data/types.h"
#include "actionengine/stores/local_chunk_store.h"

namespace act::data {

using Byte = uint8_t;

enum BytePacketType {
  kCompleteBytes = 0x00,
  kByteChunk = 0x01,
  kLengthSuffixedByteChunk = 0x02,
};

struct CompleteBytesPacket {
  static constexpr Byte kType = BytePacketType::kCompleteBytes;

  static uint32_t GetSerializedMetadataSize(uint64_t transient_id);

  std::vector<Byte> serialized_message;
  uint64_t transient_id;
};

struct ByteChunkPacket {
  static constexpr Byte kType = BytePacketType::kByteChunk;

  static uint32_t GetSerializedMetadataSize(uint64_t transient_id,
                                            uint32_t seq);

  std::vector<Byte> chunk;
  uint32_t seq;
  uint64_t transient_id;
};

struct LengthSuffixedByteChunkPacket {
  static constexpr Byte kType = BytePacketType::kLengthSuffixedByteChunk;

  static uint32_t GetSerializedMetadataSize(uint64_t transient_id, uint32_t seq,
                                            uint32_t length);

  std::vector<Byte> chunk;
  uint32_t length;
  uint32_t seq;
  uint64_t transient_id;
};

using BytePacket = std::variant<CompleteBytesPacket, ByteChunkPacket,
                                LengthSuffixedByteChunkPacket>;

BytePacket ProducePacket(std::vector<Byte>::const_iterator it,
                         std::vector<Byte>::const_iterator end,
                         uint64_t transient_id, uint32_t packet_size,
                         uint32_t seq = 0, int32_t length = -1,
                         bool force_no_length = false);

absl::StatusOr<BytePacket> ParseBytePacket(Byte* data, size_t size);

std::vector<BytePacket> SplitBytesIntoPackets(const std::vector<Byte>& data,
                                              uint64_t transient_id,
                                              uint64_t packet_size = 65536);

uint64_t GetTransientIdFromPacket(const BytePacket& packet);

std::vector<Byte> SerializeBytePacket(BytePacket packet);

class ChunkedBytes {
 public:
  absl::StatusOr<std::vector<Byte>> ConsumeCompleteBytes();
  absl::StatusOr<bool> FeedPacket(BytePacket packet);
  absl::StatusOr<bool> FeedSerializedPacket(std::vector<Byte> data);

 private:
  mutable act::Mutex mu_;

  absl::StatusOr<bool> FeedPacketInternal(BytePacket packet)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  absl::StatusOr<bool> FeedSerializedPacketInternal(std::vector<Byte> data)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  LocalChunkStore chunk_store_;
  size_t total_message_size_ ABSL_GUARDED_BY(mu_) = 0;
  uint32_t total_expected_chunks_ ABSL_GUARDED_BY(mu_) = -1;

  // Holdout chunks received before total_expected_chunks_ is known. These are
  // inserted into chunk_store_ once total_expected_chunks_ is set. The vector
  // is inlined to avoid heap allocation for the common case of few holdout
  // chunks.
  absl::InlinedVector<std::pair<uint32_t, Chunk>, 4> holdout_chunks_
      ABSL_GUARDED_BY(mu_);
};

}  // namespace act::data

#endif  // ACTIONENGINE_DATA_BYTE_CHUNKING_H_