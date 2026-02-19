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

#include "actionengine/data/msgpack.h"

#include <cstdint>
#include <string>
#include <vector>

#include "actionengine/data/types.h"
#include "cppack/msgpack.h"

namespace cppack {

namespace internal {

void PackStrToStrAbslFlatHashMap(
    const absl::flat_hash_map<std::string, std::string>& map, Packer& packer) {
  const int64_t map_size = map.size();
  packer(map_size);
  for (const auto& [key, value] : map) {
    packer(key);
    packer(value);
  }
}

absl::Status UnpackStrToStrAbslFlatHashMap(
    absl::flat_hash_map<std::string, std::string>& map, Unpacker& unpacker) {
  int64_t map_size;
  unpacker(map_size);
  if (map_size < 0) {
    return absl::InvalidArgumentError("Negative map size in AbslFlatHashMap");
  }
  for (int64_t i = 0; i < map_size; ++i) {
    std::string key;
    std::string value;
    unpacker(key);
    unpacker(value);
    map[std::move(key)] = std::move(value);
  }
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking AbslFlatHashMap from bytes.");
  }
  return absl::OkStatus();
}

}  // namespace internal

absl::Status CppackToBytes(const absl::Status& status, Packer& packer) {
  packer(status.raw_code());
  packer(std::string(status.message()));
  return absl::OkStatus();
}

absl::Status CppackFromBytes(absl::Status& status, Unpacker& unpacker) {
  int code;
  std::string message;
  unpacker(code);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking absl::Status from bytes (.code).");
  }
  unpacker(message);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking absl::Status from bytes (.message).");
  }
  status = absl::Status(static_cast<absl::StatusCode>(code), message);
  return absl::OkStatus();
}

absl::Status CppackToBytes(absl::Time obj, Packer& packer) {
  const int64_t time = absl::ToUnixMicros(obj);
  packer(time);
  return absl::OkStatus();
}

absl::Status CppackFromBytes(absl::Time& obj, Unpacker& unpacker) {
  int64_t time;
  unpacker(time);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError("Error unpacking absl::Time from bytes.");
  }
  obj = absl::FromUnixMicros(time);
  return absl::OkStatus();
}

absl::Status CppackToBytes(const act::ChunkMetadata& obj, Packer& packer) {
  packer(obj.mimetype);
  packer(obj.timestamp);

  internal::PackStrToStrAbslFlatHashMap(obj.attributes, packer);

  return absl::OkStatus();
}

absl::Status CppackFromBytes(act::ChunkMetadata& obj, Unpacker& unpacker) {
  unpacker(obj.mimetype);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking ChunkMetadata from bytes (.mimetype).");
  }
  unpacker(obj.timestamp);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking ChunkMetadata from bytes (.timestamp).");
  }

  return internal::UnpackStrToStrAbslFlatHashMap(obj.attributes, unpacker);
}

absl::Status CppackToBytes(const act::Chunk& obj, Packer& packer) {
  const std::vector<uint8_t> data(obj.data.begin(), obj.data.end());
  packer(data);
  packer(obj.ref);
  if (!obj.metadata) {
    const std::optional<act::ChunkMetadata> empty_metadata;
    packer(empty_metadata);
  } else {
    packer(obj.metadata.value());
  }
  return absl::OkStatus();
}

absl::Status CppackFromBytes(act::Chunk& obj, Unpacker& unpacker) {
  std::vector<uint8_t> data;
  unpacker(data);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking Chunk from bytes (.data).");
  }
  obj.data = std::string(data.begin(), data.end());
  unpacker(obj.ref);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking Chunk from bytes (.ref).");
  }
  unpacker(obj.metadata);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking Chunk from bytes (.metadata).");
  }
  return absl::OkStatus();
}

absl::Status CppackToBytes(const act::NodeRef& obj, Packer& packer) {
  packer(obj.id);
  packer(obj.offset);
  packer(obj.length);
  return absl::OkStatus();
}

absl::Status CppackFromBytes(act::NodeRef& obj, Unpacker& unpacker) {
  unpacker(obj.id);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking NodeRef from bytes (.id).");
  }
  unpacker(obj.offset);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking NodeRef from bytes (.offset).");
  }
  unpacker(obj.length);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking NodeRef from bytes (.length).");
  }
  return absl::OkStatus();
}

absl::Status CppackToBytes(const act::NodeFragment& obj, Packer& packer) {
  uint8_t data_variant_index = 0;

  if (std::holds_alternative<act::Chunk>(obj.data)) {
    data_variant_index = 0;
    packer(data_variant_index);
    packer(std::get<act::Chunk>(obj.data));
  } else if (std::holds_alternative<act::NodeRef>(obj.data)) {
    data_variant_index = 1;
    packer(data_variant_index);
    packer(std::get<act::NodeRef>(obj.data));
  } else {
    return absl::InvalidArgumentError(
        "NodeFragment data must be either Chunk or NodeRef.");
  }
  packer(obj.continued);
  packer(obj.id);
  packer(obj.seq);
  return absl::OkStatus();
}

absl::Status CppackFromBytes(act::NodeFragment& obj, Unpacker& unpacker) {
  uint8_t data_variant_index;
  unpacker(data_variant_index);
  if (data_variant_index == 0) {
    act::Chunk chunk;
    unpacker(chunk);
    if (unpacker.GetErrorCode()) {
      return absl::InvalidArgumentError(
          "Error unpacking NodeFragment Chunk data from bytes.");
    }
    obj.data = std::move(chunk);
  } else if (data_variant_index == 1) {
    act::NodeRef node_ref;
    unpacker(node_ref);
    if (unpacker.GetErrorCode()) {
      return absl::InvalidArgumentError(
          "Error unpacking NodeFragment NodeRef data from bytes.");
    }
    obj.data = std::move(node_ref);
  } else {
    return absl::InvalidArgumentError(
        absl::StrFormat("NodeFragment data must be either Chunk or NodeRef, "
                        "got index %d.",
                        data_variant_index));
  }
  unpacker(obj.continued);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking NodeFragment from bytes (.continued).");
  }
  unpacker(obj.id);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking NodeFragment from bytes (.id).");
  }
  unpacker(obj.seq);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking NodeFragment from bytes (.seq).");
  }
  return absl::OkStatus();
}

absl::Status CppackToBytes(const act::Port& obj, Packer& packer) {
  packer(obj.name);
  packer(obj.id);
  return absl::OkStatus();
}

absl::Status CppackFromBytes(act::Port& obj, Unpacker& unpacker) {
  unpacker(obj.name);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking Port from bytes (.name).");
  }
  unpacker(obj.id);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError("Error unpacking Port from bytes (.id).");
  }
  return absl::OkStatus();
}

absl::Status CppackToBytes(const act::ActionMessage& obj, Packer& packer) {
  packer(obj.id);
  packer(obj.name);
  packer(obj.inputs);
  packer(obj.outputs);
  internal::PackStrToStrAbslFlatHashMap(obj.headers, packer);
  return absl::OkStatus();
}

absl::Status CppackFromBytes(act::ActionMessage& obj, Unpacker& unpacker) {
  unpacker(obj.id);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking ActionMessage from bytes (.id).");
  }
  unpacker(obj.name);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking ActionMessage from bytes (.name).");
  }
  unpacker(obj.inputs);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking ActionMessage from bytes (.inputs).");
  }
  unpacker(obj.outputs);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking ActionMessage from bytes (.outputs).");
  }
  absl::Status status =
      internal::UnpackStrToStrAbslFlatHashMap(obj.headers, unpacker);
  if (!status.ok()) {
    return status;
  }

  return absl::OkStatus();
}

absl::Status CppackToBytes(const act::WireMessage& obj, Packer& packer) {
  packer(obj.node_fragments);
  packer(obj.actions);
  internal::PackStrToStrAbslFlatHashMap(obj.headers, packer);
  return absl::OkStatus();
}

absl::Status CppackFromBytes(act::WireMessage& obj, Unpacker& unpacker) {
  unpacker(obj.node_fragments);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking WireMessage from bytes (.node_fragments).");
  }
  unpacker(obj.actions);
  if (unpacker.GetErrorCode()) {
    return absl::InvalidArgumentError(
        "Error unpacking WireMessage from bytes (.actions).");
  }
  absl::Status status =
      internal::UnpackStrToStrAbslFlatHashMap(obj.headers, unpacker);
  if (!status.ok()) {
    return status;
  }

  return absl::OkStatus();
}

}  // namespace cppack