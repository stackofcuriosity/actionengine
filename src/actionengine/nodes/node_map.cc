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

#include "node_map.h"

#include <string_view>
#include <utility>
#include <vector>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/nodes/async_node.h"
#include "actionengine/stores/chunk_store.h"

namespace act {
NodeMap::NodeMap(ChunkStoreFactory chunk_store_factory)
    : chunk_store_factory_(std::move(chunk_store_factory)) {}

NodeMap::~NodeMap() {
  act::MutexLock lock(&mu_);
  nodes_.clear();
}

AsyncNode* absl_nonnull NodeMap::Get(
    std::string_view id, const ChunkStoreFactory& chunk_store_factory) {
  act::MutexLock lock(&mu_);
  if (!nodes_.contains(id)) {
    nodes_.emplace(id, std::make_unique<AsyncNode>(
                           id, this, MakeChunkStore(chunk_store_factory, id)));
  }
  return nodes_[id].get();
}

std::shared_ptr<AsyncNode> NodeMap::Borrow(
    std::string_view id, const ChunkStoreFactory& chunk_store_factory) {
  act::MutexLock lock(&mu_);
  if (!nodes_.contains(id)) {
    nodes_.emplace(id, std::make_unique<AsyncNode>(
                           id, this, MakeChunkStore(chunk_store_factory, id)));
  }
  return nodes_[id];
}

std::shared_ptr<AsyncNode> NodeMap::Extract(std::string_view id) {
  act::MutexLock lock(&mu_);
  if (const auto map_node = nodes_.extract(id); !map_node.empty()) {
    return std::move(map_node.mapped());
  }
  return nullptr;
}

std::shared_ptr<AsyncNode> NodeMap::operator[](std::string_view id) {
  return Borrow(id);
}

bool NodeMap::contains(std::string_view id) const {
  act::MutexLock lock(&mu_);
  return nodes_.contains(id);
}

std::unique_ptr<ChunkStore> NodeMap::MakeChunkStore(
    const ChunkStoreFactory& factory, std::string_view id) const {
  if (factory) {
    return factory(id);
  }
  if (chunk_store_factory_) {
    return chunk_store_factory_(id);
  }
  return nullptr;
}
}  // namespace act
