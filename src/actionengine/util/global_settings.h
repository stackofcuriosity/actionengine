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

#ifndef ACTIONENGINE_UTIL_GLOBAL_SETTINGS_H_
#define ACTIONENGINE_UTIL_GLOBAL_SETTINGS_H_

#include <cstdint>

#include <absl/time/time.h>

namespace act {

struct GlobalSettings {
  bool readers_deserialise_automatically = true;
  bool readers_read_in_order = true;
  bool readers_remove_read_chunks = true;

  int64_t readers_buffer_size = 32;
  absl::Duration readers_timeout = absl::InfiniteDuration();
};

GlobalSettings& GetGlobalSettings();

}  // namespace act

#endif  // ACTIONENGINE_UTIL_GLOBAL_SETTINGS_H_