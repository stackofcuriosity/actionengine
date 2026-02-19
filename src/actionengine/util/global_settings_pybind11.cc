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

#include "actionengine/util/global_settings_pybind11.h"

#include <string>
#include <string_view>

#include <absl/time/time.h>
#include <pybind11/pybind11.h>

#include "actionengine/util/global_settings.h"

namespace act::pybindings {

namespace py = ::pybind11;

void BindGlobalSettings(py::handle scope, std::string_view name) {
  py::class_<act::GlobalSettings>(scope, std::string(name).c_str())
      .def_readwrite("readers_deserialise_automatically",
                     &act::GlobalSettings::readers_deserialise_automatically)
      .def_readwrite("readers_read_in_order",
                     &act::GlobalSettings::readers_read_in_order)
      .def_readwrite("readers_remove_read_chunks",
                     &act::GlobalSettings::readers_remove_read_chunks)
      .def_readwrite("readers_buffer_size",
                     &act::GlobalSettings::readers_buffer_size)
      .def_property(
          "readers_timeout",
          [](const act::GlobalSettings& self) {
            return absl::ToDoubleSeconds(self.readers_timeout);
          },
          [](act::GlobalSettings& self, double timeout_seconds) {
            if (timeout_seconds < 0) {
              self.readers_timeout = absl::InfiniteDuration();
              return;
            }
            self.readers_timeout = absl::Seconds(timeout_seconds);
          });
}

void BindGetGlobalSettingsFunction(py::module scope,
                                   std::string_view function_name) {
  scope.def(std::string(function_name).c_str(), &act::GetGlobalSettings,
            py::return_value_policy::reference);
}

}  // namespace act::pybindings