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

#ifndef ACTIONENGINE_UTIL_METRICS_H_
#define ACTIONENGINE_UTIL_METRICS_H_

#include <any>
#include <atomic>
#include <string>
#include <string_view>
#include <variant>

#include <absl/container/flat_hash_map.h>
#include <absl/status/status.h>

#include "actionengine/concurrency/concurrency.h"
#include "thread/channel.h"
#include "thread/fiber.h"

namespace act {

class MetricStore;

class ScopedIntegerGaugeIncrement {
 public:
  ScopedIntegerGaugeIncrement(MetricStore* absl_nonnull store,
                              std::string_view name, int64_t by = 1);

  void Add(int64_t delta);

  ~ScopedIntegerGaugeIncrement();

 private:
  MetricStore* const absl_nonnull store_;
  std::string name_;
  uint64_t by_;
};

class MetricStore {
 public:
  using MetricValue = std::variant<uint64_t, int64_t, double, std::monostate>;

  double GetValue(std::string_view name) {
    act::MutexLock lock(&mu_);
    if (const auto it = metrics_.find(std::string(name));
        it == metrics_.end()) {
      return 0.0;
    } else {
      if (std::holds_alternative<uint64_t>(it->second)) {
        return static_cast<double>(std::get<uint64_t>(it->second));
      }
      if (std::holds_alternative<int64_t>(it->second)) {
        return static_cast<double>(std::get<int64_t>(it->second));
      }
      if (std::holds_alternative<double>(it->second)) {
        return std::get<double>(it->second);
      }
    }
    return 0.0;
  }

  absl::Status IncrementCounter(std::string_view name, uint64_t by) {
    act::MutexLock lock(&mu_);

    if (MetricValue& value = metrics_[std::string(name)];
        std::holds_alternative<uint64_t>(value)) {
      std::get<uint64_t>(value) += by;
    } else if (std::holds_alternative<std::monostate>(value)) {
      value = by;
    } else {
      return absl::InvalidArgumentError("Metric " + std::string(name) +
                                        " is not a counter");
    }
    return absl::OkStatus();
  }

  uint64_t GetCounterValue(std::string_view name) {
    act::MutexLock lock(&mu_);
    if (const auto it = metrics_.find(std::string(name));
        it != metrics_.end() && std::holds_alternative<uint64_t>(it->second)) {
      return std::get<uint64_t>(it->second);
    }
    return 0;
  }

  absl::Status AddToIntegerGauge(std::string_view name, int64_t value) {
    act::MutexLock lock(&mu_);

    if (MetricValue& current_value = metrics_[std::string(name)];
        std::holds_alternative<int64_t>(current_value)) {
      std::get<int64_t>(current_value) += value;
    } else if (std::holds_alternative<std::monostate>(current_value)) {
      current_value = value;
    } else {
      return absl::InvalidArgumentError("Metric " + std::string(name) +
                                        " is not an integer gauge");
    }
    return absl::OkStatus();
  }

  ScopedIntegerGaugeIncrement MakeScopedIntegerGauge(std::string_view name,
                                                     int64_t by = 1) {
    return {this, name, by};
  }

  int64_t GetIntegerGaugeValue(std::string_view name) {
    act::MutexLock lock(&mu_);
    if (const auto it = metrics_.find(std::string(name));
        it != metrics_.end() && std::holds_alternative<int64_t>(it->second)) {
      return std::get<int64_t>(it->second);
    }
    return 0;
  }

  absl::Status UpdateDoubleGauge(std::string_view name, double value) {
    act::MutexLock lock(&mu_);

    if (MetricValue& current_value = metrics_[std::string(name)];
        std::holds_alternative<double>(current_value)) {
      std::get<double>(current_value) = value;
    } else if (std::holds_alternative<std::monostate>(current_value)) {
      current_value = value;
    } else {
      return absl::InvalidArgumentError("Metric " + std::string(name) +
                                        " is not a double gauge");
    }
    return absl::OkStatus();
  }

  double GetDoubleGaugeValue(std::string_view name) {
    act::MutexLock lock(&mu_);
    if (const auto it = metrics_.find(std::string(name));
        it != metrics_.end() && std::holds_alternative<double>(it->second)) {
      return std::get<double>(it->second);
    }
    return 0.0;
  }

 private:
  mutable act::Mutex mu_;
  absl::flat_hash_map<std::string, MetricValue> metrics_ ABSL_GUARDED_BY(mu_);
};

inline ScopedIntegerGaugeIncrement::ScopedIntegerGaugeIncrement(
    MetricStore* absl_nonnull store, std::string_view name, int64_t by)
    : store_(store), name_(name), by_(by) {
  store_->IncrementCounter(name_, by_).IgnoreError();
}

inline void ScopedIntegerGaugeIncrement::Add(int64_t delta) {
  store_->AddToIntegerGauge(name_, delta).IgnoreError();
  by_ += delta;
}

inline ScopedIntegerGaugeIncrement::~ScopedIntegerGaugeIncrement() {
  store_->IncrementCounter(name_, -by_).IgnoreError();
}

inline MetricStore& GetGlobalMetricStore() {
  static auto* store = new MetricStore();
  return *store;
}

}  // namespace act

#endif  // ACTIONENGINE_UTIL_METRICS_H_