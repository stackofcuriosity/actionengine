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

#ifndef ACTIONENGINE_NET_HTTP_PROXYGEN_WS_CLIENT_STREAM_H_
#define ACTIONENGINE_NET_HTTP_PROXYGEN_WS_CLIENT_STREAM_H_

#include <chrono>
#include <string_view>

#include <absl/random/random.h>
#include <absl/status/status.h>

#include "actionengine/net/http/ws_common.h"
#include "actionengine/net/stream.h"

namespace folly {
class EventBase;
template <class Duration>
class HHWheelTimerBase;
using HHWheelTimer = HHWheelTimerBase<std::chrono::milliseconds>;
using HHWheelTimerHighRes = HHWheelTimerBase<std::chrono::microseconds>;
}  // namespace folly

namespace proxygen {
class HTTPMessage;
}  // namespace proxygen

namespace act::net::http {

class WebsocketClientStreamImpl;

class WebsocketClientStream final : public WebsocketStream {
 public:
  friend class WebsocketClientStreamImpl;

  using WebsocketStream::SendBytes;

  static absl::StatusOr<std::unique_ptr<WebsocketClientStream>> Connect(
      std::string_view url, folly::EventBase* evb = nullptr,
      folly::HHWheelTimer* timer = nullptr);

  explicit WebsocketClientStream(std::string_view url);
  ~WebsocketClientStream() override;

  void SetRequestHeaders(const proxygen::HTTPMessage& headers) const;
  void SetRequestHeaders(std::unique_ptr<proxygen::HTTPMessage> headers) const;
  absl::StatusOr<std::reference_wrapper<const proxygen::HTTPMessage>>
  GetResponseHeaders(absl::Duration timeout) const;

  absl::Status SendBytes(const uint8_t* data, size_t size) override;
  absl::Status SendFrame(const WebsocketFrame& frame) override;
  absl::Status SendText(std::string_view text) override;

  void Accept() override;
  void Start() override;
  absl::Status GetStatus() const override;

  void HalfClose(uint16_t code, std::string_view reason) override;
  void Abort(absl::Status status) override;
  [[nodiscard]] bool HalfClosed() const override;

  void SetFrameCallback(FrameCallback on_frame) override;
  void SetDoneCallback(DoneCallback on_done) override;

 private:
  std::unique_ptr<WebsocketClientStreamImpl> impl_;
};

}  // namespace act::net::http

#endif  // ACTIONENGINE_NET_HTTP_PROXYGEN_WS_CLIENT_STREAM_H_