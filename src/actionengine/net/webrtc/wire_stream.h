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

#ifndef ACTIONENGINE_NET_WEBRTC_WIRE_STREAM_H_
#define ACTIONENGINE_NET_WEBRTC_WIRE_STREAM_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/time/time.h>
#include <rtc/configuration.hpp>
#include <rtc/datachannel.hpp>
#include <rtc/peerconnection.hpp>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/types.h"
#include "actionengine/net/stream.h"
#include "actionengine/net/webrtc/signalling_client.h"
#include "actionengine/stores/byte_chunking.h"

namespace act::net {

struct TurnServer {
  static absl::StatusOr<TurnServer> FromString(std::string_view url);

  bool operator==(const TurnServer& other) const;

  std::string hostname;
  uint16_t port = 3478;
  std::string username;
  std::string password;
};

bool AbslParseFlag(std::string_view text, TurnServer* absl_nonnull server,
                   std::string* absl_nonnull error);
std::string AbslUnparseFlag(const TurnServer& server);

bool AbslParseFlag(std::string_view text,
                   std::vector<act::net::TurnServer>* absl_nonnull servers,
                   std::string* absl_nonnull error);
std::string AbslUnparseFlag(const std::vector<act::net::TurnServer>& servers);

struct RtcConfig {
  static constexpr int kDefaultMaxMessageSize =
      65536;  // 64 KiB to match the defaults of several browsers

  absl::StatusOr<rtc::Configuration> BuildLibdatachannelConfig() const;

  std::optional<size_t> max_message_size = kDefaultMaxMessageSize;

  bool enable_ice_udp_mux = true;

  std::vector<std::string> stun_servers = {
      "stun:stun.l.google.com:19302",
  };
  std::vector<TurnServer> turn_servers;

  // The port range for ICE candidates is only preferred: for example,
  // libdatachannel will honor it, but libwebrtc will not.
  std::optional<std::pair<uint16_t, uint16_t>> preferred_port_range;
};

struct WebRtcDataChannelConnection {
  std::shared_ptr<rtc::PeerConnection> connection;
  std::shared_ptr<rtc::DataChannel> data_channel;
};

class EstablishmentState {
 public:
  absl::StatusOr<WebRtcDataChannelConnection> Wait(absl::Time deadline);

  void ReportDoneWithStatus(absl::Status status);

  ~EstablishmentState();

  SignallingClient* absl_nullable signalling_client() const;
  void set_signalling_client(
      std::unique_ptr<SignallingClient> signalling_client);

  rtc::PeerConnection* absl_nullable connection() const;
  void set_connection(std::unique_ptr<rtc::PeerConnection> connection);

  rtc::DataChannel* absl_nullable data_channel() const;
  void set_data_channel(std::shared_ptr<rtc::DataChannel> data_channel);

  bool should_send_candidates() const;
  void set_should_send_candidates(bool value);

 private:
  void EnsureNoCallbacks() const;

  std::unique_ptr<SignallingClient> signalling_client_;
  std::unique_ptr<rtc::PeerConnection> connection_;
  std::shared_ptr<rtc::DataChannel> data_channel_;

  absl::Status status_;
  thread::PermanentEvent done_;

  std::atomic<bool> should_send_candidates_ = true;
};

absl::StatusOr<WebRtcDataChannelConnection> StartWebRtcDataChannel(
    std::string_view identity, std::string_view peer_identity = "server",
    std::string_view signalling_address = "localhost",
    uint16_t signalling_port = 80,
    std::optional<RtcConfig> rtc_config = std::nullopt, bool use_ssl = false,
    const absl::flat_hash_map<std::string, std::string>& headers = {}) noexcept;

/**
 * WebRtcWireStream is a concrete implementation of WireStream that
 * uses WebRTC for communication.
 *
 * @headerfile actionengine/net/webrtc/wire_stream.h
 *
 * It supports sending and receiving ActionEngine wire messages over
 * a WebRTC data channel. This class is designed to be used in both
 * client and server contexts, allowing for flexible communication patterns.
 */
class WebRtcWireStream final : public WireStream {
 public:
  static constexpr int kBufferSize = 256;
  static constexpr absl::Duration kHalfCloseTimeout = absl::Seconds(5);

  explicit WebRtcWireStream(
      std::shared_ptr<rtc::DataChannel> data_channel,
      std::shared_ptr<rtc::PeerConnection> connection = nullptr);

  ~WebRtcWireStream() override;

  absl::Status Send(WireMessage message) override;

  absl::Status SendWithoutBuffering(WireMessage message) override;
  absl::Status AttachBufferingBehaviour(
      WireMessageBufferingBehaviour* absl_nonnull sender) override;

  bool HasAttachedBufferingBehaviour() const override {
    act::MutexLock lock(&mu_);
    return buffering_behaviour_ != nullptr;
  }

  absl::StatusOr<std::optional<WireMessage>> Receive(
      absl::Duration timeout) override;

  absl::Status Start() override { return absl::OkStatus(); }

  absl::Status Accept() override { return absl::OkStatus(); }

  void HalfClose() override {
    act::MutexLock lock(&mu_);
    HalfCloseInternal().IgnoreError();
  }

  void Abort(absl::Status) override;

  absl::Status GetStatus() const override;

  [[nodiscard]] std::string GetId() const override { return id_; }

  [[nodiscard]] const void* absl_nullable GetImpl() const override {
    return data_channel_.get();
  }

 private:
  absl::Status SendInternal(WireMessage message)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void AbortInternal(absl::Status status) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  absl::Status HalfCloseInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void CloseOnError(absl::Status status) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  mutable act::Mutex mu_;
  mutable act::CondVar cv_ ABSL_GUARDED_BY(mu_);

  absl::Status status_ ABSL_GUARDED_BY(mu_);

  const std::string id_;
  std::shared_ptr<rtc::PeerConnection> connection_;
  std::shared_ptr<rtc::DataChannel> data_channel_;
  thread::Channel<WireMessage> recv_channel_{kBufferSize};

  absl::flat_hash_map<uint64_t, std::unique_ptr<data::ChunkedBytes>>
      chunked_messages_ ABSL_GUARDED_BY(mu_) = {};
  uint64_t next_transient_id_ ABSL_GUARDED_BY(mu_) = 0;

  bool opened_ ABSL_GUARDED_BY(mu_) = false;
  bool closed_ ABSL_GUARDED_BY(mu_) = false;

  bool half_closed_ ABSL_GUARDED_BY(mu_) = false;

  WireMessageBufferingBehaviour* absl_nullable buffering_behaviour_ = nullptr;
};

absl::StatusOr<std::unique_ptr<WebRtcWireStream>> StartStreamWithSignalling(
    std::string_view identity, std::string_view peer_identity,
    std::string_view signalling_url,
    const absl::flat_hash_map<std::string, std::string>& headers = {},
    std::optional<RtcConfig> rtc_config = std::nullopt);

absl::StatusOr<std::unique_ptr<WebRtcWireStream>> StartStreamWithSignalling(
    std::string_view identity, std::string_view peer_identity,
    std::string_view address, uint16_t port, bool use_ssl,
    const absl::flat_hash_map<std::string, std::string>& headers = {},
    std::optional<RtcConfig> rtc_config = std::nullopt);

}  // namespace act::net

#endif  // ACTIONENGINE_NET_WEBRTC_WIRE_STREAM_H_