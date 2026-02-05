// Copyright 2025 Google LLC
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

#define BOOST_ASIO_NO_DEPRECATED

#include "actionengine/net/webrtc/wire_stream.h"

#include <cstddef>
#include <functional>
#include <utility>

#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <absl/time/clock.h>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/string.hpp>
#include <boost/json/value.hpp>
#include <boost/system/detail/error_code.hpp>
#include <rtc/candidate.hpp>
#include <rtc/common.hpp>
#include <rtc/configuration.hpp>
#include <rtc/description.hpp>
#include <rtc/reliability.hpp>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/msgpack.h"
#include "actionengine/net/webrtc/signalling_client.h"
#include "actionengine/stores/byte_chunking.h"
#include "actionengine/util/status_macros.h"
#include "cppack/msgpack.h"

namespace act::net {

absl::StatusOr<TurnServer> TurnServer::FromString(std::string_view url) {
  std::string_view username_password;
  std::string_view hostname_port;

  if (size_t at_pos = url.find('@'); at_pos != std::string_view::npos) {
    username_password = url.substr(0, at_pos);
    hostname_port = url.substr(at_pos + 1);
  }

  if (hostname_port.empty()) {
    return absl::InvalidArgumentError(
        "TurnServer URL must contain a hostname and port");
  }

  std::string_view hostname;
  uint16_t port = 3478;  // Default TURN port

  if (size_t colon_pos = hostname_port.find(':');
      colon_pos == std::string_view::npos) {
    hostname = hostname_port;
  } else {
    hostname = hostname_port.substr(0, colon_pos);
    if (std::string_view port_str = hostname_port.substr(colon_pos + 1);
        !absl::SimpleAtoi(port_str, &port)) {
      return absl::InvalidArgumentError(
          "TurnServer URL contains an invalid port");
    }
  }

  std::string username;
  std::string password;
  if (username_password.empty()) {
    username = "actionengine";
    password = "";
  } else {
    if (size_t colon_pos = username_password.find(':');
        colon_pos == std::string_view::npos) {
      username = username_password;
      password = "";
    } else {
      username = username_password.substr(0, colon_pos);
      password = username_password.substr(colon_pos + 1);
    }
  }

  TurnServer server;
  server.hostname = std::string(hostname);
  server.port = port;
  server.username = std::move(username);
  server.password = std::move(password);

  return server;
}

bool TurnServer::operator==(const TurnServer& other) const {
  return hostname == other.hostname && port == other.port &&
         username == other.username && password == other.password;
}

bool AbslParseFlag(std::string_view text, TurnServer* server,
                   std::string* error) {
  auto result = TurnServer::FromString(text);
  if (!result.ok()) {
    *error = result.status().message();
    return false;
  }
  *server = std::move(result.value());
  return true;
}

std::string AbslUnparseFlag(const TurnServer& server) {
  if (server.username.empty()) {
    return absl::StrFormat("%s:%d", server.hostname, server.port);
  }

  return absl::StrFormat("%s:%s@%s:%d", server.username, server.password,
                         server.hostname, server.port);
}

bool AbslParseFlag(std::string_view text,
                   std::vector<act::net::TurnServer>* servers,
                   std::string* error) {
  if (text.empty()) {
    return true;
  }
  for (const auto& server_str : absl::StrSplit(text, ',')) {
    act::net::TurnServer server;
    if (!AbslParseFlag(server_str, &server, error)) {
      return false;
    }
    servers->push_back(std::move(server));
  }
  return true;
}

std::string AbslUnparseFlag(const std::vector<act::net::TurnServer>& servers) {
  if (servers.empty()) {
    return "";
  }
  return absl::StrJoin(servers, ",",
                       [](std::string* out, const TurnServer& server) {
                         *out = AbslUnparseFlag(server);
                       });
}

absl::StatusOr<rtc::Configuration> RtcConfig::BuildLibdatachannelConfig()
    const {
  rtc::Configuration config;
  config.maxMessageSize = max_message_size;
  const std::pair<uint16_t, uint16_t> port_range =
      preferred_port_range.value_or(std::pair(1024, 65535));
  config.portRangeBegin = port_range.first;
  config.portRangeEnd = port_range.second;
  config.enableIceUdpMux = enable_ice_udp_mux;

  for (const auto& server : stun_servers) {
    try {
      config.iceServers.emplace_back(server);
    } catch (const std::exception& exc) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "Failed to parse STUN server URL '%s': %s", server, exc.what()));
    }
  }
  for (const auto& server : turn_servers) {
    try {
      config.iceServers.emplace_back(server.hostname, server.port,
                                     server.username, server.password);
    } catch (const std::exception& exc) {
      return absl::InvalidArgumentError(
          absl::StrFormat("Failed to parse TURN server URL '%s:%d': %s",
                          server.hostname, server.port, exc.what()));
    }
  }

  return config;
}

absl::StatusOr<WebRtcDataChannelConnection> EstablishmentState::Wait(
    absl::Time deadline) {
  thread::Case on_signalling_error = signalling_client_ != nullptr
                                         ? signalling_client_->OnError()
                                         : thread::NonSelectableCase();
  const int selected = thread::SelectUntil(
      deadline, {done_.OnEvent(), thread::OnCancel(), on_signalling_error});
  EnsureNoCallbacks();

  if (connection_ == nullptr || data_channel_ == nullptr) {
    return absl::InternalError(
        "WebRtcDataChannel establishment failed: missing connection or "
        "data channel.");
  }

  if (selected == -1) {
    return absl::DeadlineExceededError(
        "WebRtcDataChannel establishment timed out.");
  }

  if (thread::Cancelled()) {
    if (signalling_client_ != nullptr) {
      signalling_client_->Cancel();
    }

    return absl::CancelledError("WebRtcDataChannel establishment cancelled.");
  }

  if (!status_.ok()) {
    return status_;
  }

  if (selected == 2) {
    // safe to do as we have already checked signalling_client_ != nullptr
    return signalling_client_->GetStatus();
  }

  if (!data_channel_->isOpen()) {
    return absl::InternalError(
        "WebRtcWireStream data channel is not open, likely due to a failed "
        "connection.");
  }

  return WebRtcDataChannelConnection{
      .connection = std::move(connection_),
      .data_channel = std::move(data_channel_),
  };
}

void EstablishmentState::ReportDoneWithStatus(absl::Status status) {
  EnsureNoCallbacks();
  status_.Update(std::move(status));
  if (!done_.HasBeenNotified()) {
    done_.Notify();
  }
}

EstablishmentState::~EstablishmentState() {
  EnsureNoCallbacks();
  if (!done_.HasBeenNotified()) {
    done_.Notify();
  }
}

SignallingClient* EstablishmentState::signalling_client() const {
  return signalling_client_.get();
}

void EstablishmentState::set_signalling_client(
    std::unique_ptr<SignallingClient> signalling_client) {
  signalling_client_ = std::move(signalling_client);
}

rtc::PeerConnection* EstablishmentState::connection() const {
  return connection_.get();
}

void EstablishmentState::set_connection(
    std::unique_ptr<rtc::PeerConnection> connection) {
  connection_ = std::move(connection);
}

rtc::DataChannel* EstablishmentState::data_channel() const {
  return data_channel_.get();
}

void EstablishmentState::set_data_channel(
    std::shared_ptr<rtc::DataChannel> data_channel) {
  data_channel_ = std::move(data_channel);
}

bool EstablishmentState::should_send_candidates() const {
  return should_send_candidates_.load();
}

void EstablishmentState::set_should_send_candidates(bool value) {
  should_send_candidates_.store(value);
}

void EstablishmentState::EnsureNoCallbacks() const {
  if (signalling_client_ != nullptr) {
    signalling_client_->ResetCallbacks();
  }

  if (connection_ != nullptr) {
    connection_->resetCallbacks();
  }
}

WebRtcWireStream::WebRtcWireStream(
    std::shared_ptr<rtc::DataChannel> data_channel,
    std::shared_ptr<rtc::PeerConnection> connection)
    : id_(data_channel->label()),
      connection_(std::move(connection)),
      data_channel_(std::move(data_channel)) {

  data_channel_->onMessage(
      [this](rtc::binary message) {
        const size_t message_size = message.size();
        const auto data = reinterpret_cast<uint8_t*>(std::move(message).data());
        absl::StatusOr<data::BytePacket> packet =
            data::ParseBytePacket(data, message_size);

        act::MutexLock lock(&mu_);

        if (closed_) {
          return;
        }

        if (!packet.ok()) {
          CloseOnError(absl::InternalError(
              absl::StrFormat("WebRtcWireStream unpack failed: %s",
                              packet.status().message())));
          return;
        }

        const uint64_t transient_id = GetTransientIdFromPacket(*packet);
        auto& chunked_message = chunked_messages_[transient_id];
        if (!chunked_message) {
          chunked_message = std::make_unique<data::ChunkedBytes>();
        }
        auto got_full_message = chunked_message->FeedPacket(*std::move(packet));
        if (!got_full_message.ok()) {
          CloseOnError(absl::InternalError(
              absl::StrFormat("WebRtcWireStream chunked message "
                              "feed failed: %s",
                              got_full_message.status().message())));
          return;
        }

        if (!*got_full_message) {
          return;  // Not all chunks received yet, wait for more.
        }

        absl::StatusOr<std::vector<uint8_t>> message_data =
            chunked_message->ConsumeCompleteBytes();
        if (!message_data.ok()) {
          CloseOnError(absl::InternalError(
              absl::StrFormat("WebRtcWireStream consume failed: %s",
                              message_data.status().message())));
          return;
        }

        mu_.unlock();
        absl::StatusOr<WireMessage> unpacked =
            cppack::Unpack<WireMessage>(std::vector(*std::move(message_data)));
        mu_.lock();

        if (!unpacked.ok()) {
          CloseOnError(absl::InternalError(
              absl::StrFormat("WebRtcWireStream unpack failed: %s",
                              unpacked.status().message())));
          return;
        }

        recv_channel_.writer()->WriteUnlessCancelled(*std::move(unpacked));
        chunked_messages_.erase(transient_id);
      },
      [](const rtc::string&) {});

  if (data_channel_ && data_channel_->isOpen()) {
    opened_ = true;
  } else {
    data_channel_->onOpen([this]() {
      act::MutexLock lock(&mu_);
      status_ = absl::OkStatus();
      opened_ = true;
      cv_.SignalAll();
    });
  }

  data_channel_->onClosed([this]() {
    act::MutexLock lock(&mu_);
    half_closed_ = true;

    if (!closed_) {
      status_ = absl::CancelledError("WebRtcWireStream closed");
      recv_channel_.writer()->Close();
    }

    closed_ = true;
    cv_.SignalAll();
  });

  data_channel_->onError([this](const std::string& error) {
    act::MutexLock lock(&mu_);
    half_closed_ = true;

    if (!closed_) {
      status_ = absl::InternalError(
          absl::StrFormat("WebRtcWireStream error: %s", error));
      recv_channel_.writer()->Close();
    }

    closed_ = true;
    cv_.SignalAll();
  });
}

WebRtcWireStream::~WebRtcWireStream() {
  act::MutexLock lock(&mu_);

  if (!half_closed_) {
    if (!closed_) {
      LOG(ERROR) << "WebRtcWireStream destructor called before half-closing or "
                    "aborting.";
    }
    // If closed at this point, AbortInternal will be a no-op.
    AbortInternal(absl::ResourceExhaustedError(
        "Stream was destroyed unexpectedly (before half-closing or aborting)"));
  }

  mu_.unlock();
  try {
    data_channel_->close();
  } catch (const std::exception&) {}
  mu_.lock();

  const absl::Time deadline = absl::Now() + absl::Seconds(10);
  while (!closed_) {
    if (cv_.WaitWithDeadline(&mu_, deadline)) {
      break;
    }
  }
  if (!closed_) {
    LOG(WARNING) << "WebRtcWireStream destructor timed out waiting for close.";
  }
  mu_.unlock();
  try {
    connection_->close();
  } catch (const std::exception&) {}
  mu_.lock();
}

absl::Status WebRtcWireStream::AttachBufferingBehaviour(
    WireMessageBufferingBehaviour* sender) {
  act::MutexLock lock(&mu_);

  if (sender == nullptr) {
    buffering_behaviour_ = nullptr;
    return absl::OkStatus();
  }

  if (!status_.ok()) {
    return status_;
  }

  if (half_closed_) {
    return absl::FailedPreconditionError(
        "WebRtcWireStream is half-closed, cannot attach a "
        "buffering behaviour.");
  }

  if (closed_) {
    return absl::FailedPreconditionError(
        "WebRtcWireStream is closed, cannot attach a buffering behaviour.");
  }

  if (buffering_behaviour_ != nullptr) {
    return absl::FailedPreconditionError(
        "WebRtcWireStream already has a ReducingWireMessageSender attached");
  }

  buffering_behaviour_ = sender;
  return absl::OkStatus();
}

absl::Status WebRtcWireStream::Send(WireMessage message) {
  act::MutexLock lock(&mu_);

  if (!status_.ok()) {
    return status_;
  }

  if (half_closed_) {
    return absl::FailedPreconditionError(
        "WebRtcWireStream is half-closed, cannot send messages");
  }

  if (buffering_behaviour_ != nullptr) {
    mu_.unlock();
    absl::Status status = buffering_behaviour_->Send(std::move(message));
    mu_.lock();
    return status;
  }

  return SendInternal(std::move(message));
}

absl::Status WebRtcWireStream::SendWithoutBuffering(WireMessage message) {
  act::MutexLock lock(&mu_);

  if (!status_.ok()) {
    return status_;
  }

  if (half_closed_) {
    return absl::FailedPreconditionError(
        "WebRtcWireStream is half-closed, cannot send messages");
  }

  return SendInternal(std::move(message));
}

absl::Status WebRtcWireStream::SendInternal(WireMessage message)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  constexpr absl::Duration openOrCloseTimeout = absl::Seconds(10);
  const absl::Time deadline = absl::Now() + openOrCloseTimeout;
  while (!opened_ && !closed_) {
    if (cv_.WaitWithDeadline(&mu_, deadline) && !opened_ && !closed_) {
      return absl::DeadlineExceededError(
          "WebRtcWireStream Send timed out waiting for channel to open.");
    }
  }

  if (closed_) {
    return absl::CancelledError("WebRtcWireStream is closed");
  }

  const uint64_t transient_id = next_transient_id_++;

  mu_.unlock();

  const std::vector<uint8_t> message_uint8_t = cppack::Pack(std::move(message));

  const std::vector<data::BytePacket> packets = data::SplitBytesIntoPackets(
      message_uint8_t, transient_id,
      static_cast<int64_t>(connection_->remoteMaxMessageSize()));

  std::vector<std::vector<uint8_t>> serialized_packets;
  serialized_packets.reserve(packets.size());
  for (auto& packet : packets) {
    serialized_packets.push_back(data::SerializeBytePacket(packet));
    // TODO: clear packet to save memory
  }

  absl::Status status;
  for (const auto& serialized_packet : serialized_packets) {
    const auto* message_chunk_data =
        reinterpret_cast<const rtc::byte*>(serialized_packet.data());
    rtc::binary message_chunk_bytes(
        message_chunk_data, message_chunk_data + serialized_packet.size());
    data_channel_->send(std::move(message_chunk_bytes));
  }

  mu_.lock();
  return status;
}

absl::StatusOr<std::optional<WireMessage>> WebRtcWireStream::Receive(
    absl::Duration timeout) {
  const absl::Time now = absl::Now();
  act::MutexLock lock(&mu_);

  const absl::Time deadline =
      !half_closed_ ? now + timeout : now + kHalfCloseTimeout;

  WireMessage message;
  bool ok;

  mu_.unlock();
  const int selected = thread::SelectUntil(
      deadline, {recv_channel_.reader()->OnRead(&message, &ok)});
  mu_.lock();

  if (selected == 0 && !ok) {
    return std::nullopt;
  }

  if (selected == -1) {
    return absl::DeadlineExceededError(
        "WebRtcWireStream Receive timed out while waiting for a message.");
  }

  if (message.actions.empty() && message.node_fragments.empty()) {
    return std::nullopt;
  }

  for (const auto& fragment : message.node_fragments) {
    if (fragment.id == "__abort__") {
      if (!std::holds_alternative<Chunk>(fragment.data)) {
        status_ = absl::InternalError(
            "Received abort fragment with invalid data type. Aborting anyway.");
        CloseOnError(status_);
        return status_;
      }
      absl::StatusOr<absl::Status> abort_status_or =
          ConvertTo<absl::Status>(std::get<Chunk>(fragment.data));
      if (!abort_status_or.ok()) {
        status_ = abort_status_or.status();
      } else {
        status_ = *abort_status_or;
      }
      CloseOnError(status_);
      return status_;
    }
  }

  return message;
}

void WebRtcWireStream::Abort(absl::Status status) {
  act::MutexLock lock(&mu_);
  AbortInternal(std::move(status));
}

void WebRtcWireStream::AbortInternal(absl::Status status) {
  if (closed_ || half_closed_) {
    return;
  }

  WireMessage abort_message{
      .node_fragments = {{
          .id = "__abort__",
          .data = ConvertTo<Chunk>(std::move(status)).value(),
          .seq = 0,
          .continued = false,
      }}};

  if (buffering_behaviour_ != nullptr) {
    buffering_behaviour_->Send(abort_message).IgnoreError();
    buffering_behaviour_->NoMoreSends();

    mu_.unlock();
    const absl::Status finalize_status = buffering_behaviour_->Finalize();
    mu_.lock();

    DCHECK(buffering_behaviour_ == nullptr)
        << "Buffering behaviour must have been detached in Finalize.";
    if (!finalize_status.ok()) {
      LOG(ERROR) << "Error finalizing buffering behaviour during Abort: "
                 << finalize_status;
    }
  } else {
    SendInternal(std::move(abort_message)).IgnoreError();
  }

  CloseOnError(absl::CancelledError("WebRtcWireStream aborted"));
}

absl::Status WebRtcWireStream::GetStatus() const {
  act::MutexLock lock(&mu_);
  return status_;
}

absl::Status WebRtcWireStream::HalfCloseInternal() {
  if (half_closed_) {
    return absl::OkStatus();
  }

  if (buffering_behaviour_ != nullptr) {
    buffering_behaviour_->NoMoreSends();

    mu_.unlock();
    const absl::Status status = buffering_behaviour_->Finalize();
    mu_.lock();

    DCHECK(buffering_behaviour_ == nullptr)
        << "Buffering behaviour must have been detached in Finalize.";
    RETURN_IF_ERROR(status);
  }

  half_closed_ = true;
  RETURN_IF_ERROR(SendInternal(WireMessage{}));

  return absl::OkStatus();
}

void WebRtcWireStream::CloseOnError(absl::Status status)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  LOG(ERROR) << "WebRtcWireStream error: " << status.message();
  half_closed_ = true;

  if (!closed_) {
    closed_ = true;
    status_.Update(std::move(status));
    recv_channel_.writer()->Close();
    mu_.unlock();
    data_channel_->close();
    mu_.lock();
  }

  cv_.SignalAll();
}

static absl::StatusOr<rtc::Description> ParseDescriptionFromMessage(
    const boost::json::value& message) {
  boost::system::error_code error;

  const auto desc_ptr = message.find_pointer("/description", error);
  if (error) {
    return absl::InvalidArgumentError(
        "Error parsing 'description' field in signalling message: " +
        boost::json::serialize(message));
  }

  if (desc_ptr == nullptr) {
    return absl::InvalidArgumentError(
        "No 'description' field in signalling message: " +
        boost::json::serialize(message));
  }

  const boost::system::result<const boost::json::string&> desc_or =
      desc_ptr->try_as_string();
  if (desc_or.has_error()) {
    return absl::InvalidArgumentError(
        "'description' field is not a string in signalling message: " +
        boost::json::serialize(message));
  }

  // Libdatachannel will throw an exception if the description is invalid, so
  // we need to catch it and return an error instead.
  try {
    return rtc::Description(desc_or->c_str());
  } catch (const std::exception& exc) {
    return absl::InvalidArgumentError(
        "Error parsing description from signalling message: " +
        boost::json::serialize(message) + ". Exception: " + exc.what());
  }
}

static absl::StatusOr<rtc::Candidate> ParseCandidateFromMessage(
    const boost::json::value& message) {
  boost::system::error_code error;

  const auto type_ptr = message.find_pointer("/type", error);
  if (error) {
    return absl::InvalidArgumentError(
        "Error parsing 'type' field in signalling message: " +
        boost::json::serialize(message));
  }

  const auto candidate_ptr = message.find_pointer("/candidate", error);
  if (error) {
    return absl::InvalidArgumentError(
        "Error parsing 'candidate' field in signalling message: " +
        boost::json::serialize(message));
  }

  if (type_ptr == nullptr) {
    return absl::InvalidArgumentError(
        "No 'type' field in signalling message: " +
        boost::json::serialize(message));
  }
  if (candidate_ptr == nullptr) {
    return absl::InvalidArgumentError(
        "No 'candidate' field in signalling message: " +
        boost::json::serialize(message));
  }

  const boost::system::result<const boost::json::string&> type_or =
      type_ptr->try_as_string();
  if (type_or.has_error()) {
    return absl::InvalidArgumentError(
        "'type' field is not a string in signalling message: " +
        boost::json::serialize(message));
  }

  if (*type_or != "candidate") {
    return absl::InvalidArgumentError("Not a candidate message: " +
                                      boost::json::serialize(message));
  }

  const boost::system::result<const boost::json::string&> candidate_or =
      candidate_ptr->try_as_string();
  if (candidate_or.has_error()) {
    return absl::InvalidArgumentError(
        "'candidate' field is not a string in signalling message: " +
        boost::json::serialize(message));
  }

  // Libdatachannel will throw an exception if the candidate is invalid, so
  // we need to catch it and return an error instead.
  try {
    return rtc::Candidate(candidate_or->c_str());
  } catch (const std::exception& exc) {
    return absl::InvalidArgumentError(
        "Error parsing candidate from signalling message: " +
        boost::json::serialize(message) + ". Exception: " + exc.what());
  }
}

static std::string MakeCandidateMessage(std::string_view peer_id,
                                        const rtc::Candidate& candidate) {
  boost::json::object candidate_json;
  candidate_json["id"] = peer_id;
  candidate_json["type"] = "candidate";
  candidate_json["candidate"] = std::string(candidate);
  candidate_json["mid"] = candidate.mid();

  return boost::json::serialize(candidate_json);
}

static std::string MakeOfferMessage(std::string_view peer_id,
                                    const rtc::Description& description) {
  boost::json::object offer;
  offer["id"] = peer_id;
  offer["type"] = "offer";
  offer["description"] = description.generateSdp("\r\n");

  return boost::json::serialize(offer);
}

absl::StatusOr<WebRtcDataChannelConnection> StartWebRtcDataChannel(
    std::string_view identity, std::string_view peer_identity,
    std::string_view signalling_address, uint16_t signalling_port,
    std::optional<RtcConfig> rtc_config, bool use_ssl,
    const absl::flat_hash_map<std::string, std::string>& headers) noexcept {
  auto state = std::make_shared<EstablishmentState>();

  state->set_signalling_client(std::make_unique<SignallingClient>(
      signalling_address, signalling_port, use_ssl));

  RtcConfig config = std::move(rtc_config).value_or(RtcConfig());
  ASSIGN_OR_RETURN(rtc::Configuration rtc_config_libdatachannel,
                   config.BuildLibdatachannelConfig());

  // Create PeerConnection which may throw if configuration is invalid.
  try {
    state->set_connection(std::make_unique<rtc::PeerConnection>(
        std::move(rtc_config_libdatachannel)));
  } catch (const std::exception& exc) {
    return absl::InternalError(
        absl::StrFormat("Error creating PeerConnection: %s", exc.what()));
  }

  // Set up signalling client callbacks to handle answers and remote ICE
  // candidates by updating the PeerConnection state.
  state->signalling_client()->OnAnswer(
      [peer_identity = std::string(peer_identity), state](
          std::string_view received_peer_id,
          const boost::json::value& message) noexcept {
        if (received_peer_id != peer_identity) {
          DLOG(INFO) << "Ignoring answer for a different peer ID: "
                     << received_peer_id;
          return;
        }

        absl::StatusOr<rtc::Description> description =
            ParseDescriptionFromMessage(message);
        if (!description.ok()) {
          state->ReportDoneWithStatus(description.status());
          return;
        }

        try {
          state->connection()->setRemoteDescription(*std::move(description));
        } catch (const std::exception& exc) {
          state->ReportDoneWithStatus(absl::InternalError(absl::StrFormat(
              "Error setting remote description: %s", exc.what())));
        }
      });
  state->signalling_client()->OnCandidate(
      [peer_identity = std::string(peer_identity), state](
          std::string_view received_peer_id,
          const boost::json::value& message) noexcept {
        if (received_peer_id != peer_identity) {
          return;
        }

        absl::StatusOr<rtc::Candidate> candidate =
            ParseCandidateFromMessage(message);
        if (!candidate.ok()) {
          state->ReportDoneWithStatus(candidate.status());
          return;
        }

        try {
          state->connection()->addRemoteCandidate(*std::move(candidate));
        } catch (const std::exception& exc) {
          state->ReportDoneWithStatus(absl::InternalError(absl::StrFormat(
              "Error adding remote candidate: %s", exc.what())));
        }
      });

  auto local_candidate_channel =
      std::make_shared<thread::Channel<rtc::Candidate>>(16);
  state->connection()->onLocalCandidate(
      [peer_id = std::string(peer_identity), state,
       local_candidate_channel](const rtc::Candidate& candidate) noexcept {
        if (state->should_send_candidates()) {
          local_candidate_channel->writer()->WriteUnlessCancelled(candidate);
        }
      });
  state->connection()->onIceStateChange(
      [state, local_candidate_channel](
          rtc::PeerConnection::IceState ice_state) noexcept {
        if (ice_state == rtc::PeerConnection::IceState::Connected ||
            ice_state == rtc::PeerConnection::IceState::Completed ||
            ice_state == rtc::PeerConnection::IceState::Disconnected ||
            ice_state == rtc::PeerConnection::IceState::Closed ||
            ice_state == rtc::PeerConnection::IceState::Failed) {
          if (state->should_send_candidates()) {
            state->set_should_send_candidates(false);
            local_candidate_channel->writer()->Close();
          }
        }
        if (ice_state == rtc::PeerConnection::IceState::Failed) {
          state->connection()->resetCallbacks();
          try {
            state->connection()->close();
          } catch (const std::exception& exc) {
            LOG(WARNING) << absl::StrFormat(
                "Error closing PeerConnection after ICE failure: %s",
                exc.what());
          }
          state->ReportDoneWithStatus(absl::InternalError(
              "WebRtcWireStream connection failed during ICE negotiation"));
        }
      });

  RETURN_IF_ERROR(
      state->signalling_client()->ConnectWithIdentity(identity, headers));

  auto init = rtc::DataChannelInit{};
  init.reliability.unordered = true;
  try {
    state->set_data_channel(state->connection()->createDataChannel(
        std::string(identity), std::move(init)));
  } catch (const std::exception& exc) {
    return absl::InternalError(
        absl::StrFormat("Error creating data channel: %s", exc.what()));
  }

  std::string offer_message;
  try {
    offer_message =
        MakeOfferMessage(peer_identity, state->connection()->createOffer());
  } catch (const std::exception& exc) {
    return absl::InternalError(
        absl::StrFormat("Error creating offer: %s", exc.what()));
  }
  state->data_channel()->onOpen(
      [state]() { state->ReportDoneWithStatus(absl::OkStatus()); });
  RETURN_IF_ERROR(state->signalling_client()->Send(offer_message));

  rtc::Candidate candidate;
  while (local_candidate_channel->reader()->Read(&candidate)) {
    if (state->should_send_candidates()) {
      const std::string message =
          MakeCandidateMessage(peer_identity, candidate);
      if (absl::Status send_status = state->signalling_client()->Send(message);
          !send_status.ok()) {
        state->ReportDoneWithStatus(std::move(send_status));
        break;
      }
    }
  }

  return state->Wait(absl::Now() + absl::Seconds(30));
}

absl::StatusOr<std::unique_ptr<WebRtcWireStream>> StartStreamWithSignalling(
    std::string_view identity, std::string_view peer_identity,
    std::string_view signalling_url,
    const absl::flat_hash_map<std::string, std::string>& headers,
    std::optional<RtcConfig> rtc_config) {

  ASSIGN_OR_RETURN(auto ws_url, WsUrl::FromString(signalling_url));

  return StartStreamWithSignalling(identity, peer_identity, ws_url.host,
                                   ws_url.port, ws_url.scheme == "wss", headers,
                                   std::move(rtc_config));
}

absl::StatusOr<std::unique_ptr<WebRtcWireStream>> StartStreamWithSignalling(
    std::string_view identity, std::string_view peer_identity,
    std::string_view address, uint16_t port, bool use_ssl,
    const absl::flat_hash_map<std::string, std::string>& headers,
    std::optional<RtcConfig> rtc_config) {

  absl::StatusOr<WebRtcDataChannelConnection> connection =
      StartWebRtcDataChannel(identity, peer_identity, address, port,
                             std::move(rtc_config), use_ssl, headers);
  if (!connection.ok()) {
    return connection.status();
  }

  return std::make_unique<WebRtcWireStream>(std::move(connection->data_channel),
                                            std::move(connection->connection));
}

}  // namespace act::net