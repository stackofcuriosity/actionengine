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

#define BOOST_ASIO_NO_DEPRECATED

#include "actionengine/net/webrtc/server.h"

#include <functional>
#include <utility>
#include <vector>

#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/status/statusor.h>
#include <absl/time/time.h>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/string.hpp>
#include <boost/json/value.hpp>
#include <boost/system/detail/error_code.hpp>
#include <rtc/candidate.hpp>
#include <rtc/configuration.hpp>
#include <rtc/datachannel.hpp>
#include <rtc/description.hpp>
#include <rtc/peerconnection.hpp>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/net/webrtc/signalling_client.h"
#include "actionengine/net/webrtc/wire_stream.h"
#include "actionengine/util/map_util.h"

// TODO: split this file into multiple files for better organization.

namespace act::net {

WebRtcServer::WebRtcServer(act::Service* service, std::string_view address,
                           std::string_view signalling_identity,
                           const WsUrl& signalling_url,
                           std::optional<RtcConfig> rtc_config)
    : service_(service),
      address_(address),
      signalling_address_(signalling_url.host),
      signalling_port_(signalling_url.port),
      signalling_identity_(signalling_identity),
      rtc_config_(std::move(rtc_config)),
      signalling_use_ssl_(signalling_url.scheme == "wss"),
      ready_data_connections_(32) {}

WebRtcServer::WebRtcServer(act::Service* service, std::string_view address,
                           std::string_view signalling_identity,
                           std::string_view signalling_url,
                           std::optional<RtcConfig> rtc_config)
    : WebRtcServer(service, address, signalling_identity,
                   WsUrl::FromStringOrDie(signalling_url),
                   std::move(rtc_config)) {}

WebRtcServer::~WebRtcServer() {
  act::MutexLock lock(&mu_);
  CancelInternal().IgnoreError();
  JoinInternal().IgnoreError();
}

void WebRtcServer::Run() {
  act::MutexLock l(&mu_);
  main_loop_ = thread::NewTree({}, [this]() {
    act::MutexLock lock(&mu_);
    RunLoop();
  });
}

absl::Status WebRtcServer::Cancel() {
  act::MutexLock lock(&mu_);
  return CancelInternal();
}

absl::Status WebRtcServer::Join() {
  act::MutexLock lock(&mu_);
  absl::Status status = JoinInternal();
  return status;
}

void WebRtcServer::SetSignallingHeader(std::string_view key,
                                       std::string_view value) {
  signalling_headers_[std::string(key)] = std::string(value);
}

absl::Status WebRtcServer::CancelInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (main_loop_ == nullptr) {
    return absl::FailedPreconditionError(
        "WebRtcServer Cancel called on either unstarted or already "
        "cancelled server.");
  }
  ready_data_connections_.writer()->Close();
  main_loop_->Cancel();
  return absl::OkStatus();
}

absl::Status WebRtcServer::JoinInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {

  if (main_loop_ != nullptr) {
    mu_.unlock();
    main_loop_->Join();
    mu_.lock();
    main_loop_ = nullptr;
  }

  return absl::OkStatus();
}

static std::string MakeCandidateMessage(std::string_view peer_id,
                                        const rtc::Candidate& candidate) {
  boost::json::object message;
  message["type"] = "candidate";
  message["id"] = peer_id;
  message["candidate"] = std::string(candidate);
  message["mid"] = candidate.mid();
  return boost::json::serialize(message);
}

static std::string MakeAnswerMessage(std::string_view peer_id,
                                     const rtc::Description& description) {
  boost::json::object message;
  message["type"] = "answer";
  message["id"] = peer_id;
  message["description"] = description.generateSdp("\r\n");
  return boost::json::serialize(message);
}

static absl::StatusOr<rtc::Description> ParseDescriptionFromOfferMessage(
    const boost::json::value& message) {
  boost::system::error_code error;

  const auto type_ptr = message.find_pointer("/type", error);
  if (error) {
    return absl::InvalidArgumentError(
        "Error parsing 'type' field in signalling message: " +
        boost::json::serialize(message));
  }

  const auto desc_ptr = message.find_pointer("/description", error);
  if (error) {
    return absl::InvalidArgumentError(
        "Error parsing 'description' field in signalling message: " +
        boost::json::serialize(message));
  }

  if (type_ptr == nullptr) {
    return absl::InvalidArgumentError(
        "No 'type' field in signalling message: " +
        boost::json::serialize(message));
  }
  if (desc_ptr == nullptr) {
    return absl::InvalidArgumentError(
        "No 'description' field in signalling message: " +
        boost::json::serialize(message));
  }

  const boost::system::result<const boost::json::string&> type_or =
      type_ptr->try_as_string();
  if (type_or.has_error()) {
    return absl::InvalidArgumentError(
        "'type' field is not a string in signalling message: " +
        boost::json::serialize(message));
  }

  const boost::system::result<const boost::json::string&> desc_or =
      desc_ptr->try_as_string();
  if (desc_or.has_error()) {
    return absl::InvalidArgumentError(
        "'description' field is not a string in signalling message: " +
        boost::json::serialize(message));
  }

  if (*type_or != "offer") {
    return absl::InvalidArgumentError("Not an offer message: " +
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

void WebRtcServer::RunLoop() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  auto connections = std::make_shared<DataChannelConnectionMap>();
  std::shared_ptr<SignallingClient> signalling_client;

  const auto channel_reader = ready_data_connections_.reader();

  int retries_remaining = 5000000;

  while (true) {
    if (signalling_client == nullptr) {
      signalling_client =
          InitSignallingClient(signalling_address_, signalling_port_,
                               signalling_use_ssl_, connections);
      absl::Status connect_status = signalling_client->ConnectWithIdentity(
          signalling_identity_, signalling_headers_);
      if (!connect_status.ok()) {
        LOG(ERROR) << "WebRtcServer failed to connect to "
                      "signalling server: "
                   << connect_status;
        mu_.unlock();
        act::SleepFor(absl::Seconds(0.5));
        mu_.lock();
        signalling_client = nullptr;
        --retries_remaining;
        continue;
      }
    }

    absl::StatusOr<WebRtcDataChannelConnection> next_connection_or;
    bool channel_open;

    // LOG(INFO) << "WebRtcServer waiting for new connections...";
    mu_.unlock();
    const int selected = thread::Select(
        {channel_reader->OnRead(&next_connection_or, &channel_open),
         signalling_client->OnError(), thread::OnCancel()});
    mu_.lock();
    // LOG(INFO) << "WebRtcServer woke up from wait.";

    // Check if our fiber has been cancelled, which means we should stop.
    if (thread::Cancelled()) {
      break;
    }

    // If the signalling client has an error, we need to restart it while
    // we still have retries left.
    if (selected == 1) {
      if (retries_remaining <= 0) {
        LOG(ERROR) << "WebRtcServer signalling client error: "
                   << signalling_client->GetStatus()
                   << ". No more retries left. Exiting.";
        break;
      }
      const absl::Status signalling_status = signalling_client->GetStatus();
      if (absl::IsCancelled(signalling_status)) {
        DLOG(INFO) << "WebRtcServer signalling client cancelled: "
                   << signalling_status << ". Restarting in 0.5 seconds.";
      } else {
        LOG(ERROR) << "WebRtcServer signalling client error: "
                   << signalling_status << ". Restarting in 0.5 seconds.";
      }
      signalling_client->ResetCallbacks();
      signalling_client->Cancel();
      mu_.unlock();
      signalling_client->Join();  // Wait for the old client to actually stop
      mu_.lock();
      signalling_client = nullptr;
      act::SleepFor(absl::Seconds(0.5));

      --retries_remaining;
      continue;
    }

    // This happens when the channel is closed for writing and does not have
    // any more data to read.
    if (!channel_open) {
      LOG(INFO) << "WebRtcServer RunLoop was cancelled by externally "
                   "closing the channel for new connections.";
    }

    if (!next_connection_or.status().ok()) {
      if (absl::IsCancelled(next_connection_or.status())) {
        DLOG(INFO) << "WebRtcServer RunLoop connection channel cancelled: "
                   << next_connection_or.status();
      } else {
        LOG(ERROR) << "WebRtcServer RunLoop received an error while waiting "
                      "for a new connection: "
                   << next_connection_or.status();
      }
      continue;
    }

    WebRtcDataChannelConnection next_connection =
        *std::move(next_connection_or);

    if (next_connection.connection->state() ==
            rtc::PeerConnection::State::Failed ||
        next_connection.connection->state() ==
            rtc::PeerConnection::State::Closed) {
      LOG(ERROR) << "WebRtcServer RunLoop could not accept a new "
                    "connection because the connection is in a failed or "
                    "closed state.";
      continue;
    }

    if (next_connection.data_channel == nullptr) {
      LOG(ERROR) << "WebRtcServer RunLoop received a connection without"
                    "a data channel. This should not happen.";
      continue;
    }

    auto stream = std::make_unique<WebRtcWireStream>(
        std::move(next_connection.data_channel),
        std::move(next_connection.connection));

    // DLOG(INFO) << "WebRtcServer received a new connection, "
    //               "establishing with the service...";
    std::string stream_id = stream->GetId();
    absl::Status est_status = service_->StartStreamHandler(std::move(stream));
    if (!est_status.ok()) {
      LOG(ERROR) << "WebRtcServer failed to start stream handler: "
                 << est_status;
      continue;
    } else {
      DLOG(INFO) << "WebRtcServer established a new connection from peer: "
                 << stream_id;
    }

    // At this point, the connection is established and the responsibility
    // of the WebRtcServer is done. The service will handle the
    // connection from here on out.
  }

  // Very important: callbacks for the signalling client must be reset
  // because their closures contain shared pointers to the client itself.
  // If we don't reset them, the client will never be destroyed.
  signalling_client->ResetCallbacks();
  DCHECK(signalling_client.use_count() == 1)
      << "WebRtcServer signalling client should be the only owner of "
         "the SignallingClient instance at this point, but it is not. "
         "This indicates a bug in the code.";
  signalling_client.reset();

  // Clean up all connections that might not be ready yet.
  for (auto& [peer_id, connection] : *connections) {
    if (connection.data_channel) {
      connection.data_channel->resetCallbacks();
      connection.data_channel->close();
    }
    if (connection.connection) {
      connection.connection->resetCallbacks();
      connection.connection->close();
    }
  }
  connections->clear();
}

std::shared_ptr<SignallingClient> WebRtcServer::InitSignallingClient(
    std::string_view signalling_address, uint16_t signalling_port, bool use_ssl,
    const std::shared_ptr<DataChannelConnectionMap>& connections) {
  auto signalling_client = std::make_shared<SignallingClient>(
      signalling_address, signalling_port, use_ssl);

  auto abort_establishment_with_error = [connections, this](
                                            std::string_view peer_id,
                                            const absl::Status& status) {
    // act::MutexLock lock(&mu_);
    DCHECK(!status.ok()) << "abort_establishment_with_error called with an OK "
                            "status, this should not "
                            "happen.";
    if (const auto it = connections->find(peer_id); it != connections->end()) {
      if (it->second.data_channel) {
        it->second.data_channel->close();
      }
      it->second.connection->resetCallbacks();
      it->second.connection->close();
    }
    ready_data_connections_.writer()->Write(status);
  };

  std::weak_ptr weak_signalling = signalling_client;

  signalling_client->OnOffer([this, connections, weak_signalling,
                              abort_establishment_with_error](
                                 std::string_view peer_id,
                                 const boost::json::value& message) {
    auto signalling_client_locked = weak_signalling.lock();
    if (!signalling_client_locked) {
      return;
    }

    act::MutexLock lock(&mu_);
    if (connections->contains(std::string(peer_id))) {
      abort_establishment_with_error(
          peer_id,
          absl::AlreadyExistsError(absl::StrFormat(
              "Connection with peer ID '%s' already exists.", peer_id)));
      return;
    }

    absl::StatusOr<rtc::Description> description =
        ParseDescriptionFromOfferMessage(message);
    if (!description.ok()) {
      abort_establishment_with_error(peer_id, description.status());
      return;
    }

    RtcConfig config = rtc_config_.value_or(RtcConfig());
    config.enable_ice_udp_mux = true;
    if (config.stun_servers.empty()) {
      config.stun_servers.emplace_back("stun:stun.l.google.com:19302");
    }
    absl::StatusOr<rtc::Configuration> libdatachannel_config =
        config.BuildLibdatachannelConfig();
    if (!libdatachannel_config.ok()) {
      abort_establishment_with_error(peer_id, libdatachannel_config.status());
      return;
    }
    const std::pair<uint16_t, uint16_t> port_range =
        config.preferred_port_range.value_or(std::pair(1024, 65535));
    libdatachannel_config->bindAddress = address_;
    libdatachannel_config->portRangeBegin = port_range.first;
    libdatachannel_config->portRangeEnd = port_range.second;

    auto connection = std::make_unique<rtc::PeerConnection>(
        *std::move(libdatachannel_config));

    rtc::PeerConnection* connection_ptr = connection.get();

    connections->emplace(peer_id, WebRtcDataChannelConnection{
                                      .connection = std::move(connection),
                                      .data_channel = nullptr});

    connection_ptr->onLocalDescription(
        [peer_id = std::string(peer_id), weak_signalling,
         abort_establishment_with_error,
         this](const rtc::Description& local_description) {
          act::MutexLock lock(&mu_);
          auto signalling_client_locked = weak_signalling.lock();
          if (!signalling_client_locked) {
            return;
          }
          const std::string answer_message =
              MakeAnswerMessage(peer_id, local_description);
          if (const auto answer_status =
                  signalling_client_locked->Send(answer_message);
              !answer_status.ok()) {
            abort_establishment_with_error(peer_id, answer_status);
          }
        });

    connection_ptr->onLocalCandidate([peer_id = std::string(peer_id),
                                      weak_signalling,
                                      abort_establishment_with_error,
                                      this](const rtc::Candidate& candidate) {
      act::MutexLock lock(&mu_);
      auto signalling_client_locked = weak_signalling.lock();
      if (!signalling_client_locked) {
        return;
      }
      const std::string candidate_message =
          MakeCandidateMessage(peer_id, candidate);
      if (const auto candidate_status =
              signalling_client_locked->Send(candidate_message);
          !candidate_status.ok()) {
        abort_establishment_with_error(peer_id, candidate_status);
      }
    });

    connection_ptr->onIceStateChange([peer_id = std::string(peer_id),
                                      connection_ptr,
                                      abort_establishment_with_error](
                                         rtc::PeerConnection::IceState state) {
      // Unsuccessful connections should be cleaned up: data channel closed,
      // if any, and connection closed.
      if (state == rtc::PeerConnection::IceState::Failed) {
        abort_establishment_with_error(
            peer_id,
            absl::InternalError(absl::StrFormat(
                "WebRtcServer connection with peer ID '%s' failed.", peer_id)));
        return;
      }

      // Successful connections should have their callbacks reset, as the
      // callbacks' closures contain shared pointers to the signalling client,
      // which would prevent it from being destroyed.
      if (state == rtc::PeerConnection::IceState::Completed ||
          state == rtc::PeerConnection::IceState::Connected) {
        connection_ptr->onLocalDescription([](const rtc::Description&) {});
        connection_ptr->onLocalCandidate([](const rtc::Candidate&) {});
        connection_ptr->onIceStateChange([](rtc::PeerConnection::IceState) {});
      }
    });

    connection_ptr->onDataChannel(
        [this, peer_id = std::string(peer_id),
         connections](std::shared_ptr<rtc::DataChannel> dc) {
          act::MutexLock lock(&mu_);
          const auto map_node = connections->extract(peer_id);
          DCHECK(!map_node.empty())
              << "WebRtcServer no connection for peer: " << peer_id;

          WebRtcDataChannelConnection connection_from_map =
              std::move(map_node.mapped());
          connection_from_map.data_channel = std::move(dc);

          ready_data_connections_.writer()->WriteUnlessCancelled(
              std::move(connection_from_map));
        });

    const auto it = connections->find(peer_id);
    if (it == connections->end()) {
      abort_establishment_with_error(
          peer_id,
          absl::NotFoundError(absl::StrFormat(
              "Connection with peer ID '%s' not found after insertion.",
              peer_id)));
      return;
    }
    try {
      it->second.connection->setRemoteDescription(*std::move(description));
    } catch (const std::exception& exc) {
      abort_establishment_with_error(
          peer_id, absl::InternalError(absl::StrFormat(
                       "Error setting remote description: %s", exc.what())));
    }
  });

  signalling_client->OnCandidate([connections, abort_establishment_with_error,
                                  this](std::string_view peer_id,
                                        const boost::json::value& message) {
    {
      act::MutexLock lock(&mu_);
      if (!connections->contains(peer_id)) {
        DLOG(WARNING) << "Received candidate for unknown peer ID: " << peer_id;
        return;
      }
    }

    absl::StatusOr<rtc::Candidate> candidate =
        ParseCandidateFromMessage(message);
    if (!candidate.ok()) {
      abort_establishment_with_error(peer_id, candidate.status());
      return;
    }

    {
      act::MutexLock lock(&mu_);
      const auto it = connections->find(peer_id);
      if (it == connections->end()) {
        abort_establishment_with_error(
            peer_id, absl::NotFoundError(absl::StrFormat(
                         "Connection with peer ID '%s' not found.", peer_id)));
        return;
      }
      try {
        it->second.connection->addRemoteCandidate(*std::move(candidate));
      } catch (const std::exception& exc) {
        abort_establishment_with_error(
            peer_id, absl::InternalError(absl::StrFormat(
                         "Error adding remote candidate: %s", exc.what())));
      }
    }
  });

  return signalling_client;
}

}  // namespace act::net