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

#include "actionengine/net/websockets/fiber_aware_websocket_stream.h"

#include <variant>

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>

#include "actionengine/net/http/ws_common.h"
#include "actionengine/util/boost_asio_utils.h"
#include "actionengine/util/status_macros.h"

namespace act::net {

static constexpr std::string_view kVersionString = "Action Engine 0.4.0";
static constexpr absl::Duration kDebugWarningTimeout = absl::Seconds(5);

BoostWebsocketStream::BoostWebsocketStream(
    boost::asio::ip::tcp::socket&& socket)
    : stream_(PlainStream(std::move(socket))) {}

BoostWebsocketStream::BoostWebsocketStream(
    boost::asio::ssl::stream<boost::asio::ip::tcp::socket>&& stream)
    : stream_(SslStream(std::move(stream))) {}

void BoostWebsocketStream::binary(bool value) {
  std::visit([value](auto& s) { s.binary(value); }, stream_);
}

void BoostWebsocketStream::text(bool value) {
  std::visit([value](auto& s) { s.text(value); }, stream_);
}

bool BoostWebsocketStream::got_text() const {
  return std::visit([](auto& s) { return s.got_text(); }, stream_);
}

bool BoostWebsocketStream::is_open() const {
  return std::visit([](auto& s) { return s.is_open(); }, stream_);
}

void BoostWebsocketStream::write_buffer_bytes(std::size_t bytes) {
  std::visit([bytes](auto& s) { s.write_buffer_bytes(bytes); }, stream_);
}

boost::asio::ip::tcp::socket& BoostWebsocketStream::next_layer() {
  if (std::holds_alternative<PlainStream>(stream_)) {
    return std::get<PlainStream>(stream_).next_layer();
  }
  return std::get<SslStream>(stream_).next_layer().next_layer();
}

const boost::asio::ip::tcp::socket& BoostWebsocketStream::next_layer() const {
  if (std::holds_alternative<PlainStream>(stream_)) {
    return std::get<PlainStream>(stream_).next_layer();
  }
  return std::get<SslStream>(stream_).next_layer().next_layer();
}

void BoostWebsocketStream::handshake(std::string_view host,
                                     std::string_view target,
                                     boost::system::error_code& ec) {
  std::visit([&](auto& s) { s.handshake(host, target, ec); }, stream_);
}

void BoostWebsocketStream::async_ssl_handshake(
    boost::asio::ssl::stream_base::handshake_type type,
    std::function<void(boost::system::error_code)> cb) {
  if (std::holds_alternative<SslStream>(stream_)) {
    std::get<SslStream>(stream_).next_layer().async_handshake(type,
                                                              std::move(cb));
  } else {
    // should not happen if logic is correct
    cb(boost::asio::error::no_protocol_option);
  }
}

BoostWebsocketStream::SslStream* absl_nullable
BoostWebsocketStream::GetSslStreamOrNull() {
  if (auto* ssl_stream = std::get_if<SslStream>(&stream_)) {
    return ssl_stream;
  }
  return nullptr;
}

absl::Status PrepareClientStream(
    BoostWebsocketStream* stream,
    absl::AnyInvocable<void(boost::beast::websocket::request_type&)>
        decorator) {
  stream->set_option(boost::beast::websocket::stream_base::decorator(
      [decorator = std::move(decorator)](
          boost::beast::websocket::request_type& req) mutable {
        req.set(boost::beast::http::field::user_agent,
                absl::StrCat(kVersionString, "WebsocketWireStream client"));
        if (decorator) {
          std::move(decorator)(req);
        }
      }));
  stream->write_buffer_bytes(16);

  stream->set_option(boost::beast::websocket::stream_base::timeout{
      std::chrono::seconds(30), std::chrono::seconds(1800), true});

  boost::beast::websocket::permessage_deflate permessage_deflate_option;
  permessage_deflate_option.msg_size_threshold = 1024;  // 1 KiB
  permessage_deflate_option.server_enable = true;
  permessage_deflate_option.client_enable = true;
  stream->set_option(permessage_deflate_option);

  return absl::OkStatus();
}

absl::Status PrepareServerStream(BoostWebsocketStream* stream) {
  stream->set_option(boost::beast::websocket::stream_base::decorator(
      [](boost::beast::websocket::request_type& req) {
        req.set(boost::beast::http::field::user_agent,
                absl::StrCat(kVersionString, "WebsocketWireStream server"));
      }));
  stream->write_buffer_bytes(16);

  stream->set_option(boost::beast::websocket::stream_base::timeout{
      std::chrono::seconds(30), std::chrono::seconds(1800), true});

  boost::beast::websocket::permessage_deflate permessage_deflate_option;
  permessage_deflate_option.msg_size_threshold = 1024;  // 1 KiB
  permessage_deflate_option.server_enable = true;
  permessage_deflate_option.client_enable = true;
  stream->set_option(permessage_deflate_option);

  return absl::OkStatus();
}

FiberAwareWebsocketStream::FiberAwareWebsocketStream(
    std::unique_ptr<BoostWebsocketStream> stream,
    PerformHandshakeFn handshake_fn)
    : stream_(std::move(stream)), handshake_fn_(std::move(handshake_fn)) {}

FiberAwareWebsocketStream::~FiberAwareWebsocketStream() {
  act::MutexLock lock(&mu_);

  cancel_signal_.emit(boost::asio::cancellation_type::total);

  bool timeout_logged = false;
  while (write_pending_ || read_pending_) {
    cv_.WaitWithTimeout(&mu_, kDebugWarningTimeout);
    if ((write_pending_ || read_pending_) && !timeout_logged) {
      LOG(WARNING) << "FiberAwareWebsocketStream destructor waiting for "
                      "pending operations to finish. You may have "
                      "forgotten to call Close() on the stream.";
      timeout_logged = true;
    }
  }

  if (stream_ == nullptr) {
    return;  // Stream already closed or moved
  }

  CloseInternal()
      .IgnoreError();  // Close the stream gracefully, ignoring errors
}

absl::StatusOr<std::unique_ptr<FiberAwareWebsocketStream>>
FiberAwareWebsocketStream::Connect(std::string_view address, uint16_t port,
                                   std::string_view target,
                                   PrepareStreamFn prepare_stream_fn,
                                   bool use_ssl) {
  return Connect(*util::GetDefaultAsioExecutionContext(), address, port, target,
                 std::move(prepare_stream_fn), use_ssl);
}

BoostWebsocketStream& FiberAwareWebsocketStream::GetStream() const {
  return *stream_;
}

absl::Status FiberAwareWebsocketStream::Write(
    const std::vector<uint8_t>& message_bytes) const {
  act::MutexLock lock(&mu_);
  return WriteBytesInternal(message_bytes, /*text=*/false);
}

absl::Status FiberAwareWebsocketStream::WriteText(
    const std::string& message) const {
  act::MutexLock lock(&mu_);
  const std::vector<uint8_t> message_bytes(message.begin(), message.end());
  return WriteBytesInternal(message_bytes, /*text=*/true);
}

absl::Status FiberAwareWebsocketStream::Close(absl::Status status) {
  act::MutexLock lock(&mu_);
  return CloseInternal(std::move(status));
}

absl::Status FiberAwareWebsocketStream::WriteBytesInternal(
    const std::vector<uint8_t>& message_bytes, bool text) const {
  while (write_pending_) {
    cv_.Wait(&mu_);
  }
  if (!stream_->is_open()) {
    return absl::FailedPreconditionError(
        "Websocket stream is not open for writing");
  }
  write_pending_ = true;

  auto write_done = std::make_shared<AsioDone>();
  if (!text) {
    stream_->binary(true);
  } else {
    stream_->text(true);
  }
  stream_->async_write(
      boost::asio::buffer(message_bytes),
      [write_done](const boost::system::error_code& ec, std::size_t) {
        write_done->error = ec;
        write_done->event.Notify();
      });

  mu_.unlock();
  thread::Select({write_done->event.OnEvent()});
  mu_.lock();
  write_pending_ = false;
  cv_.SignalAll();

  if (thread::Cancelled()) {
    if (stream_->is_open()) {
      stream_->next_layer().shutdown(boost::asio::socket_base::shutdown_send,
                                     write_done->error);
    }

    return absl::CancelledError("WsWrite cancelled");
  }

  if (write_done->error) {
    if (write_done->error == boost::system::errc::operation_canceled) {
      return absl::CancelledError("WsWrite cancelled");
    }
    LOG(ERROR) << absl::StrFormat("Cannot write to websocket stream: %v",
                                  write_done->error.message());
    return absl::InternalError(write_done->error.message());
  }

  return absl::OkStatus();
}

absl::Status FiberAwareWebsocketStream::CloseInternal(absl::Status status)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  // TODO(hpnkv): Find out how to cancel Read to be able to close the stream
  //   w/ a graceful WebSocket close code.

  // Already closed or moved
  if (stream_ == nullptr) {
    return absl::OkStatus();
  }

  if (!stream_->is_open()) {
    return absl::OkStatus();
  }
  while (write_pending_) {
    cv_.Wait(&mu_);
  }

  cancel_signal_.emit(boost::asio::cancellation_type::total);
  // while (read_pending_) {
  //   cv_.Wait(&mu_);
  // }

  write_pending_ = true;

  auto close_done = std::make_shared<AsioDone>();
  stream_->async_close(
      status.ok() ? boost::beast::websocket::close_code::normal
                  : boost::beast::websocket::close_code::internal_error,
      [close_done](const boost::system::error_code& async_error) {
        close_done->error = async_error;
        close_done->event.Notify();
      });

  mu_.unlock();
  thread::Select({close_done->event.OnEvent()});
  mu_.lock();
  write_pending_ = false;
  cv_.SignalAll();

  if (close_done->error) {
    LOG(ERROR) << absl::StrFormat("Cannot close websocket stream: %v",
                                  close_done->error.message());
  }

  return absl::OkStatus();
}

absl::Status ResolveAndConnect(BoostWebsocketStream* stream,
                               std::string_view address, uint16_t port) {
  return ResolveAndConnect(*util::GetDefaultAsioExecutionContext(), stream,
                           address, port);
}

absl::Status DoHandshake(BoostWebsocketStream* stream, std::string_view host,
                         std::string_view target) {
  boost::system::error_code error;
  stream->handshake(host, target, error);

  if (error == boost::beast::websocket::error::closed ||
      error == boost::system::errc::operation_canceled) {
    return absl::CancelledError("WsHandshake cancelled");
  }
  if (error) {
    return absl::InternalError(error.message());
  }
  return absl::OkStatus();
}

absl::Status FiberAwareWebsocketStream::Accept() {
  act::MutexLock lock(&mu_);
  stream_->set_option(boost::beast::websocket::stream_base::decorator(
      [](boost::beast::websocket::response_type& res) {
        res.set(boost::beast::http::field::server,
                absl::StrCat(kVersionString, "WebsocketActionEngineServer"));
      }));
  stream_->write_buffer_bytes(16);

  auto accept_done = std::make_shared<AsioDone>();

  stream_->async_accept([accept_done](const boost::system::error_code& ec) {
    accept_done->error = ec;
    accept_done->event.Notify();
  });

  mu_.unlock();
  thread::Select({accept_done->event.OnEvent(), thread::OnCancel()});
  mu_.lock();

  boost::system::error_code error = accept_done->error;

  if (thread::Cancelled()) {
    if (stream_->is_open()) {
      stream_->next_layer().shutdown(boost::asio::socket_base::shutdown_receive,
                                     error);
      if (error && error != boost::asio::error::not_connected) {
        LOG(ERROR) << absl::StrFormat(
            "Cannot shut down receive on websocket stream: %v",
            error.message());
      }
    }

    return absl::CancelledError("WsAccept cancelled");
  }

  if (error == boost::beast::websocket::error::closed ||
      error == boost::system::errc::operation_canceled) {
    return absl::CancelledError("WsAccept cancelled");
  }

  if (error) {
    LOG(ERROR) << absl::StrFormat("Cannot accept websocket stream: %v",
                                  error.message());
    return absl::InternalError(error.message());
  }

  return absl::OkStatus();
}

absl::Status FiberAwareWebsocketStream::Read(
    absl::Duration timeout,
    std::optional<std::vector<uint8_t>>* absl_nonnull buffer,
    bool* absl_nullable got_text) {
  const absl::Time deadline = absl::Now() + timeout;

  act::MutexLock lock(&mu_);

  while (read_pending_) {
    if (!cv_.WaitWithDeadline(&mu_, deadline)) {
      return absl::DeadlineExceededError(
          "FiberAwareWebsocketStream Read operation timed out waiting for "
          "another read to complete.");
    }
  }

  if (!stream_->is_open()) {
    return absl::FailedPreconditionError(
        "Websocket stream is not open for reading");
  }

  read_pending_ = true;

  std::vector<uint8_t> temp_buffer;
  temp_buffer.reserve(64);  // Reserve some space to avoid some reallocations
  auto dynamic_buffer = boost::asio::dynamic_buffer(temp_buffer);

  auto read_done = std::make_shared<AsioDone>();
  stream_->async_read(
      dynamic_buffer,
      boost::asio::bind_cancellation_slot(
          cancel_signal_.slot(),
          [read_done](const boost::system::error_code& ec, std::size_t) {
            read_done->error = ec;
            read_done->event.Notify();
          }));

  mu_.unlock();
  const int selected =
      thread::SelectUntil(deadline, {read_done->event.OnEvent()});
  mu_.lock();

  if (selected == -1) {
    cancel_signal_.emit(boost::asio::cancellation_type::total);
    mu_.unlock();
    // We still need to wait for the read_done event to be processed because
    // it is a local variable, and we need to ensure that the callback has
    // completed before we return to avoid memory corruption.
    thread::Select({read_done->event.OnEvent()});
    mu_.lock();
  }

  boost::system::error_code error = read_done->error;

  // Only here we can safely let other read operations proceed.
  read_pending_ = false;
  cv_.SignalAll();

  // Only after we have notified other threads that the read is done,
  // we can return if the read was cancelled or timed out.
  if (selected == -1) {
    // Timed out.
    return absl::DeadlineExceededError(
        "FiberAwareWebsocketStream Read operation timed out.");
  }

  // Finally, we can move the received data to the output buffer.
  *buffer = std::move(temp_buffer);

  if (got_text != nullptr) {
    *got_text = stream_->got_text();
  }
  if (!error) {
    return absl::OkStatus();
  }

  if (error == boost::beast::websocket::error::closed ||
      error == boost::system::errc::operation_canceled ||
      error == boost::asio::error::eof) {
    return absl::CancelledError("WsRead cancelled");
  }

  if (error) {
    DLOG(ERROR) << absl::StrFormat("Cannot read from websocket stream: %v",
                                   error.message());
    return absl::InternalError(error.message());
  }

  return absl::OkStatus();
}

absl::Status FiberAwareWebsocketStream::ReadText(
    absl::Duration timeout, std::optional<std::string>* absl_nonnull buffer) {
  bool got_text = false;
  std::optional<std::vector<uint8_t>> temp_buffer;

  RETURN_IF_ERROR(
      Read(timeout, &temp_buffer, &got_text));  // Reuse the Read method.

  if (!temp_buffer) {
    *buffer = std::nullopt;
  }

  if (!got_text) {
    return absl::FailedPreconditionError(
        "Websocket stream did not receive text data");
  }

  // Convert the received bytes to a string.
  *buffer = std::string(std::make_move_iterator(temp_buffer->begin()),
                        std::make_move_iterator(temp_buffer->end()));

  return absl::OkStatus();
}

absl::Status FiberAwareWebsocketStream::Start() {
  act::MutexLock lock(&mu_);
  if (handshake_fn_) {
    return handshake_fn_(stream_.get());
  }
  return absl::OkStatus();
}

}  // namespace act::net
