add_library(
        actNetWebrtc STATIC
        server.cc
        server.h
        signalling_client.h
        signalling_client.cc
        wire_stream.cc
        wire_stream.h
)
add_library(act::net::webrtc ALIAS actNetWebrtc)
target_include_directories(
        actNetWebrtc PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actNetWebrtc PUBLIC
        LibDataChannel::LibDataChannel
        act::net::http
        act::net::http::proxygen
)
target_link_libraries(
        actNetWebrtc PRIVATE
        cppack
        act::concurrency
        act::data
        act::net::asio_websockets
        act::nodes
        act::stores
)

# ------------------------------------------------------------------

add_library(
        actNetWebrtcPybind11 STATIC
)
add_library(
        act::net::webrtc::pybind11 ALIAS
        actNetWebrtcPybind11
)
target_sources(
        actNetWebrtcPybind11 PRIVATE
        webrtc_pybind11.cc
)
target_sources(
        actNetWebrtcPybind11 PUBLIC
        webrtc_pybind11.h
)
target_link_libraries(
        actNetWebrtcPybind11 PUBLIC
        act::net::webrtc
        ${ACTIONENGINE_PYBIND11_DEPENDENCIES}
)
target_link_libraries(
        actNetWebrtcPybind11 PRIVATE
        act::concurrency
        act::data
        act::net::asio_websockets
        act::net::http
)
target_include_directories(
        actNetWebrtcPybind11 PUBLIC
        ${ACTIONENGINE_PYBIND11_INCLUDE_DIRS}
)