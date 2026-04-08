add_library(
        actNetAsioWebsockets STATIC
        fiber_aware_websocket_stream.h
        fiber_aware_websocket_stream.cc
)
add_library(act::net::asio_websockets ALIAS actNetAsioWebsockets)
target_include_directories(
        actNetAsioWebsockets PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actNetAsioWebsockets PRIVATE
)
target_link_libraries(
        actNetAsioWebsockets PUBLIC
        cppack
        act::nodes
        Boost::asio
        Boost::beast
        act::util::boost_asio
        OpenSSL::SSL
        OpenSSL::Crypto
)