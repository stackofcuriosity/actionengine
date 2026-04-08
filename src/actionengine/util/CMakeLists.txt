add_library(
        actUtilBoostAsio STATIC
        boost_asio_utils.h
        boost_asio_utils.cc
)
add_library(
        act::util::boost_asio ALIAS
        actUtilBoostAsio
)
target_include_directories(
        actUtilBoostAsio PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${Boost_INCLUDE_DIRS}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actUtilBoostAsio PRIVATE
        absl::base
        act::concurrency
        Boost::asio

)
# --------------------------------------------------------------------------
add_library(
        actUtilBoostJson STATIC
        boost_json.h
        boost_json.cc
)
add_library(
        act::util::boost_json ALIAS
        actUtilBoostJson
)
target_include_directories(
        actUtilBoostJson PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${Boost_INCLUDE_DIRS}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actUtilBoostJson PUBLIC
        Boost::json
)
target_link_libraries(
        actUtilBoostJson PRIVATE
        absl::status
)
# --------------------------------------------------------------------------
add_library(
        actUtil STATIC
        source_location.h
        status_builder.cc
        status_builder.h
        status_macros.h
)
add_library(
        act::util ALIAS
        actUtil
)
target_include_directories(
        actUtil PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actUtil PRIVATE
        absl::base
        absl::inlined_vector
        absl::log
)
# --------------------------------------------------------------------------
add_library(
        actUtilPybind11 STATIC
)
add_library(
        act::util::pybind11 ALIAS
        actUtilPybind11
)
target_sources(
        actUtilPybind11 PRIVATE
        utils_pybind11.cc
)
target_sources(
        actUtilPybind11 PUBLIC
        utils_pybind11.h
)
target_link_libraries(
        actUtilPybind11 PRIVATE
        ${ACTIONENGINE_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        actUtilPybind11 PUBLIC
        ${ACTIONENGINE_PYBIND11_INCLUDE_DIRS}
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
# --------------------------------------------------------------------------
add_library(
        actUtilGlobalSettings STATIC
        global_settings.cc
        global_settings.h
)
add_library(
        act::util::global_settings ALIAS
        actUtilGlobalSettings
)
target_include_directories(
        actUtilGlobalSettings PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actUtilGlobalSettings PRIVATE
        absl::base
        absl::log
        absl::time
)
# --------------------------------------------------------------------------
add_library(
        actUtilGlobalSettingsPybind11 STATIC
)
add_library(
        act::util::global_settings::pybind11 ALIAS
        actUtilGlobalSettingsPybind11
)
target_sources(
        actUtilGlobalSettingsPybind11 PRIVATE
        global_settings_pybind11.cc
)
target_sources(
        actUtilGlobalSettingsPybind11 PUBLIC
        global_settings_pybind11.h
)
target_link_libraries(
        actUtilGlobalSettingsPybind11 PRIVATE
        ${ACTIONENGINE_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        actUtilGlobalSettingsPybind11 PUBLIC
        ${ACTIONENGINE_PYBIND11_INCLUDE_DIRS}
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)