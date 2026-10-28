cmake_minimum_required(VERSION 3.14)
project(spark_push LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# 生成 compile_commands.json 文件，供 IDE 或静态分析工具（如 clang-tidy）使用
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# -Wno-unused-parameter 抑制 gRPC 生成代码的刷屏
add_compile_options(-Wall -Wextra -Wno-unused-parameter)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

find_package(Threads REQUIRED)

# 课程依赖由安装手册预先安装到 /usr/local；项目构建不下载或编译第三方软件。
find_package(spdlog 1.17.0 CONFIG REQUIRED
  PATHS /usr/local/lib/cmake/spdlog
  NO_DEFAULT_PATH)
find_package(llhttp CONFIG REQUIRED
  PATHS /usr/local/lib/cmake/llhttp
  NO_DEFAULT_PATH)
file(STRINGS "/usr/local/include/llhttp.h" _sparkpush_llhttp_version_lines
  REGEX "^#define LLHTTP_VERSION_(MAJOR|MINOR|PATCH) [0-9]+$")
foreach(_sparkpush_llhttp_part MAJOR MINOR PATCH)
  string(REGEX MATCH "#define LLHTTP_VERSION_${_sparkpush_llhttp_part} ([0-9]+)"
    _sparkpush_llhttp_match "${_sparkpush_llhttp_version_lines}")
  set(_sparkpush_llhttp_${_sparkpush_llhttp_part} "${CMAKE_MATCH_1}")
endforeach()
set(_sparkpush_llhttp_version
  "${_sparkpush_llhttp_MAJOR}.${_sparkpush_llhttp_MINOR}.${_sparkpush_llhttp_PATCH}")
if(NOT _sparkpush_llhttp_version STREQUAL "9.2.1")
  message(FATAL_ERROR
    "Spark Push requires llhttp 9.2.1 in /usr/local, but found ${_sparkpush_llhttp_version}.")
endif()

find_path(SPARKPUSH_ASIO_INCLUDE_DIR asio.hpp
  PATHS /usr/local/include
  NO_DEFAULT_PATH
  REQUIRED)
file(STRINGS "${SPARKPUSH_ASIO_INCLUDE_DIR}/asio/version.hpp" _sparkpush_asio_version_line
  REGEX "^#define ASIO_VERSION [0-9]+")
string(REGEX MATCH "#define ASIO_VERSION ([0-9]+)"
  _sparkpush_asio_version_match "${_sparkpush_asio_version_line}")
if(NOT CMAKE_MATCH_1 STREQUAL "103002")
  message(FATAL_ERROR
    "Spark Push requires Asio 1.30.2 in /usr/local, but found version code ${CMAKE_MATCH_1}.")
endif()
add_library(asio INTERFACE)
target_include_directories(asio INTERFACE ${SPARKPUSH_ASIO_INCLUDE_DIR})
target_compile_definitions(asio INTERFACE ASIO_STANDALONE)
target_link_libraries(asio INTERFACE Threads::Threads)

find_path(SPARKPUSH_WEBSOCKETPP_INCLUDE_DIR websocketpp/config/core.hpp
  PATHS /usr/local/include
  NO_DEFAULT_PATH
  REQUIRED)
file(STRINGS "${SPARKPUSH_WEBSOCKETPP_INCLUDE_DIR}/websocketpp/version.hpp" _sparkpush_websocketpp_version_line
  REGEX "WebSocket\\+\\+/0\\.8\\.2")
if(NOT _sparkpush_websocketpp_version_line)
  message(FATAL_ERROR
    "Spark Push requires websocketpp 0.8.2 in /usr/local.")
endif()
add_library(websocketpp INTERFACE)
target_include_directories(websocketpp INTERFACE ${SPARKPUSH_WEBSOCKETPP_INCLUDE_DIR})
target_compile_definitions(websocketpp INTERFACE
  _WEBSOCKETPP_CPP11_STL_
  ASIO_STANDALONE)
target_link_libraries(websocketpp INTERFACE asio)

add_subdirectory(proto)
add_subdirectory(common)
add_subdirectory(logic)
add_subdirectory(comet)
add_subdirectory(job)

option(BUILD_WEB_DEMO "Build web demo (static files only)" ON)
if(BUILD_WEB_DEMO)
  add_subdirectory(web_demo)
endif()

add_subdirectory(load_test)

option(BUILD_TESTS "Build unit tests" ON)
if(BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
