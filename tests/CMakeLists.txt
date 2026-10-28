cmake_minimum_required(VERSION 3.14)

# 单元测试可执行文件 unit_tests
# 使用 header-only 的 doctest（third_party/doctest.h），无需联网或额外安装。
#
# 说明:
#   - 直接把被测的纯函数源文件(comet/websocket_utils.cpp)编进测试,避免依赖 comet_server 目标
#   - 链接 spark_push_core 以测试 LoadConfig / 日志脱敏 / ThreadPool
find_package(nlohmann_json 3 REQUIRED)

add_executable(unit_tests
  doctest_main.cpp
  test_websocket.cpp
  test_config.cpp
  test_logging.cpp
  test_thread_pool.cpp
  test_conn_registry.cpp
  test_timer_wheel.cpp
  ${CMAKE_SOURCE_DIR}/comet/websocket_utils.cpp
  ${CMAKE_SOURCE_DIR}/comet/conn_registry.cpp
)

target_include_directories(unit_tests
  PRIVATE
    ${CMAKE_SOURCE_DIR}/third_party
    ${CMAKE_SOURCE_DIR}/common
    ${CMAKE_SOURCE_DIR}/comet
)

target_link_libraries(unit_tests
  PRIVATE
    spark_push_core
    nlohmann_json::nlohmann_json
    websocketpp
)

# doctest.h 是自动生成的大头文件,关闭对它的告警噪声
target_compile_options(unit_tests PRIVATE -Wno-unused-parameter)

add_test(NAME unit_tests COMMAND unit_tests)
