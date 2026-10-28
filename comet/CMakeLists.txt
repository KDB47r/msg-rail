cmake_minimum_required(VERSION 3.14)

add_executable(comet_server
  main.cpp
  app.cpp
  comet_server.cpp
  comet_grpc_service.cpp
  websocket_utils.cpp
  logic_stream_client.cpp
  conn_registry.cpp
)

find_package(nlohmann_json 3 REQUIRED)

target_link_libraries(comet_server
  PRIVATE
    spark_push_core
    spark_push_proto
    grpc++
    grpc
    pthread
    nlohmann_json::nlohmann_json
    websocketpp
)
