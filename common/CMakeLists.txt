cmake_minimum_required(VERSION 3.14)

find_package(Threads REQUIRED)

function(spark_push_common_includes target)
  target_include_directories(${target}
    PUBLIC
      ${CMAKE_CURRENT_SOURCE_DIR}
  )
endfunction()

add_library(spark_push_core
  config.cpp
  logging.cpp
  local_timer.cpp
  thread_pool.cpp
  timer_wheel.cpp
)
spark_push_common_includes(spark_push_core)
target_link_libraries(spark_push_core
  PRIVATE
    Threads::Threads
    spdlog::spdlog_header_only
)

add_library(spark_push_mysql
  mysql_pool.cpp
)
spark_push_common_includes(spark_push_mysql)
target_link_libraries(spark_push_mysql
  PRIVATE
    spark_push_core
    mysqlclient
)

add_library(spark_push_redis
  redis_pool.cpp
)
spark_push_common_includes(spark_push_redis)
target_link_libraries(spark_push_redis
  PRIVATE
    spark_push_core
    hiredis
)

add_library(spark_push_kafka
  kafka_producer.cpp
  kafka_consumer.cpp
)
spark_push_common_includes(spark_push_kafka)
target_link_libraries(spark_push_kafka
  PRIVATE
    spark_push_core
    rdkafka++
    rdkafka
)

add_library(spark_push_http
  net/http_server_lite.cpp
)
spark_push_common_includes(spark_push_http)
target_link_libraries(spark_push_http
  PRIVATE
    asio
    llhttp::llhttp_static
    Threads::Threads
)

# 仓外兼容入口；仓内目标按需链接上述细分组件。
add_library(spark_push_common INTERFACE)
target_link_libraries(spark_push_common
  INTERFACE
    spark_push_core
    spark_push_mysql
    spark_push_redis
    spark_push_kafka
    spark_push_http
)
