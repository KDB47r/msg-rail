cmake_minimum_required(VERSION 3.14)

add_executable(logic_server
  main.cpp
  grpc_service.cpp
  http_server.cpp
  http_handlers_auth.cpp
  http_handlers_message.cpp
  http_handlers_user.cpp
  http_handlers_friend.cpp
  http_handlers_chatroom.cpp
  http_handlers_danmaku.cpp
  http_handlers_admin.cpp
  http_handlers_media.cpp
  user_dao.cpp
  friend_dao.cpp
  friend_service.cpp
  redis_store.cpp
  media_cache.cpp
  media_read.cpp
  danmaku_dao.cpp
  group_dao.cpp
  session_dao.cpp
  message_dao.cpp
  user_session_state_dao.cpp
  conversation_store.cpp
)

target_link_libraries(logic_server
  PRIVATE
    spark_push_core
    spark_push_mysql
    spark_push_redis
    spark_push_kafka
    spark_push_http
    spark_push_proto
    spark_push_media
    spark_push_s3
    pthread
)



