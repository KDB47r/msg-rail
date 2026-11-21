cmake_minimum_required(VERSION 3.14)

# 图片编解码依赖。Ubuntu 安装命令：
#   sudo apt install libjpeg-turbo8-dev libpng-dev libwebp-dev pkg-config
find_package(PkgConfig REQUIRED)
find_package(OpenSSL REQUIRED)
pkg_check_modules(SPARKPUSH_JPEG REQUIRED IMPORTED_TARGET GLOBAL libjpeg)
pkg_check_modules(SPARKPUSH_PNG REQUIRED IMPORTED_TARGET GLOBAL libpng16)
pkg_check_modules(SPARKPUSH_WEBP REQUIRED IMPORTED_TARGET GLOBAL libwebp)

add_library(spark_push_media
  media_id.cpp
  media_dao.cpp
  image_processor.cpp
  video_processor.cpp
)
target_include_directories(spark_push_media
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(spark_push_media
  PUBLIC
    spark_push_mysql
  PRIVATE
    PkgConfig::SPARKPUSH_JPEG
    PkgConfig::SPARKPUSH_PNG
    PkgConfig::SPARKPUSH_WEBP
)

add_executable(media_worker
  media_worker_main.cpp
)
target_link_libraries(media_worker
  PRIVATE
    spark_push_media
    spark_push_s3
    spark_push_core
    spark_push_mysql
    OpenSSL::Crypto
    Threads::Threads
)
