PROJECT(lspf)

CMAKE_MINIMUM_REQUIRED(VERSION 3.5)

add_compile_options(-std=c++0x)

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH}" "${CMAKE_LOCAL_PATH}/cmake")

message("Build CMAKE_MODULE_PATH:" ${CMAKE_MODULE_PATH})

include(DefineOptions)

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} SRC_LIST)

add_library(redisclient STATIC ${SRC_LIST})

set(HEADERS redisclient.hpp redislock.hpp)

install(FILES ${HEADERS} DESTINATION ${INCLUDE_OUTPUT_PATH}/include)

