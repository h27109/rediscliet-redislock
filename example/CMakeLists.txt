PROJECT(lspf)
CMAKE_MINIMUM_REQUIRED(VERSION 3.5)

add_compile_options(-std=c++0x)

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH}" "${CMAKE_LOCAL_PATH}/cmake")

include(DefineOptions)

include_directories(../src)
include_directories(/usr/lib/oracle/12.2/client64/lib)

add_executable(example example.cpp)
target_link_libraries(example hiredis)
target_link_libraries(example redisclient)
target_link_libraries(example boost_system)
target_link_libraries(example dbpool)
target_link_libraries(example manage)
target_link_libraries(example log)
target_link_libraries(example common)
target_link_libraries(example pthread)
target_link_libraries(example thrift)
target_link_libraries(example thriftnb)
#target_link_libraries(example zookeeper_mt)
target_link_libraries(example event)
#target_link_libraries(example occi)
#target_link_libraries(example clntsh)
target_link_libraries(example json_mt)
#target_link_libraries(example activemq-cpp)
target_link_libraries(example boost_system)
target_link_libraries(example boost_thread)


