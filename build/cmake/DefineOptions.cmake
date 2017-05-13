#CMAKE_C_COMPILER：指定C编译器
#CMAKE_CXX_COMPILER：
#CMAKE_C_FLAGS：编译C文件时的选项，如-g；也可以通过add_definitions添加编译选项

#set(CMAKE_C_COMPILER "gcc")
#set(CMAKE_CXX_COMPILER "g++")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "release")
endif()

if( CMAKE_BUILD_TYPE STREQUAL "debug" )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 -Wall -DDEBUG -ffunction-sections -fdata-sections")
	set(SUFFIXES_FLAG "_d")
elseif( CMAKE_BUILD_TYPE STREQUAL "release" )
    set(CMAKE_BUILD_TYPE "release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")
	set(SUFFIXES_FLAG "")
else()
    set(CMAKE_BUILD_TYPE "release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")
	set(SUFFIXES_FLAG "")
endif( CMAKE_BUILD_TYPE STREQUAL "debug" )

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -gc-sections")

message("Build Type:" ${CMAKE_BUILD_TYPE} ${CMAKE_CXX_FLAGS})
message("Build PATH:" ${CMAKE_LOCAL_PATH})

# include directories
INCLUDE_DIRECTORIES(
  /usr/local/include
  ${CMAKE_LOCAL_PATH}/../source
  ${CMAKE_LOCAL_PATH}/include
  ${CMAKE_LOCAL_PATH}/../contrib/include
  ${CMAKE_LOCAL_PATH}/../contrib/include/occi
  ${CMAKE_LOCAL_PATH}/../contrib/include/activemq-cpp-3.9.4
  ${CMAKE_LOCAL_PATH}/../contrib/include/apr-1
)

# lib directories
LINK_DIRECTORIES(
  /usr/local/lib
  /usr/lib/oracle/12.1/client64/lib
  ${CMAKE_LOCAL_PATH}/lib
#  ${PROJECT_SOURCE_DIR}/contrib/lib
  ${PROJECT_BINARY_DIR}/oldapi/lib

)

#SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_LOCAL_PATH}/bin)
#SET(LIBRARY_OUTPUT_PATH ${CMAKE_LOCAL_PATH}/lib)
#SET(INCLUDE_OUTPUT_PATH ${CMAKE_LOCAL_PATH}/include)