#CMAKE_C_COMPILER��ָ��C������
#CMAKE_CXX_COMPILER��
#CMAKE_C_FLAGS������C�ļ�ʱ��ѡ���-g��Ҳ����ͨ��add_definitions��ӱ���ѡ��

#set(CMAKE_C_COMPILER "gcc")
#set(CMAKE_CXX_COMPILER "g++")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "release")
endif()

if( CMAKE_BUILD_TYPE STREQUAL "debug" )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O1 -Wall -DDEBUG")
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

message("Build Type:" ${CMAKE_BUILD_TYPE} ${CMAKE_CXX_FLAGS})
message("Build PATH:" ${CMAKE_LOCAL_PATH})

# include directories
INCLUDE_DIRECTORIES(
  /usr/local/include
  ${CMAKE_LOCAL_PATH}/../local/include
  ${CMAKE_LOCAL_PATH}/../contrib/include
  ${CMAKE_LOCAL_PATH}/../contrib/include/occi
  ${CMAKE_LOCAL_PATH}/../contrib/include/activemq-cpp-3.9.4
  ${CMAKE_LOCAL_PATH}/../contrib/include/apr-1
)

# lib directories
LINK_DIRECTORIES(
  /usr/local/lib
  /usr/lib/oracle/12.1/client64/lib
  ${CMAKE_LOCAL_PATH}/../local/lib
  ${CMAKE_LOCAL_PATH}/../contrib/lib
)

SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_LOCAL_PATH}/${CMAKE_BUILD_TYPE}/bin)
SET(LIBRARY_OUTPUT_PATH ${CMAKE_LOCAL_PATH}/${CMAKE_BUILD_TYPE}/lib)
SET(INCLUDE_OUTPUT_PATH ${CMAKE_LOCAL_PATH}/include)