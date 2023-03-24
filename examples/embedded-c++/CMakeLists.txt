cmake_minimum_required(VERSION 2.8.12)
project(example-c++)

set(CMAKE_CXX_STANDARD 11)
include_directories(../../third_party/catch)
include_directories(../../third_party/tpce-tool/include)
include_directories(../../third_party/sqlite/include)
include_directories(../../src/include)
#link_directories(/home/liyu/duckdb/cmake-build-debug/src)

add_executable(example-c++ main.cpp)
target_link_libraries(example-c++ duckdb)
