cmake_minimum_required(VERSION 3.22)

project(ibis)

set(CMAKE_MODULE_PATH              "${CMAKE_SOURCE_DIR}/cmake")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(cmake/BuildFlags.cmake)
include(cmake/FindBenchmark.cmake)
include(cmake/FindGTest.cmake)
include(cmake/Task.cmake)

add_subdirectory(lsmtree)
