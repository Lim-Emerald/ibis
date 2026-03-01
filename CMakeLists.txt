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

# Set depending on the boost.

# Compose boost libraries as static.
set(Boost_USE_STATIC_LIBS ON)

# Use the multithreaded version.
set(Boost_USE_MULTITHREADED ON)

# Does not replace the standard library.
set(Boost_USE_STATIC_RUNTIME OFF)

message(STATUS "Searching for Boost...")
find_package(Boost 1.53 COMPONENTS
  date_time
  filesystem
  program_options
  system
  unit_test_framework
  iostreams
  regex
  serialization
  locale
)

message(STATUS "Boost components summary:")
message(STATUS "  date time: ${Boost_DATE_TIME_FOUND}")
message(STATUS "  filesystem: ${Boost_FILESYSTEM_FOUND}")
message(STATUS "  program options: ${Boost_PROGRAM_OPTIONS_FOUND}")
message(STATUS "  system: ${Boost_SYSTEM_FOUND}")
message(STATUS "  unit test framework: ${Boost_UNIT_TEST_FRAMEWORK_FOUND}")
message(STATUS "  iostreams: ${Boost_IOSTREAMS_FOUND}")
message(STATUS "  serialization: ${Boost_SERIALIZATION_FOUND}")
message(STATUS "  regex: ${Boost_REGEX_FOUND}")
message(STATUS "  locale: ${Boost_LOCALE_FOUND}")

if (Boost_FOUND)
  message(STATUS "Boost library dirs: " ${Boost_LIBRARY_DIRS})
  message(STATUS "Boost include dirs: " ${Boost_INCLUDE_DIRS})
  include_directories(${Boost_INCLUDE_DIRS})
else ()
  message(FATAL_ERROR "Some boost libraries not found.")
endif ()

add_subdirectory(inverted_index)
add_subdirectory(lsmtree)
add_subdirectory(strutext)
