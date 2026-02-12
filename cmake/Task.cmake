option(TEST_SOLUTION "Build solution" OFF)

function(declare_task)
    set(HW_DIR ${CMAKE_CURRENT_SOURCE_DIR})

    file(GLOB_RECURSE SOURCES ${HW_DIR}/*.cpp)
    file(GLOB_RECURSE HEADERS ${HW_DIR}/*.h ${HW_DIR}/*.hpp)

    get_filename_component(HW_NAME ${HW_DIR} NAME)

    set(HW_BIS bis_${HW_NAME})
    set(HW_TESTS test_${HW_NAME})
    set(HW_BENCH bench_${HW_NAME})

    if (SOURCES)
        add_library(${HW_BIS} STATIC ${SOURCES} ${HEADERS})
        target_include_directories(${HW_BIS} PUBLIC ${HW_DIR})
    else()
        add_library(${HW_BIS} INTERFACE ${HEADERS})
        target_include_directories(${HW_BIS} INTERFACE ${HW_DIR})
    endif()

    file(GLOB_RECURSE TEST_SOURCES ${HW_DIR}/tests/*.cpp)
    file(GLOB_RECURSE BENCH_SOURCES ${HW_DIR}/bench/*.cpp)
    
    add_executable(${HW_TESTS} ${TEST_SOURCES} ${CMAKE_SOURCE_DIR}/contrib/gmock_main.cc)
    target_link_libraries(${HW_TESTS} gmock ${HW_BIS})
    target_include_directories(${HW_TESTS} PUBLIC ${HW_DIR})


    add_executable(${HW_BENCH} ${BENCH_SOURCES})
    target_link_libraries(${HW_BENCH} benchmark ${HW_BIS})
    target_include_directories(${HW_BENCH} PUBLIC ${HW_DIR})

    if (TEST_SOLUTION)
        add_custom_target(
                run_${HW_TESTS}
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                DEPENDS ${HW_TESTS}
                COMMAND ${CMAKE_BINARY_DIR}/${HW_TESTS})

        add_dependencies(test-all run_${HW_TESTS})
    endif()
endfunction()

add_custom_target(test-all)
