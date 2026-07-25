cmake_minimum_required(VERSION 3.10)
project(branch C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

if(CMAKE_BUILD_TYPE STREQUAL "MinSizeRel" OR CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-Os -fdata-sections -ffunction-sections -flto)
    if(NOT APPLE)
        add_link_options(-Wl,--gc-sections -s -flto)
    else()
        add_link_options(-Wl,-dead_strip -flto)
    endif()
endif()

add_executable(branch_broker main.c)
target_include_directories(branch_broker PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(test_branch test_branch.c)
target_include_directories(test_branch PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
enable_testing()
add_test(NAME branch_unit COMMAND test_branch)
