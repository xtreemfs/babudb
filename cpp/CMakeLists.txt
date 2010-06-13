cmake_minimum_required (VERSION 2.8)
project (babudb)

find_path(SRC_DIR CMakeLists.txt PATHS .)
message(STATUS "babudb root directory: " ${SRC_DIR})
# Important for installing yield_platform to lib/
set(CMAKE_INSTALL_PREFIX ${SRC_DIR})

include_directories(include/ src/ third_party/yield_platform/include)

# Get all files
file(GLOB ALL_FILES src/*.cpp src/log/*.cpp src/index/*.cpp)

# Separate code and tests
set(SRC_FILES "")
set(TEST_FILES "")
foreach(FILE ${ALL_FILES})
  if(FILE MATCHES ".*_test.cpp")
    list(APPEND TEST_FILES ${FILE})
  else()
    list(APPEND SRC_FILES ${FILE})
  endif()
endforeach(FILE)

add_subdirectory("third_party/yield_platform")
add_library(babudb ${SRC_FILES})

list(APPEND TEST_FILES "src/test_main.cpp")
add_executable(babudb_tests ${TEST_FILES})
target_link_libraries(babudb_tests babudb yield_platform)

set_target_properties(babudb PROPERTIES COMPILE_FLAGS "/MTd")
set_target_properties(babudb_tests PROPERTIES COMPILE_FLAGS "/MTd")
set_target_properties(yield_platform PROPERTIES COMPILE_FLAGS "/MTd")

install(TARGETS babudb babudb_tests
        RUNTIME DESTINATION "${SRC_DIR}/bin"
        LIBRARY DESTINATION "${SRC_DIR}/lib"
        ARCHIVE DESTINATION "${SRC_DIR}/lib")