cmake_minimum_required (VERSION 2.8)
project (yield_platform)

include_directories(include/)
file(GLOB ALL_FILES src/yield/platform/*.cpp)
add_library(yield_platform ${ALL_FILES})

# Do the install - destination ultimately determined by babudb's CMakeLists.txt
install(TARGETS yield_platform
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
