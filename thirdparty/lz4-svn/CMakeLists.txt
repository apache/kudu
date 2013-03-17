cmake_minimum_required(VERSION 2.8)

SET(CMAKE_BUILD_TYPE "Release")
set(CMAKE_C_FLAGS_RELEASE "-O3 -std=c99 -Wall -W -Wundef -Wno-implicit-function-declaration")

set(LZ4_SOURCES lz4.c lz4hc.c)
set(LZ4_HEADERS lz4.h lz4hc.h)

add_library(lz4 STATIC ${LZ4_SOURCES})
target_link_libraries(lz4)

install(FILES ${LZ4_HEADERS} DESTINATION include)
install(TARGETS lz4 DESTINATION lib)