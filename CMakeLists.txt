cmake_minimum_required(VERSION 3.14)
project(DataFrameLib)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

# ── Library sources ────────────────────────────────────────────────────────
set(LIB_SOURCES
    src/Expression.cpp
)

add_library(dataframelib ${LIB_SOURCES})
target_include_directories(dataframelib PUBLIC include)
target_link_libraries(dataframelib PUBLIC Arrow::arrow_shared Parquet::parquet_shared)

# ── Test executable ────────────────────────────────────────────────────────
add_executable(dataframe_test main.cpp)
target_link_libraries(dataframe_test PRIVATE dataframelib)
