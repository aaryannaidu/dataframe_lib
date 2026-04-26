cmake_minimum_required(VERSION 3.14)
project(DataFrameLib)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Arrow REQUIRED)
find_package(ArrowCompute REQUIRED)
find_package(Parquet REQUIRED)

# ── Library sources ────────────────────────────────────────────────────────
set(LIB_SOURCES
    src/Expression.cpp
    src/IO.cpp
    src/EagerDataFrame.cpp
    src/DAGNode.cpp
    src/QueryOptimizer.cpp
    src/LazyDataFrame.cpp
)

add_library(dataframelib ${LIB_SOURCES})
target_include_directories(dataframelib PUBLIC include)
target_link_libraries(dataframelib PUBLIC Arrow::arrow_shared ArrowCompute::arrow_compute_shared Parquet::parquet_shared)

# ── Test executables ───────────────────────────────────────────────────────


add_executable(dfl_test_expressions tests/test_expressions.cpp)
target_link_libraries(dfl_test_expressions PRIVATE dataframelib)

add_executable(dfl_test_eager tests/test_eager.cpp)
target_link_libraries(dfl_test_eager PRIVATE dataframelib)

add_executable(dfl_test_lazy tests/test_lazy.cpp)
target_link_libraries(dfl_test_lazy PRIVATE dataframelib)

add_executable(dfl_test_optimizer tests/test_optimizer.cpp)
target_link_libraries(dfl_test_optimizer PRIVATE dataframelib)
