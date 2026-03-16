cmake_minimum_required(VERSION 2.8.12...3.29)
project(benchmark)

set(CMAKE_CXX_STANDARD 11)

include_directories(/opt/intel/oneapi/vtune/latest/include)
link_libraries(/opt/intel/oneapi/vtune/latest/lib64/libittnotify.a)

add_compile_options(-g3)


add_executable(nosteal main.cpp)

target_include_directories(nosteal PUBLIC ../../no-steal/src/include)
target_link_directories(nosteal PUBLIC ../../no-steal/build/release/src)

target_link_libraries(nosteal duckdb)


add_executable(baseline main.cpp)

target_include_directories(baseline PUBLIC ../../baseline/src/include)
target_link_directories(baseline PUBLIC ../../baseline/build/release/src)

target_link_libraries(baseline duckdb)


add_executable(nonuma main.cpp)

target_include_directories(nonuma PUBLIC ../../no-numa/src/include)
target_link_directories(nonuma PUBLIC ../../no-numa/build/release/src)

target_link_libraries(nonuma duckdb)

add_executable(steal main.cpp)

target_include_directories(steal PUBLIC ../../steal/src/include)
target_link_directories(steal PUBLIC ../../steal/build/release/src)

target_link_libraries(steal duckdb)



add_executable(prof prof.cpp)

target_include_directories(prof PUBLIC ../../baseline/src/include)
target_link_directories(prof PUBLIC ../../baseline/build/release/src)

target_link_libraries(prof duckdb)


