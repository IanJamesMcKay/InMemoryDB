# Configure server
add_executable(
    hyriseServer

    server.cpp
)
target_link_libraries(
    hyriseServer
    hyrise
    ${Boost_SYSTEM_LIBRARY}
    ${Boost_THREAD_LIBRARY}
    boost_program_options
)
target_include_directories(
    hyriseServer

    PUBLIC
    ${Boost_INCLUDE_DIRS}
)

# Configure playground
add_executable(
    hyrisePlayground

    playground.cpp
)
target_link_libraries(
    hyrisePlayground
    hyrise
    hyriseBenchmarkLib
)

# Configure playground
add_executable(
    numaJOB

    numa_job_main.cpp
    numa_job/numa_job.cpp
    numa_job/numa_job.hpp
    numa_job/numa_job_config.cpp
    numa_job/numa_job_config.hpp
    numa_job/numa_job_query.cpp
    numa_job/numa_job_query.hpp
    numa_job/out.cpp
    numa_job/out.hpp
    numa_job/abstract_join_ordering_workload.hpp
    numa_job/job_join_ordering_workload.cpp
    numa_job/job_join_ordering_workload.hpp
    numa_job/tpch_join_ordering_workload.cpp
    numa_job/tpch_join_ordering_workload.hpp
    numa_job/write_csv.hpp
)
target_link_libraries(
    numaJOB
    ${FILESYSTEM_LIBRARY}
    hyrise
    hyriseBenchmarkLib
)

# Configure tpchTableGenerator
add_executable(tpchTableGenerator tpch_table_generator.cpp)
target_link_libraries(tpchTableGenerator hyrise hyriseBenchmarkLib)

# Configure client
add_executable(
    hyriseClient
    client.cpp
    client.hpp
)
target_link_libraries(
    hyriseClient
    hyrise
)

# Configure Console
add_executable(
    hyriseConsole

    console/console.cpp
    console/console.hpp
    console/pagination.cpp
    console/pagination.hpp
)
target_link_libraries(
    hyriseConsole
    hyrise
    hyriseBenchmarkLib
    ncurses
    ${READLINE_LIBRARY}
    ${FILESYSTEM_LIBRARY}
)
target_include_directories(
    hyriseConsole

    PUBLIC
    ${READLINE_INCLUDE_DIR}
)
