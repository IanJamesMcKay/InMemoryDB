# Configure server
add_executable(
    hyriseServer

    server.cpp
)
target_link_libraries(
    hyriseServer
    hyrise
)

# Configure playground
add_executable(
    hyrisePlayground
    playground.cpp
)
target_link_libraries(
    hyrisePlayground
     ${FILESYSTEM_LIBRARY}
    hyrise
    hyriseBenchmarkLib
)
# Configure playground2
add_executable(
    hyrisePlayground2
    playground2.cpp
)
target_link_libraries(
    hyrisePlayground2
     ${FILESYSTEM_LIBRARY}
    hyrise
    hyriseBenchmarkLib
)

# Configure hyriseJoinOrderingEvaluator
add_executable(
    hyriseJoinOrderingEvaluator

    joe_main.cpp
    joe/abstract_join_ordering_workload.hpp
    joe/out.cpp
    joe/out.hpp
    joe/joe_config.cpp
    joe/joe_config.hpp
    joe/tpch_join_ordering_workload.cpp
    joe/tpch_join_ordering_workload.hpp
    joe/job_join_ordering_workload.cpp
    joe/job_join_ordering_workload.hpp
    joe/joe.cpp
    joe/joe.hpp
    joe/joe_query.cpp
    joe/joe_query.hpp
    joe/joe_query_iteration.cpp
    joe/joe_query_iteration.hpp
    joe/joe_plan.cpp
    joe/joe_plan.hpp
    joe/write_csv.hpp
)
target_link_libraries(
    hyriseJoinOrderingEvaluator
    ${FILESYSTEM_LIBRARY}
    hyrise
    hyriseBenchmarkLib
)


# Configure hyriseCostModelCalibrator
add_executable(hyriseCostModelCalibrator cost_model_calibrator.cpp)
target_link_libraries(hyriseCostModelCalibrator hyrise hyriseBenchmarkLib)

# Configure tpccTableGenerator
add_executable(tpccTableGenerator tpcc_table_generator.cpp)
target_link_libraries(tpccTableGenerator hyrise hyriseBenchmarkLib)

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
)
target_include_directories(
    hyriseConsole

    PUBLIC
    ${READLINE_INCLUDE_DIR}
)
