#include <boost/asio/io_service.hpp>

#include <cstdlib>
#include <iostream>
#include <memory>

#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "server/server.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

int main(int argc, char* argv[]) {
  auto table_a = opossum::load_table("src/test/tables/int_float.tbl", 2);
  auto customer = opossum::load_table("src/test/tables/tpch/sf-0.001/customer.tbl", 2);
  opossum::StorageManager::get().add_table("table_a", table_a);
  opossum::StorageManager::get().add_table("customer", customer);

  // Create audit log table.
  opossum::TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("user", opossum::DataType::String, false);
  column_definitions.emplace_back("commit_id", opossum::DataType::Int, false);
  column_definitions.emplace_back("epoch_ms", opossum::DataType::Long, false);
  column_definitions.emplace_back("query", opossum::DataType::String, false);
  column_definitions.emplace_back("row_count", opossum::DataType::Long, false);
  column_definitions.emplace_back("execution_time_micros", opossum::DataType::Long, false);
  column_definitions.emplace_back("query_allowed", opossum::DataType::Int, false);
  column_definitions.emplace_back("id", opossum::DataType::Int, false);
  column_definitions.emplace_back("snapshot_id", opossum::DataType::Int, false);
  std::shared_ptr<opossum::Table> audit_log_table =
      std::make_shared<opossum::Table>(column_definitions, opossum::TableType::Data);
  opossum::StorageManager::get().add_table("audit_log", audit_log_table);

  // Load user rate limiting table.
  opossum::StorageManager::get().add_table("user_rate_limiting", opossum::load_table("user_rate_limiting.tbl"));

  try {
    uint16_t port = 5432;

    if (argc >= 2) {
      port = static_cast<uint16_t>(std::atoi(argv[1]));
    }

    // Set scheduler so that the server can execute the tasks on separate threads.
    opossum::CurrentScheduler::set(
        std::make_shared<opossum::NodeQueueScheduler>(opossum::Topology::create_numa_topology()));

    boost::asio::io_service io_service;

    // The server registers itself to the boost io_service. The io_service is the main IO control unit here and it lives
    // until the server doesn't request any IO any more, i.e. is has terminated. The server requests IO in its
    // constructor and then runs forever.
    opossum::Server server{io_service, port};

    io_service.run();
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
