#include <boost/asio/io_service.hpp>

#include <cstdlib>
#include <iostream>

#include "global.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "server/server.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

int main(int argc, char* argv[]) {
  try {
    uint16_t port = 5432;

    if (argc >= 2) {
      port = static_cast<uint16_t>(std::atoi(argv[1]));
    }

    auto& global = opossum::Global::get();
    global.jit = true;
    global.lazy_load = false;
    global.jit_validate = true;

    auto table_a = opossum::load_table("src/test/tables/int_float.tbl", 2);
    opossum::StorageManager::get().add_table("table_a", table_a);

    auto table_b = opossum::load_table("src/test/tables/int.tbl", 1000);
    opossum::StorageManager::get().add_table("tmp", table_b);

    auto table_c = opossum::load_table("src/test/tables/int3.tbl", 1000);
    opossum::StorageManager::get().add_table("tmp2", table_c);

    auto lineitem = opossum::load_table("src/test/tables/tpch/sf-0.001/lineitem.tbl", 1000);
    opossum::StorageManager::get().add_table("lineitem", lineitem);

    // Set scheduler so that the server can execute the tasks on separate threads.
    opossum::CurrentScheduler::set(std::make_shared<opossum::NodeQueueScheduler>());

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
