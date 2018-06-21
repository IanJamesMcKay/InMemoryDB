#include <boost/asio/io_service.hpp>

#include <cstdlib>
#include <iostream>

#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "server/server.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"
#include "operators/validate.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/jit_operator_wrapper.hpp"
#include "concurrency/transaction_manager.hpp"
#include "operators/jit_operator/operators/jit_read_tuples.hpp"
#include "operators/jit_operator/operators/jit_write_tuples.hpp"
#include "operators/jit_operator/operators/jit_expression.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "types.hpp"


int main(int argc, char* argv[]) {
  try {
    uint16_t port = 5432;

    if (argc >= 2) {
      port = static_cast<uint16_t>(std::atoi(argv[1]));
    }

    auto table_a = opossum::load_table("src/test/tables/int_float.tbl", 2);
    opossum::StorageManager::get().add_table("table_a", table_a);

    auto table_b = opossum::load_table("src/test/tables/int_int.tbl", 1000);
    opossum::StorageManager::get().add_table("tmp", table_b);

    auto context = opossum::TransactionManager::get().new_transaction_context();
    auto get_table = std::make_shared<opossum::GetTable>("tmp");
    get_table->set_transaction_context(context);
    auto validate = std::make_shared<opossum::Validate>(get_table);
    validate->set_transaction_context(context);
    auto jit_operator = std::make_shared<opossum::JitOperatorWrapper>(get_table, opossum::JitExecutionMode::Compile); // Interpret validate
    auto read_tuple = std::make_shared<opossum::JitReadTuples>();
    opossum::JitTupleValue tuple_val = read_tuple->add_input_column(opossum::DataType::Int, false, opossum::ColumnID(0));
    jit_operator->add_jit_operator(read_tuple);

    auto id = read_tuple->add_temporary_value();

    auto expression = std::make_shared<opossum::JitExpression>(std::make_shared<opossum::JitExpression>(tuple_val),
                                                               opossum::ExpressionType::Addition, std::make_shared<opossum::JitExpression>(tuple_val), id);

    auto compute = std::make_shared<opossum::JitCompute>(expression);

    jit_operator->add_jit_operator(compute);

    auto write_table = std::make_shared<opossum::JitWriteTuples>();
    auto tuple_value = expression->result();
    write_table->add_output_column("a+1", tuple_value);
    jit_operator->add_jit_operator(write_table);

    jit_operator->set_transaction_context(context);
    auto print = std::make_shared<opossum::Print>(jit_operator);
    print->set_transaction_context(context);
    get_table->execute();
    // validate->execute();
    jit_operator->execute();
    print->execute();


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
