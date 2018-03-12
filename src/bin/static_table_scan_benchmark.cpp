#include <numa.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_all.hpp"

#include "scheduler/abstract_scheduler.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "scheduler/topology.hpp"

#include "storage/chunk.hpp"
#include "storage/numa_placement_manager.hpp"
#include "storage/storage_manager.hpp"
#include "storage/value_column.hpp"

#include "utils/load_table.hpp"

#include "benchmark/table_generator.hpp"

// #include "println.hpp"
#include "types.hpp"


namespace opossum {

class PrintLn final {
 public:
  explicit PrintLn(std::mutex& mutex) : m_mutex(mutex) { m_mutex.lock(); };
  ~PrintLn() {
    std::cout << std::endl;
    std::cout.flush();
    m_mutex.unlock();
  };

  template <typename T>
  PrintLn& operator<<(const T& value) {
    std::cout << value;
    return *this;
  }

 private:
  std::mutex& m_mutex;
};

PrintLn println() {
  static std::mutex mutex;
  return PrintLn(mutex);
}

}  // namespace opossum


void collect_counters() {
  const auto &tables = opossum::StorageManager::get().get_tables();
  for (const auto &entry : tables) {
    const auto &_table = entry.second;
    for (auto i = opossum::ChunkID{0}; i < _table->chunk_count(); i++) {
      const auto &_chunk = _table->get_chunk(i);
      if (const auto access_counter = _chunk->access_counter()) {
        access_counter->process();
        // opossum::println() << "Table: " << entry.first << " Chunk: " << i << " "
        //                    << access_counter->counter();
      }
    }
  }
}

int main(int argc, char *argv[]) {
  uint32_t num_cores = 28;
  const auto num_iterations = 250;
  const auto num_rows_per_chunk = 25 * 100 * 1000;
  const auto num_chunks = 1000;
  const auto num_cols = 2;
  const auto max_value = 100;

  numa_run_on_node(0);

  std::string numa_mode = "roundrobin";
  if (argc > 1) {
    numa_mode = argv[1];
  }
  if (argc > 2) {
    num_cores = atoi(argv[2]);
  }
  // auto actual_numa_topology = opossum::Topology::create_numa_topology();
  // auto topology = opossum::Topology::create_numa_topology(num_cores);
  if (numa_mode != "first-touch") {
    auto node_bitmask = numa_allocate_nodemask();
    for (size_t i = 0; i < opossum::NUMAPlacementManager::get(num_cores).topology()->nodes().size(); i++) {
      numa_bitmask_setbit(node_bitmask, i);
    }
    numa_set_interleave_mask(node_bitmask);
    numa_free_nodemask(node_bitmask);
  }

  opossum::println() << "PID: " << ::getpid();

  size_t table_size = sizeof(int) * num_cols * num_chunks * num_rows_per_chunk;
  opossum::println() << "Table size: " << table_size << " - " << num_iterations << " iterations";

  // for (size_t current_num_cores = num_cores; current_num_cores <= num_cores; current_num_cores += 30) {
  // opossum::NUMAPlacementManager::set(std::make_shared<opossum::NUMAPlacementManager>(topology));

  auto table = opossum::generate_table(
      num_cols, num_rows_per_chunk, num_chunks, max_value,
      (numa_mode == "interleave" || numa_mode == "first-touch")
          ? std::make_shared<opossum::AllocatorGenerator>()
          : std::make_shared<opossum::NUMAAllocatorGenerator>(opossum::NUMAPlacementManager::get()));
  opossum::println() << "Generated table";

  opossum::StorageManager::get().add_table("table", table);
  opossum::CurrentScheduler::set(std::make_shared<opossum::NodeQueueScheduler>(opossum::NUMAPlacementManager::get().topology()));
  opossum::NUMAPlacementManager::get().resume_collector();

  auto begin = std::chrono::high_resolution_clock::now();
  {
    for (size_t iteration = 0; iteration < num_iterations; iteration++) {
      auto get_table_task = std::make_shared<opossum::OperatorTask>(std::make_shared<opossum::GetTable>("table"));

      auto table_scan_task_0 = std::make_shared<opossum::OperatorTask>(
          std::make_shared<opossum::TableScan>(get_table_task->get_operator(), "a", "=", 1));
      auto table_scan_task_1 = std::make_shared<opossum::OperatorTask>(
          std::make_shared<opossum::TableScan>(table_scan_task_0->get_operator(), "b", "=", 10));

      get_table_task->set_as_predecessor_of(table_scan_task_0);
      table_scan_task_0->set_as_predecessor_of(table_scan_task_1);

      get_table_task->schedule();
      table_scan_task_0->schedule();
      table_scan_task_1->schedule();
    }
    opossum::CurrentScheduler::get()->finish();
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
  opossum::NUMAPlacementManager::get()->pause_collector();
  // opossum::NUMAPlacementManager::get()->print_collection_log(std::cout);

  opossum::StorageManager::get().reset();

  std::cout << current_num_cores << "; "
            << (0.55f * static_cast<float>(num_iterations * table_size / (1 << 30))) /
                   (static_cast<float>(duration) / 1000000)
            << std::endl;
  // }

  return 0;
}