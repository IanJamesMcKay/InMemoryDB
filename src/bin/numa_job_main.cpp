#include <iostream>

#include <cxxopts.hpp>

#include "types.hpp"

#include "numa_job/numa_job_config.hpp"
#include "numa_job/numa_job.hpp"

using namespace opossum;  // NOLINT

int main(int argc, char ** argv) {
  auto cli_options_description = cxxopts::Options{"NUMA Join Ordering Benchmark", ""};

  const auto config = std::make_shared<NumaJobConfig>();
  config->add_options(cli_options_description);

  cli_options_description.parse_positional("queries");
  const auto cli_parse_result = cli_options_description.parse(argc, argv);

  // Display usage and quit
  if (cli_parse_result.count("help")) {
    std::cout << cli_options_description.help({}) << std::endl;
    return 0;
  }

  config->parse(cli_parse_result);
  config->setup();

  auto numa_job = NumaJob{config};
  numa_job.run();
}
