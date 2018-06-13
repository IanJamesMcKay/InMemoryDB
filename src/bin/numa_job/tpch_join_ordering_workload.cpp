#include "tpch_join_ordering_workload.hpp"

#include <cxxopts.hpp>
#include <experimental/filesystem>

#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"

#include "out.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

TpchJoinOrderingWorkload::TpchJoinOrderingWorkload(float scale_factor, const std::optional<std::vector<QueryID>>& query_ids):
_scale_factor(scale_factor)
{
  if (!query_ids) {
    for (auto it = std::begin(tpch_queries) ; it != std::end(tpch_queries) ; ++it) {
      _query_ids.push_back(it->first);
    }
  } else {
    _query_ids = *query_ids;
  }
}

void TpchJoinOrderingWorkload::setup() {
  out() << "-- Generating TPCH tables with scale factor " << _scale_factor << std::endl;
  TpchDbGenerator{_scale_factor}.generate_and_store();
  out() << "-- Done." << std::endl;
}

size_t TpchJoinOrderingWorkload::query_count() const {
  return _query_ids.size();
}

std::string TpchJoinOrderingWorkload::get_query(const size_t query_idx) const {
  return tpch_queries.at(_query_ids[query_idx]);
}

std::string TpchJoinOrderingWorkload::get_query_name(const size_t query_idx) const {
  return "TPCH"s + "-" + std::to_string(_query_ids[query_idx]);
}

}  // namespace opossum