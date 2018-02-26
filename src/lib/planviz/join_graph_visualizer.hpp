#pragma once

#include <memory>

#include "optimizer/join_ordering/join_graph.hpp"
#include "planviz/abstract_visualizer.hpp"

namespace opossum {

class JoinGraphVisualizer : public AbstractVisualizer<std::shared_ptr<const JoinGraph>> {
 public:
  using AbstractVisualizer<std::shared_ptr<const JoinGraph>>::AbstractVisualizer;

 protected:
  void _build_graph(const std::shared_ptr<const JoinGraph>& graph) override;
};

}  // namespace opossum
