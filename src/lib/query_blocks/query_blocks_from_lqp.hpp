#pragma once

#include <memory>

namespace opossum {

class AbstractQueryBlock;
class AbstractLQPNode;

/**
 * Build QueryBlocks from an LQP.
 */
std::shared_ptr<AbstractQueryBlock> query_blocks_from_lqp(const std::shared_ptr<AbstractLQPNode>& lqp);

}  // namespace opossum
