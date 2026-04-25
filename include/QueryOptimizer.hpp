#pragma once

#include "DAGNode.hpp"

#include <memory>

namespace dataframelib {

// Returns an optimized DAG rooted at `root`.
// Applies projection pushdown (top-down), then runs predicate pushdown,
// limit pushdown, constant folding, and expression simplification to a fixed point.
std::shared_ptr<DAGNode> optimize(const std::shared_ptr<DAGNode>& root);

}
