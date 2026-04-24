#pragma once

#include "DAGNode.hpp"

#include <memory>

namespace dataframelib {

// Returns an optimized DAG rooted at `root`.
// Currently a pass-through; full optimization rules are added in step 8.
std::shared_ptr<DAGNode> optimize(const std::shared_ptr<DAGNode>& root);

}
