#include "DAGNode.hpp"

namespace dataframelib {

int DAGNode::next_id_ = 0;

DAGNode::DAGNode(std::shared_ptr<DAGNode> input)
    : id_(next_id_++), input_(std::move(input)) {}

}
