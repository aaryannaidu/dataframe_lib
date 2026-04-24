#pragma once

#include "Expression.hpp"

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace dataframelib {

enum class NodeType {
    SCAN, FILTER, SELECT, WITH_COLUMN, GROUP_BY, AGGREGATE, JOIN, SORT, HEAD
};

enum class FileType { CSV, PARQUET };

// Base class for all DAG nodes. Each node gets a unique auto-incremented ID.
struct DAGNode {
    virtual ~DAGNode() = default;
    virtual NodeType type() const = 0;

    int id() const { return id_; }
    const std::shared_ptr<DAGNode>& input() const { return input_; }

protected:
    explicit DAGNode(std::shared_ptr<DAGNode> input = nullptr);

private:
    int id_;
    std::shared_ptr<DAGNode> input_;
    static int next_id_;
};

// Reads a file (CSV or Parquet) — root of every DAG.
struct ScanNode : DAGNode {
    std::string path;
    FileType    file_type;

    ScanNode(std::string p, FileType ft)
        : DAGNode(nullptr), path(std::move(p)), file_type(ft) {}
    NodeType type() const override { return NodeType::SCAN; }
};

// Keeps only rows where predicate is true.
struct FilterNode : DAGNode {
    Expr predicate;

    FilterNode(std::shared_ptr<DAGNode> in, Expr pred)
        : DAGNode(std::move(in)), predicate(std::move(pred)) {}
    NodeType type() const override { return NodeType::FILTER; }
};

// Projects a set of expressions; col("x") for name-only selects.
struct SelectNode : DAGNode {
    std::vector<Expr> exprs;

    SelectNode(std::shared_ptr<DAGNode> in, std::vector<Expr> exprs)
        : DAGNode(std::move(in)), exprs(std::move(exprs)) {}
    NodeType type() const override { return NodeType::SELECT; }
};

// Appends or replaces a column computed from an expression.
struct WithColumnNode : DAGNode {
    std::string name;
    Expr        expr;

    WithColumnNode(std::shared_ptr<DAGNode> in, std::string n, Expr e)
        : DAGNode(std::move(in)), name(std::move(n)), expr(std::move(e)) {}
    NodeType type() const override { return NodeType::WITH_COLUMN; }
};

// Groups rows by key columns. Must be followed by an AggregateNode.
struct GroupByNode : DAGNode {
    std::vector<std::string> keys;

    GroupByNode(std::shared_ptr<DAGNode> in, std::vector<std::string> keys)
        : DAGNode(std::move(in)), keys(std::move(keys)) {}
    NodeType type() const override { return NodeType::GROUP_BY; }
};

// Computes per-group aggregations; input must be a GroupByNode.
struct AggregateNode : DAGNode {
    std::map<std::string, Expr> agg_map;

    AggregateNode(std::shared_ptr<DAGNode> in, std::map<std::string, Expr> agg)
        : DAGNode(std::move(in)), agg_map(std::move(agg)) {}
    NodeType type() const override { return NodeType::AGGREGATE; }
};

// Hash-joins two DAGs. `input()` is the left side, `right` is the right side.
struct JoinNode : DAGNode {
    std::shared_ptr<DAGNode>  right;
    std::vector<std::string>  on;
    std::string               how;  // "inner" | "left" | "right" | "outer"

    JoinNode(std::shared_ptr<DAGNode> left, std::shared_ptr<DAGNode> right,
             std::vector<std::string> on, std::string how)
        : DAGNode(std::move(left)), right(std::move(right)),
          on(std::move(on)), how(std::move(how)) {}
    NodeType type() const override { return NodeType::JOIN; }
};

// Sorts rows by the given columns.
struct SortNode : DAGNode {
    std::vector<std::string> columns;
    std::vector<bool>        ascending;

    SortNode(std::shared_ptr<DAGNode> in,
             std::vector<std::string> cols, std::vector<bool> asc)
        : DAGNode(std::move(in)), columns(std::move(cols)), ascending(std::move(asc)) {}
    NodeType type() const override { return NodeType::SORT; }
};

// Keeps the first n rows.
struct HeadNode : DAGNode {
    int64_t n;

    HeadNode(std::shared_ptr<DAGNode> in, int64_t n)
        : DAGNode(std::move(in)), n(n) {}
    NodeType type() const override { return NodeType::HEAD; }
};

}
