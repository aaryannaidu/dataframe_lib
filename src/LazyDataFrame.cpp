#include "LazyDataFrame.hpp"
#include "QueryOptimizer.hpp"

#include <stdexcept>
#include <fstream>
#include <sstream>
#include <unordered_set>
#include <cstdlib>

namespace dataframelib {

namespace {

    std::string expr_to_string(const std::shared_ptr<ExprNode>& node) {
        if (!node) return "null";
        switch (node->type()) {
            case ExprType::COL:
                return static_cast<const ColNode*>(node.get())->name;
            case ExprType::LIT: {
                auto v = static_cast<const LitNode*>(node.get())->value;
                if (std::holds_alternative<int32_t>(v)) return std::to_string(std::get<int32_t>(v));
                if (std::holds_alternative<int64_t>(v)) return std::to_string(std::get<int64_t>(v));
                if (std::holds_alternative<float>(v)) return std::to_string(std::get<float>(v));
                if (std::holds_alternative<double>(v)) return std::to_string(std::get<double>(v));
                if (std::holds_alternative<bool>(v)) return std::get<bool>(v) ? "true" : "false";
                if (std::holds_alternative<std::string>(v)) return "'" + std::get<std::string>(v) + "'";
                return "lit";
            }
            case ExprType::ALIAS:
                return expr_to_string(static_cast<const AliasNode*>(node.get())->input) + " AS " + static_cast<const AliasNode*>(node.get())->name;
            case ExprType::BINOP: {
                static const char* ops[] = {
                    "+", "-", "*", "/", "%",
                    "==", "!=", "<", "<=", ">", ">=", "AND", "OR"
                };
                auto n = static_cast<const BinaryOpNode*>(node.get());
                std::string op = ops[static_cast<int>(n->op)];
                return "(" + expr_to_string(n->left) + " " + op + " " + expr_to_string(n->right) + ")";
            }
            case ExprType::UNARYOP: {
                auto n = static_cast<const UnaryOpNode*>(node.get());
                if (n->op == UnaryOp::IS_NULL) return expr_to_string(n->input) + " IS NULL";
                if (n->op == UnaryOp::IS_NOT_NULL) return expr_to_string(n->input) + " IS NOT NULL";
                if (n->op == UnaryOp::NOT) return "NOT " + expr_to_string(n->input);
                if (n->op == UnaryOp::ABS) return "ABS(" + expr_to_string(n->input) + ")";
                if (n->op == UnaryOp::NEGATE) return "-" + expr_to_string(n->input);
                return "unary";
            }
            case ExprType::AGG: {
                static const char* aggs[] = {"SUM", "MEAN", "COUNT", "MIN", "MAX"};
                auto n = static_cast<const AggNode*>(node.get());
                return std::string(aggs[static_cast<int>(n->agg)]) +
                       "(" + expr_to_string(n->input) + ")";
            }
            case ExprType::STRFUNC: {
                static const char* fns[] = {
                    "LENGTH", "CONTAINS", "STARTS_WITH", "ENDS_WITH", "TO_LOWER", "TO_UPPER"
                };
                auto n = static_cast<const StrFuncNode*>(node.get());
                std::string args = n->arg.empty() ? "" : ", '" + n->arg + "'";
                return std::string(fns[static_cast<int>(n->func)]) +
                       "(" + expr_to_string(n->input) + args + ")";
            }
            default: return "?";
        }
    }

    std::string expr_to_string(const Expr& expr) {
        return expr_to_string(expr.node());
    }

    std::string to_string(NodeType t) {
        static const char* names[] = {
            "SCAN", "FILTER", "SELECT", "WITH_COLUMN",
            "GROUP_BY", "AGGREGATE", "JOIN", "SORT", "HEAD"
        };
        int i = static_cast<int>(t);
        return (i >= 0 && i < 9) ? names[i] : "UNKNOWN";
    }

    std::string get_node_label(const std::shared_ptr<DAGNode>& node) {
        std::ostringstream oss;
        oss << to_string(node->type());
        switch (node->type()) {
            case NodeType::SCAN: {
                auto n = static_cast<const ScanNode*>(node.get());
                oss << "\\n" << n->path;
                break;
            }
            case NodeType::FILTER: {
                auto n = static_cast<const FilterNode*>(node.get());
                oss << "\\n" << expr_to_string(n->predicate);
                break;
            }
            case NodeType::SELECT: {
                auto n = static_cast<const SelectNode*>(node.get());
                oss << "\\n[";
                for (size_t i = 0; i < n->exprs.size(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << expr_to_string(n->exprs[i]);
                }
                oss << "]";
                break;
            }
            case NodeType::WITH_COLUMN: {
                auto n = static_cast<const WithColumnNode*>(node.get());
                oss << "\\n" << n->name << " = " << expr_to_string(n->expr);
                break;
            }
            case NodeType::GROUP_BY: {
                auto n = static_cast<const GroupByNode*>(node.get());
                oss << "\\nkeys: [";
                for (size_t i = 0; i < n->keys.size(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << n->keys[i];
                }
                oss << "]";
                break;
            }
            case NodeType::AGGREGATE: {
                auto n = static_cast<const AggregateNode*>(node.get());
                oss << "\\n{";
                bool first = true;
                for (const auto& kv : n->agg_map) {
                    if (!first) oss << ", ";
                    oss << kv.first << ":" << expr_to_string(kv.second);
                    first = false;
                }
                oss << "}";
                break;
            }
            case NodeType::JOIN: {
                auto n = static_cast<const JoinNode*>(node.get());
                oss << "\\n" << n->how << " join on [";
                for (size_t i = 0; i < n->on.size(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << n->on[i];
                }
                oss << "]";
                break;
            }
            case NodeType::SORT: {
                auto n = static_cast<const SortNode*>(node.get());
                oss << "\\n[";
                for (size_t i = 0; i < n->columns.size(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << n->columns[i] << (n->ascending[i] ? " ASC" : " DESC");
                }
                oss << "]";
                break;
            }
            case NodeType::HEAD: {
                auto n = static_cast<const HeadNode*>(node.get());
                oss << "\\nn=" << n->n;
                break;
            }
            default: break;
        }
        return oss.str();
    }

    void generate_dot_subgraph(const std::shared_ptr<DAGNode>& root,
                               std::ostream& os, 
                               const std::string& cluster_name,
                               const std::string& prefix) {
        os << "  subgraph " << cluster_name << " {\n";
        os << "    label=\"" << cluster_name << "\";\n";

        std::unordered_set<int> visited;
        std::vector<std::shared_ptr<DAGNode>> stack;
        if (root) stack.push_back(root);

        while (!stack.empty()) {
            auto node = stack.back();
            stack.pop_back();

            if (visited.count(node->id())) continue;
            visited.insert(node->id());

            std::string node_id = prefix + std::to_string(node->id());
            // Escape quotes inside labels
            std::string label = get_node_label(node);
            std::string escaped_label;
            for (char c : label) {
                if (c == '"') escaped_label += "\\\"";
                else escaped_label += c;
            }

            os << "    " << node_id << " [label=\"" << escaped_label << "\", shape=box];\n";

            if (node->input()) {
                std::string child_id = prefix + std::to_string(node->input()->id());
                os << "    " << child_id << " -> " << node_id << ";\n";
                stack.push_back(node->input());
            }

            if (node->type() == NodeType::JOIN) {
                auto join_node = static_cast<const JoinNode*>(node.get());
                if (join_node->right) {
                    std::string child_id = prefix + std::to_string(join_node->right->id());
                    os << "    " << child_id << " -> " << node_id << ";\n";
                    stack.push_back(join_node->right);
                }
            }
        }
        os << "  }\n";
    }

// Recursively execute a DAG node and return the materialised EagerDataFrame.
EagerDataFrame execute(const std::shared_ptr<DAGNode>& node) {
    switch (node->type()) {
        case NodeType::SCAN: {
            const auto& n = static_cast<const ScanNode&>(*node);
            return n.file_type == FileType::CSV
                ? read_csv(n.path)
                : read_parquet(n.path);
        }
        case NodeType::FILTER: {
            const auto& n = static_cast<const FilterNode&>(*node);
            return execute(n.input()).filter(n.predicate);
        }
        case NodeType::SELECT: {
            const auto& n = static_cast<const SelectNode&>(*node);
            return execute(n.input()).select(n.exprs);
        }
        case NodeType::WITH_COLUMN: {
            const auto& n = static_cast<const WithColumnNode&>(*node);
            return execute(n.input()).with_column(n.name, n.expr);
        }
        case NodeType::GROUP_BY:
            throw std::runtime_error("collect: GroupByNode must be followed by AggregateNode");
        case NodeType::AGGREGATE: {
            const auto& n = static_cast<const AggregateNode&>(*node);
            if (!n.input() || n.input()->type() != NodeType::GROUP_BY)
                throw std::runtime_error("collect: AggregateNode must follow GroupByNode");
            const auto& grp = static_cast<const GroupByNode&>(*n.input());
            return execute(grp.input()).group_by(grp.keys).aggregate(n.agg_map);
        }
        case NodeType::JOIN: {
            const auto& n = static_cast<const JoinNode&>(*node);
            return execute(n.input()).join(execute(n.right), n.on, n.how);
        }
        case NodeType::SORT: {
            const auto& n = static_cast<const SortNode&>(*node);
            return execute(n.input()).sort(n.columns, n.ascending);
        }
        case NodeType::HEAD: {
            const auto& n = static_cast<const HeadNode&>(*node);
            return execute(n.input()).head(n.n);
        }
    }
    throw std::runtime_error("collect: unknown node type");
}

}

LazyDataFrame::LazyDataFrame(std::shared_ptr<DAGNode> node)
    : node_(std::move(node)) {}

const std::shared_ptr<DAGNode>& LazyDataFrame::node() const { return node_; }

LazyDataFrame LazyDataFrame::filter(const Expr& predicate) const {
    return LazyDataFrame(std::make_shared<FilterNode>(node_, predicate));
}

LazyDataFrame LazyDataFrame::select(const std::vector<std::string>& columns) const {
    std::vector<Expr> exprs;
    exprs.reserve(columns.size());
    for (const auto& c : columns)
        exprs.push_back(col(c));
    return LazyDataFrame(std::make_shared<SelectNode>(node_, std::move(exprs)));
}

LazyDataFrame LazyDataFrame::select(const std::vector<Expr>& exprs) const {
    return LazyDataFrame(std::make_shared<SelectNode>(node_, exprs));
}

LazyDataFrame LazyDataFrame::with_column(const std::string& name, const Expr& expr) const {
    return LazyDataFrame(std::make_shared<WithColumnNode>(node_, name, expr));
}

LazyDataFrame LazyDataFrame::group_by(const std::vector<std::string>& keys) const {
    return LazyDataFrame(std::make_shared<GroupByNode>(node_, keys));
}

LazyDataFrame LazyDataFrame::aggregate(const std::map<std::string, Expr>& agg_map) const {
    return LazyDataFrame(std::make_shared<AggregateNode>(node_, agg_map));
}

LazyDataFrame LazyDataFrame::aggregate(
    const std::vector<std::pair<std::string, std::string>>& specs) const {
    static const std::pair<const char*, AggType> table[] = {
        {"sum", AggType::SUM}, {"mean", AggType::MEAN}, {"count", AggType::COUNT},
        {"min", AggType::MIN}, {"max", AggType::MAX},
    };
    std::map<std::string, Expr> m;
    for (const auto& s : specs) {
        AggType at = AggType::SUM;
        bool found = false;
        for (const auto& e : table) {
            if (s.second == e.first) { at = e.second; found = true; break; }
        }
        if (!found) throw std::runtime_error("aggregate: unknown function '" + s.second + "'");
        m.emplace(s.first + "_" + s.second, Expr(std::make_shared<AggNode>(
            std::make_shared<ColNode>(s.first), at)));
    }
    return aggregate(m);
}

LazyDataFrame LazyDataFrame::join(const LazyDataFrame& other,
                                   const std::vector<std::string>& on,
                                   const std::string& how) const {
    return LazyDataFrame(std::make_shared<JoinNode>(node_, other.node_, on, how));
}

LazyDataFrame LazyDataFrame::sort(const std::vector<std::string>& columns,
                                   const std::vector<bool>& ascending) const {
    return LazyDataFrame(std::make_shared<SortNode>(node_, columns, ascending));
}

LazyDataFrame LazyDataFrame::head(int64_t n) const {
    return LazyDataFrame(std::make_shared<HeadNode>(node_, n));
}

EagerDataFrame LazyDataFrame::collect() const {
    return execute(optimize(node_));
}

void LazyDataFrame::explain(const std::string& path) const {
    std::string dot_path = "plan.dot";
    std::ofstream out(dot_path);
    if (!out) throw std::runtime_error("explain: could not open plan.dot for writing");

    out << "digraph G {\n";
    out << "  rankdir=BT;\n";

    auto optimized = optimize(node_);

    generate_dot_subgraph(node_, out, "cluster_before", "b_");
    generate_dot_subgraph(optimized, out, "cluster_after", "a_");

    out << "}\n";
    out.close();

    std::string cmd = "dot -Tpng \"" + dot_path + "\" -o \"" + path + "\"";
    int ret = std::system(cmd.c_str());
    if (ret != 0) {
        throw std::runtime_error("explain: dot command failed. Is Graphviz installed?");
    }
}


void LazyDataFrame::sink_csv(const std::string& path) const     { collect().write_csv(path); }
void LazyDataFrame::sink_parquet(const std::string& path) const { collect().write_parquet(path); }

LazyDataFrame scan_csv(const std::string& path) {
    return LazyDataFrame(std::make_shared<ScanNode>(path, FileType::CSV));
}

LazyDataFrame scan_parquet(const std::string& path) {
    return LazyDataFrame(std::make_shared<ScanNode>(path, FileType::PARQUET));
}

}
