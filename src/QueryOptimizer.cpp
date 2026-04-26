#include "QueryOptimizer.hpp"

#include <unordered_set>
#include <algorithm>
#include <iostream>

namespace dataframelib {

bool is_literal(const std::shared_ptr<ExprNode>& n, int32_t val) {
    if (!n || n->type() != ExprType::LIT) return false;
    auto l = static_cast<const LitNode*>(n.get());
    if (std::holds_alternative<int32_t>(l->value) && std::get<int32_t>(l->value) == val) return true;
    if (std::holds_alternative<int64_t>(l->value) && std::get<int64_t>(l->value) == val) return true;
    return false;
}

std::shared_ptr<ExprNode> optimize_expr(const std::shared_ptr<ExprNode>& node, bool& changed) {
    if (!node) return nullptr;

    switch (node->type()) {
        case ExprType::ALIAS: {
            auto n = static_cast<const AliasNode*>(node.get());
            auto opt_in = optimize_expr(n->input, changed);
            if (opt_in != n->input) { changed = true; return std::make_shared<AliasNode>(opt_in, n->name); }
            return node;
        }
        case ExprType::UNARYOP: {
            auto n = static_cast<const UnaryOpNode*>(node.get());
            auto opt_in = optimize_expr(n->input, changed);
            if (opt_in != n->input) { changed = true; return std::make_shared<UnaryOpNode>(opt_in, n->op); }
            return node;
        }
        case ExprType::AGG: {
            auto n = static_cast<const AggNode*>(node.get());
            auto opt_in = optimize_expr(n->input, changed);
            if (opt_in != n->input) { changed = true; return std::make_shared<AggNode>(opt_in, n->agg); }
            return node;
        }
        case ExprType::STRFUNC: {
            auto n = static_cast<const StrFuncNode*>(node.get());
            auto opt_in = optimize_expr(n->input, changed);
            if (opt_in != n->input) { changed = true; return std::make_shared<StrFuncNode>(opt_in, n->func, n->arg); }
            return node;
        }
        case ExprType::BINOP: {
            auto n = static_cast<const BinaryOpNode*>(node.get());
            auto opt_l = optimize_expr(n->left, changed);
            auto opt_r = optimize_expr(n->right, changed);

            if (n->op == BinOp::MUL) {
                if (is_literal(opt_r, 1)) { changed = true; return opt_l; }
                if (is_literal(opt_l, 1)) { changed = true; return opt_r; }
                if (is_literal(opt_r, 0) || is_literal(opt_l, 0)) {
                    changed = true; return std::make_shared<LitNode>(LitValue{int32_t{0}});
                }
            } else if (n->op == BinOp::ADD) {
                if (is_literal(opt_r, 0)) { changed = true; return opt_l; }
                if (is_literal(opt_l, 0)) { changed = true; return opt_r; }
            } else if (n->op == BinOp::SUB) {
                if (is_literal(opt_r, 0)) { changed = true; return opt_l; }
                if (opt_l && opt_l->type() == ExprType::COL && opt_r && opt_r->type() == ExprType::COL) {
                    auto cl = static_cast<const ColNode*>(opt_l.get());
                    auto cr = static_cast<const ColNode*>(opt_r.get());
                    if (cl->name == cr->name) { changed = true; return std::make_shared<LitNode>(LitValue{int32_t{0}}); }
                }
            }

            if (opt_l && opt_l->type() == ExprType::LIT && opt_r && opt_r->type() == ExprType::LIT) {
                auto ll = static_cast<const LitNode*>(opt_l.get());
                auto lr = static_cast<const LitNode*>(opt_r.get());
                if (std::holds_alternative<int32_t>(ll->value) && std::holds_alternative<int32_t>(lr->value)) {
                    int32_t v1 = std::get<int32_t>(ll->value);
                    int32_t v2 = std::get<int32_t>(lr->value);
                    int32_t res = 0; bool folded = false;
                    switch (n->op) {
                        case BinOp::ADD: res = v1 + v2; folded = true; break;
                        case BinOp::SUB: res = v1 - v2; folded = true; break;
                        case BinOp::MUL: res = v1 * v2; folded = true; break;
                        case BinOp::DIV: if (v2 != 0) { res = v1 / v2; folded = true; } break;
                        case BinOp::MOD: if (v2 != 0) { res = v1 % v2; folded = true; } break;
                        default: break;
                    }
                    if (folded) { changed = true; return std::make_shared<LitNode>(LitValue{res}); }
                }
            }

            if (opt_l != n->left || opt_r != n->right) {
                changed = true; return std::make_shared<BinaryOpNode>(opt_l, opt_r, n->op);
            }
            return node;
        }
        default: return node;
    }
}

Expr optimize_expr(const Expr& expr, bool& changed) {
    auto opt_node = optimize_expr(expr.node(), changed);
    return Expr(opt_node);
}

std::vector<Expr> optimize_exprs(const std::vector<Expr>& exprs, bool& changed, bool& local_changed) {
    std::vector<Expr> result;
    for (const auto& e : exprs) {
        auto opt = optimize_expr(e, changed);
        if (opt.node() != e.node()) local_changed = true;
        result.push_back(opt);
    }
    return result;
}

std::map<std::string, Expr> optimize_agg_map(const std::map<std::string, Expr>& m, bool& changed, bool& local_changed) {
    std::map<std::string, Expr> res;
    for (const auto& kv : m) {
        auto opt = optimize_expr(kv.second, changed);
        if (opt.node() != kv.second.node()) local_changed = true;
        res.insert({kv.first, opt});
    }
    return res;
}

// Returns the single child input of a one-input expression node, or nullptr.
static std::shared_ptr<ExprNode> expr_child(const ExprNode* n) {
    switch (n->type()) {
        case ExprType::ALIAS:   return static_cast<const AliasNode*>(n)->input;
        case ExprType::UNARYOP: return static_cast<const UnaryOpNode*>(n)->input;
        case ExprType::AGG:     return static_cast<const AggNode*>(n)->input;
        case ExprType::STRFUNC: return static_cast<const StrFuncNode*>(n)->input;
        default:                return nullptr;
    }
}

void get_referenced_cols(const std::shared_ptr<ExprNode>& node, std::unordered_set<std::string>& cols) {
    if (!node) return;
    if (node->type() == ExprType::COL) {
        cols.insert(static_cast<const ColNode*>(node.get())->name);
        return;
    }
    if (node->type() == ExprType::BINOP) {
        auto n = static_cast<const BinaryOpNode*>(node.get());
        get_referenced_cols(n->left, cols);
        get_referenced_cols(n->right, cols);
        return;
    }
    get_referenced_cols(expr_child(node.get()), cols);
}

std::unordered_set<std::string> get_referenced_cols(const Expr& expr) {
    std::unordered_set<std::string> cols;
    get_referenced_cols(expr.node(), cols);
    return cols;
}

std::shared_ptr<DAGNode> apply_rules(const std::shared_ptr<DAGNode>& node, bool& changed) {
    if (!node) return nullptr;

    std::shared_ptr<DAGNode> opt_input = nullptr;
    if (node->input()) opt_input = apply_rules(node->input(), changed);
    
    std::shared_ptr<DAGNode> opt_right = nullptr;
    if (node->type() == NodeType::JOIN) {
        auto j = static_cast<const JoinNode*>(node.get());
        if (j->right) opt_right = apply_rules(j->right, changed);
    }

    auto in = opt_input ? opt_input : node->input();
    
    switch (node->type()) {
        case NodeType::SCAN: return node;
        case NodeType::FILTER: {
            auto n = static_cast<const FilterNode*>(node.get());
            auto opt_pred = optimize_expr(n->predicate, changed);

            if (in && in->type() == NodeType::SELECT) {
                auto sel = static_cast<const SelectNode*>(in.get());
                auto req_cols = get_referenced_cols(opt_pred);
                bool safe_to_push = true;
                for (const auto& c : req_cols) {
                    bool found = false;
                    for (const auto& e : sel->exprs) {
                        if (e.type() == ExprType::COL && static_cast<const ColNode*>(e.node().get())->name == c) {
                            found = true; break;
                        }
                    }
                    if (!found) { safe_to_push = false; break; }
                }
                if (safe_to_push) {
                    changed = true;
                    auto new_filter = std::make_shared<FilterNode>(sel->input(), opt_pred);
                    return std::make_shared<SelectNode>(new_filter, sel->exprs);
                }
            }
            else if (in && in->type() == NodeType::SORT) {
                auto s = static_cast<const SortNode*>(in.get());
                changed = true;
                auto new_filter = std::make_shared<FilterNode>(s->input(), opt_pred);
                return std::make_shared<SortNode>(new_filter, s->columns, s->ascending);
            }

            if (opt_pred.node() != n->predicate.node() || in != n->input()) {
                if (opt_pred.node() != n->predicate.node()) changed = true;
                if (in != n->input()) changed = true;
                return std::make_shared<FilterNode>(in, opt_pred);
            }
            return node;
        }
        case NodeType::SELECT: {
            auto n = static_cast<const SelectNode*>(node.get());
            bool local_changed = false;
            auto opt_exprs = optimize_exprs(n->exprs, changed, local_changed);

            // Check if this SELECT is pure column references.
            bool outer_pure = std::all_of(opt_exprs.begin(), opt_exprs.end(),
                [](const Expr& e) { return e.type() == ExprType::COL; });

            if (outer_pure) {
                // Collect the column names this SELECT projects.
                std::unordered_set<std::string> outer_names;
                for (const auto& e : opt_exprs)
                    outer_names.insert(static_cast<const ColNode*>(e.node().get())->name);

                // Look through column-preserving nodes (SORT, FILTER) for an
                // inner SELECT.  SORT and FILTER never add or remove columns,
                // so if an inner SELECT already projects exactly the same set
                // (or a superset), the outer SELECT is redundant.
                auto look = in;
                while (look && (look->type() == NodeType::SORT ||
                                look->type() == NodeType::FILTER)) {
                    look = look->input();
                }
                if (look && look->type() == NodeType::SELECT) {
                    auto inner_sel = static_cast<const SelectNode*>(look.get());
                    std::unordered_set<std::string> inner_names;
                    for (const auto& e : inner_sel->exprs) {
                        if (e.type() == ExprType::COL)
                            inner_names.insert(static_cast<const ColNode*>(e.node().get())->name);
                        else if (e.type() == ExprType::ALIAS)
                            inner_names.insert(static_cast<const AliasNode*>(e.node().get())->name);
                    }
                    bool all_covered = std::all_of(outer_names.begin(), outer_names.end(),
                        [&](const std::string& name) { return inner_names.count(name) > 0; });
                    if (all_covered) {
                        changed = true;
                        return in;  // Drop the redundant outer SELECT entirely.
                    }
                }
            }

            if (local_changed || in != n->input())
                return std::make_shared<SelectNode>(in, opt_exprs);
            return node;
        }
        case NodeType::WITH_COLUMN: {
            auto n = static_cast<const WithColumnNode*>(node.get());
            auto opt_expr = optimize_expr(n->expr, changed);
            if (opt_expr.node() != n->expr.node() || in != n->input())
                return std::make_shared<WithColumnNode>(in, n->name, opt_expr);
            return node;
        }
        case NodeType::GROUP_BY: {
            auto n = static_cast<const GroupByNode*>(node.get());
            if (in == n->input()) return node;
            changed = true;
            return std::make_shared<GroupByNode>(in, n->keys);
        }
        case NodeType::AGGREGATE: {
            auto n = static_cast<const AggregateNode*>(node.get());
            bool local_changed = false;
            auto opt_map = optimize_agg_map(n->agg_map, changed, local_changed);
            if (local_changed || in != n->input())
                return std::make_shared<AggregateNode>(in, opt_map);
            return node;
        }
        case NodeType::JOIN: {
            auto n = static_cast<const JoinNode*>(node.get());
            auto r = opt_right ? opt_right : n->right;
            if (in == n->input() && r == n->right) return node;
            changed = true;
            return std::make_shared<JoinNode>(in, r, n->on, n->how);
        }
        case NodeType::SORT: {
            auto n = static_cast<const SortNode*>(node.get());
            if (in == n->input()) return node;
            changed = true;
            return std::make_shared<SortNode>(in, n->columns, n->ascending);
        }
        case NodeType::HEAD: {
            auto n = static_cast<const HeadNode*>(node.get());
            if (in && in->type() == NodeType::SELECT) {
                auto sel = static_cast<const SelectNode*>(in.get());
                changed = true;
                return std::make_shared<SelectNode>(
                    std::make_shared<HeadNode>(sel->input(), n->n), sel->exprs);
            }
            if (in == n->input()) return node;
            changed = true;
            return std::make_shared<HeadNode>(in, n->n);
        }
    }
    return node;
}

std::shared_ptr<DAGNode> pushdown_projections(
    const std::shared_ptr<DAGNode>& node, 
    const std::unordered_set<std::string>& required_cols,
    bool select_all) 
{
    if (!node) return nullptr;

    std::unordered_set<std::string> needs = required_cols;
    bool child_select_all = select_all;

    switch (node->type()) {
        case NodeType::SELECT: {
            auto n = static_cast<const SelectNode*>(node.get());
            needs.clear();
            for (const auto& e : n->exprs) {
                auto c = get_referenced_cols(e);
                needs.insert(c.begin(), c.end());
            }
            child_select_all = false;
            auto new_input = pushdown_projections(n->input(), needs, child_select_all);
            if (new_input != n->input()) return std::make_shared<SelectNode>(new_input, n->exprs);
            return node;
        }
        case NodeType::AGGREGATE: {
            auto n = static_cast<const AggregateNode*>(node.get());
            needs.clear();
            for (const auto& kv : n->agg_map) {
                auto c = get_referenced_cols(kv.second);
                needs.insert(c.begin(), c.end());
            }
            child_select_all = false;
            auto new_input = pushdown_projections(n->input(), needs, child_select_all);
            if (new_input != n->input()) return std::make_shared<AggregateNode>(new_input, n->agg_map);
            return node;
        }
        case NodeType::FILTER: {
            auto n = static_cast<const FilterNode*>(node.get());
            auto c = get_referenced_cols(n->predicate);
            needs.insert(c.begin(), c.end());
            auto new_input = pushdown_projections(n->input(), needs, child_select_all);
            if (new_input != n->input()) return std::make_shared<FilterNode>(new_input, n->predicate);
            return node;
        }
        case NodeType::WITH_COLUMN: {
            auto n = static_cast<const WithColumnNode*>(node.get());
            needs.erase(n->name);
            auto c = get_referenced_cols(n->expr);
            needs.insert(c.begin(), c.end());
            auto new_input = pushdown_projections(n->input(), needs, child_select_all);
            if (new_input != n->input()) return std::make_shared<WithColumnNode>(new_input, n->name, n->expr);
            return node;
        }
        case NodeType::GROUP_BY: {
            auto n = static_cast<const GroupByNode*>(node.get());
            needs.insert(n->keys.begin(), n->keys.end());
            auto new_input = pushdown_projections(n->input(), needs, child_select_all);
            if (new_input != n->input()) return std::make_shared<GroupByNode>(new_input, n->keys);
            return node;
        }
        case NodeType::JOIN: {
            auto n = static_cast<const JoinNode*>(node.get());
            // Without schema, we cannot divide columns between left and right safely.
            // Require all columns from both sides.
            auto new_left = pushdown_projections(n->input(), {}, true);
            auto new_right = pushdown_projections(n->right, {}, true);
            if (new_left != n->input() || new_right != n->right) {
                return std::make_shared<JoinNode>(new_left, new_right, n->on, n->how);
            }
            return node;
        }
        case NodeType::SORT: {
            auto n = static_cast<const SortNode*>(node.get());
            needs.insert(n->columns.begin(), n->columns.end());
            auto new_input = pushdown_projections(n->input(), needs, child_select_all);
            if (new_input != n->input()) return std::make_shared<SortNode>(new_input, n->columns, n->ascending);
            return node;
        }
        case NodeType::HEAD: {
            auto n = static_cast<const HeadNode*>(node.get());
            auto new_input = pushdown_projections(n->input(), needs, child_select_all);
            if (new_input != n->input()) return std::make_shared<HeadNode>(new_input, n->n);
            return node;
        }
        case NodeType::SCAN: {
            if (!select_all && !needs.empty()) {
                std::vector<std::string> sorted_needs(needs.begin(), needs.end());
                std::sort(sorted_needs.begin(), sorted_needs.end());
                std::vector<Expr> exprs;
                for (const auto& c : sorted_needs) exprs.push_back(col(c));
                return std::make_shared<SelectNode>(node, exprs);
            }
            return node;
        }
        default: return node;
    }
}

std::shared_ptr<DAGNode> optimize(const std::shared_ptr<DAGNode>& root) {
    if (!root) return root;

    // 1. Projection pushdown (top-down pass)
    auto curr = pushdown_projections(root, {}, true);

    // 2. Fixed-point rule application (bottom-up pass for constant folding, predicate/limit pushdown)
    bool changed = true;
    while (changed) {
        changed = false;
        curr = apply_rules(curr, changed);
    }
    return curr;
}

}
