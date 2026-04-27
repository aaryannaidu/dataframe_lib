#include "Expression.hpp"
#include "Compute.hpp"

#include <stdexcept>

namespace dataframelib {

namespace {

arrow::Datum eval_node(const std::shared_ptr<ExprNode>& node,
                       const std::shared_ptr<arrow::Table>& table);

arrow::Datum eval_col(const ColNode& n, const std::shared_ptr<arrow::Table>& table) {
    auto col = table->GetColumnByName(n.name);
    if (!col)
        throw std::runtime_error("Column not found: \"" + n.name + "\"");
    return arrow::Datum(col);
}

arrow::Datum eval_lit(const LitNode& n) {
    return std::visit([](auto&& v) -> arrow::Datum {
        return arrow::Datum(arrow::MakeScalar(v));
    }, n.value);
}

arrow::Datum eval_alias(const AliasNode& n, const std::shared_ptr<arrow::Table>& table) {
    return eval_node(n.input, table);
}

arrow::Datum eval_binop(const BinaryOpNode& n, const std::shared_ptr<arrow::Table>& table) {
    return compute::apply_binop(eval_node(n.left, table), eval_node(n.right, table), n.op);
}

arrow::Datum eval_unary(const UnaryOpNode& n, const std::shared_ptr<arrow::Table>& table) {
    return compute::apply_unary(eval_node(n.input, table), n.op);
}

arrow::Datum eval_agg(const AggNode& n, const std::shared_ptr<arrow::Table>& table) {
    return compute::apply_agg(eval_node(n.input, table), n.agg);
}

arrow::Datum eval_strfunc(const StrFuncNode& n, const std::shared_ptr<arrow::Table>& table) {
    return compute::apply_strfunc(eval_node(n.input, table), n.func, n.arg);
}

arrow::Datum eval_node(const std::shared_ptr<ExprNode>& node,
                       const std::shared_ptr<arrow::Table>& table) {
    switch (node->type()) {
        case ExprType::COL:     return eval_col    (static_cast<const ColNode&>(*node),      table);
        case ExprType::LIT:     return eval_lit    (static_cast<const LitNode&>(*node));
        case ExprType::ALIAS:   return eval_alias  (static_cast<const AliasNode&>(*node),    table);
        case ExprType::BINOP:   return eval_binop  (static_cast<const BinaryOpNode&>(*node), table);
        case ExprType::UNARYOP: return eval_unary  (static_cast<const UnaryOpNode&>(*node),  table);
        case ExprType::AGG:     return eval_agg    (static_cast<const AggNode&>(*node),      table);
        case ExprType::STRFUNC: return eval_strfunc(static_cast<const StrFuncNode&>(*node),  table);
    }
    throw std::runtime_error("evaluate: unknown ExprType");
}

} // anonymous namespace

arrow::Datum evaluate(const Expr& expr, const std::shared_ptr<arrow::Table>& table) {
    return eval_node(expr.node(), table);
}

} // namespace dataframelib
