#include "Expression.hpp"

#include <arrow/compute/api.h>
#include <arrow/scalar.h>
#include <arrow/type_traits.h>

#include <sstream>
#include <stdexcept>

namespace dataframelib {

namespace {

// ── Type helpers ───────────────────────────────────────────────────────────

std::shared_ptr<arrow::DataType> datum_type(const arrow::Datum& d) {
    switch (d.kind()) {
        case arrow::Datum::SCALAR:        return d.scalar()->type;
        case arrow::Datum::ARRAY:         return d.array()->type;
        case arrow::Datum::CHUNKED_ARRAY: return d.chunked_array()->type();
        default:
            throw std::runtime_error("evaluate: unsupported Datum kind");
    }
}

bool is_numeric(const std::shared_ptr<arrow::DataType>& t) {
    return arrow::is_integer(t->id()) || arrow::is_floating(t->id());
}

bool is_float(const std::shared_ptr<arrow::DataType>& t) {
    return arrow::is_floating(t->id());
}

bool is_string(const std::shared_ptr<arrow::DataType>& t) {
    return t->id() == arrow::Type::STRING || t->id() == arrow::Type::LARGE_STRING;
}

bool is_boolean(const std::shared_ptr<arrow::DataType>& t) {
    return t->id() == arrow::Type::BOOL;
}

arrow::Datum cast_to(const arrow::Datum& d, const std::shared_ptr<arrow::DataType>& target) {
    auto result = arrow::compute::Cast(d, target);
    if (!result.ok())
        throw std::runtime_error("Type cast failed: " + result.status().message());
    return result.ValueOrDie();
}

// ── Call a named Arrow compute function, throwing on failure ───────────────

arrow::Datum call(const std::string& func,
                  const std::vector<arrow::Datum>& args,
                  const arrow::compute::FunctionOptions* opts = nullptr)
{
    auto result = arrow::compute::CallFunction(func, args, opts);
    if (!result.ok())
        throw std::runtime_error("Arrow compute \"" + func + "\" failed: " +
                                 result.status().message());
    return result.ValueOrDie();
}

// ── Forward declaration ────────────────────────────────────────────────────

arrow::Datum eval_node(const std::shared_ptr<ExprNode>& node,
                       const std::shared_ptr<arrow::Table>& table);

// ── COL ────────────────────────────────────────────────────────────────────

arrow::Datum eval_col(const ColNode& n, const std::shared_ptr<arrow::Table>& table) {
    auto col = table->GetColumnByName(n.name);
    if (!col) {
        throw std::runtime_error("Column not found: \"" + n.name + "\"");
    }
    return arrow::Datum(col);
}

// ── LIT ────────────────────────────────────────────────────────────────────

arrow::Datum eval_lit(const LitNode& n) {
    return std::visit([](auto&& v) -> arrow::Datum {
        return arrow::Datum(arrow::MakeScalar(v));
    }, n.value);
}

// ── BINOP ──────────────────────────────────────────────────────────────────

arrow::Datum eval_binop(const BinaryOpNode& n, const std::shared_ptr<arrow::Table>& table) {
    auto left  = eval_node(n.left,  table);
    auto right = eval_node(n.right, table);

    auto lt = datum_type(left);
    auto rt = datum_type(right);

    // ── Arithmetic operators: require numeric types, promote int→float ──
    auto is_arithmetic = [](BinOp op) {
        return op == BinOp::ADD || op == BinOp::SUB ||
               op == BinOp::MUL || op == BinOp::DIV || op == BinOp::MOD;
    };

    if (is_arithmetic(n.op)) {
        if (!is_numeric(lt) || !is_numeric(rt)) {
            throw std::runtime_error(
                "Arithmetic operator applied to non-numeric types: " +
                lt->ToString() + " and " + rt->ToString());
        }
        // int + float → promote int side to float64
        if (is_float(lt) != is_float(rt)) {
            if (!is_float(lt)) left  = cast_to(left,  arrow::float64());
            else                right = cast_to(right, arrow::float64());
        }

        if (n.op == BinOp::MOD) {
            // Arrow has no direct modulo function; compute as a - (a/b)*b
            auto div_r = call("divide",  {left, right});
            auto mul_r = call("multiply", {div_r, right});
            return call("subtract", {left, mul_r});
        }

        static const std::unordered_map<BinOp, std::string> arith_map = {
            {BinOp::ADD, "add"}, {BinOp::SUB, "subtract"},
            {BinOp::MUL, "multiply"}, {BinOp::DIV, "divide"},
        };
        return call(arith_map.at(n.op), {left, right});
    }

    // ── Comparison operators ────────────────────────────────────────────
    auto is_comparison = [](BinOp op) {
        return op == BinOp::EQ  || op == BinOp::NEQ ||
               op == BinOp::LT  || op == BinOp::LTE ||
               op == BinOp::GT  || op == BinOp::GTE;
    };

    if (is_comparison(n.op)) {
        // Allow numeric↔numeric (with promotion) and string↔string
        if (is_numeric(lt) && is_numeric(rt)) {
            if (is_float(lt) != is_float(rt)) {
                if (!is_float(lt)) left  = cast_to(left,  arrow::float64());
                else                right = cast_to(right, arrow::float64());
            }
        } else if (!(is_string(lt) && is_string(rt)) &&
                   !(is_boolean(lt) && is_boolean(rt))) {
            throw std::runtime_error(
                "Comparison between incompatible types: " +
                lt->ToString() + " and " + rt->ToString());
        }

        static const std::unordered_map<BinOp, std::string> cmp_map = {
            {BinOp::EQ,  "equal"},        {BinOp::NEQ, "not_equal"},
            {BinOp::LT,  "less"},         {BinOp::LTE, "less_equal"},
            {BinOp::GT,  "greater"},      {BinOp::GTE, "greater_equal"},
        };
        return call(cmp_map.at(n.op), {left, right});
    }

    // ── Boolean operators ───────────────────────────────────────────────
    if (n.op == BinOp::AND || n.op == BinOp::OR) {
        if (!is_boolean(lt) || !is_boolean(rt)) {
            throw std::runtime_error(
                "Boolean operator applied to non-boolean types: " +
                lt->ToString() + " and " + rt->ToString());
        }
        return call(n.op == BinOp::AND ? "and" : "or", {left, right});
    }

    throw std::runtime_error("Unknown binary operator");
}

// ── UNARYOP ────────────────────────────────────────────────────────────────

arrow::Datum eval_unary(const UnaryOpNode& n, const std::shared_ptr<arrow::Table>& table) {
    auto input = eval_node(n.input, table);
    auto t     = datum_type(input);

    switch (n.op) {
        case UnaryOp::ABS:
            if (!is_numeric(t))
                throw std::runtime_error("abs() requires a numeric type, got " + t->ToString());
            return call("abs", {input});

        case UnaryOp::NEGATE:
            if (!is_numeric(t))
                throw std::runtime_error("negate requires a numeric type, got " + t->ToString());
            return call("negate", {input});

        case UnaryOp::NOT:
            if (!is_boolean(t))
                throw std::runtime_error("~ (NOT) requires a boolean type, got " + t->ToString());
            return call("invert", {input});

        case UnaryOp::IS_NULL:
            return call("is_null",  {input});

        case UnaryOp::IS_NOT_NULL:
            return call("is_valid", {input});
    }
    throw std::runtime_error("Unknown unary operator");
}

// ── AGG ────────────────────────────────────────────────────────────────────

arrow::Datum eval_agg(const AggNode& n, const std::shared_ptr<arrow::Table>& table) {
    auto input = eval_node(n.input, table);
    auto t     = datum_type(input);

    switch (n.agg) {
        case AggType::SUM:
            if (!is_numeric(t))
                throw std::runtime_error("sum() requires a numeric type, got " + t->ToString());
            return call("sum", {input});

        case AggType::MEAN:
            if (!is_numeric(t))
                throw std::runtime_error("mean() requires a numeric type, got " + t->ToString());
            return call("mean", {input});

        case AggType::COUNT: {
            // count non-null values
            arrow::compute::CountOptions opts{arrow::compute::CountOptions::ONLY_VALID};
            return call("count", {input}, &opts);
        }

        case AggType::MIN: {
            if (!is_numeric(t) && !is_string(t))
                throw std::runtime_error("min() requires numeric or string type, got " + t->ToString());
            // min_max returns a struct scalar; extract the "min" field
            auto mm = call("min_max", {input});
            auto struct_scalar = std::static_pointer_cast<arrow::StructScalar>(mm.scalar());
            return arrow::Datum(struct_scalar->field("min").ValueOrDie());
        }

        case AggType::MAX: {
            if (!is_numeric(t) && !is_string(t))
                throw std::runtime_error("max() requires numeric or string type, got " + t->ToString());
            auto mm = call("min_max", {input});
            auto struct_scalar = std::static_pointer_cast<arrow::StructScalar>(mm.scalar());
            return arrow::Datum(struct_scalar->field("max").ValueOrDie());
        }
    }
    throw std::runtime_error("Unknown aggregation type");
}

// ── STRFUNC ────────────────────────────────────────────────────────────────

arrow::Datum eval_strfunc(const StrFuncNode& n, const std::shared_ptr<arrow::Table>& table) {
    auto input = eval_node(n.input, table);
    auto t     = datum_type(input);

    if (!is_string(t))
        throw std::runtime_error("String function applied to non-string column, got " + t->ToString());

    switch (n.func) {
        case StrFunc::LENGTH:
            return call("utf8_length", {input});

        case StrFunc::TO_LOWER:
            return call("utf8_lower", {input});

        case StrFunc::TO_UPPER:
            return call("utf8_upper", {input});

        case StrFunc::CONTAINS: {
            arrow::compute::MatchSubstringOptions opts{n.arg};
            return call("match_substring", {input}, &opts);
        }

        case StrFunc::STARTS_WITH: {
            arrow::compute::MatchSubstringOptions opts{n.arg};
            return call("starts_with", {input}, &opts);
        }

        case StrFunc::ENDS_WITH: {
            arrow::compute::MatchSubstringOptions opts{n.arg};
            return call("ends_with", {input}, &opts);
        }
    }
    throw std::runtime_error("Unknown string function");
}

// ── ALIAS ──────────────────────────────────────────────────────────────────
// The alias is handled at the DataFrame level (column renaming).
// The evaluator just evaluates the inner expression transparently.

arrow::Datum eval_alias(const AliasNode& n, const std::shared_ptr<arrow::Table>& table) {
    return eval_node(n.input, table);
}

// ── Main dispatch ──────────────────────────────────────────────────────────

arrow::Datum eval_node(const std::shared_ptr<ExprNode>& node,
                       const std::shared_ptr<arrow::Table>& table)
{
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

// ── Public entry point ─────────────────────────────────────────────────────

arrow::Datum evaluate(const Expr& expr, const std::shared_ptr<arrow::Table>& table) {
    return eval_node(expr.node(), table);
}

} // namespace dataframelib
