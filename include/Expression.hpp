#pragma once

#include <memory>
#include <string>
#include <variant>

#include <arrow/api.h>
#include <arrow/compute/api.h>

namespace dataframelib {

// ── Operator enums ─────────────────────────────────────────────────────────

enum class ExprType {
    COL, LIT, ALIAS, BINOP, UNARYOP, AGG, STRFUNC
};

enum class BinOp {
    ADD, SUB, MUL, DIV, MOD,        // arithmetic
    EQ, NEQ, LT, LTE, GT, GTE,      // comparison
    AND, OR                          // boolean
};

enum class UnaryOp {
    ABS, NEGATE,
    NOT,                             // boolean NOT (~)
    IS_NULL, IS_NOT_NULL
};

enum class AggType {
    SUM, MEAN, COUNT, MIN, MAX
};

enum class StrFunc {
    LENGTH, CONTAINS, STARTS_WITH, ENDS_WITH, TO_LOWER, TO_UPPER
};

// ── Literal value type ─────────────────────────────────────────────────────

using LitValue = std::variant<int32_t, int64_t, float, double, bool, std::string>;

// ── Internal AST node hierarchy ────────────────────────────────────────────
//
// ExprNode and its subclasses are internal implementation detail.
// Users only interact with the Expr value type below.

struct ExprNode {
    virtual ~ExprNode() = default;
    virtual ExprType type() const = 0;
};

struct ColNode : ExprNode {
    std::string name;
    explicit ColNode(std::string n) : name(std::move(n)) {}
    ExprType type() const override { return ExprType::COL; }
};

struct LitNode : ExprNode {
    LitValue value;
    explicit LitNode(LitValue v) : value(std::move(v)) {}
    ExprType type() const override { return ExprType::LIT; }
};

struct AliasNode : ExprNode {
    std::shared_ptr<ExprNode> input;
    std::string name;
    AliasNode(std::shared_ptr<ExprNode> in, std::string n)
        : input(std::move(in)), name(std::move(n)) {}
    ExprType type() const override { return ExprType::ALIAS; }
};

struct BinaryOpNode : ExprNode {
    std::shared_ptr<ExprNode> left;
    std::shared_ptr<ExprNode> right;
    BinOp op;
    BinaryOpNode(std::shared_ptr<ExprNode> l, std::shared_ptr<ExprNode> r, BinOp op)
        : left(std::move(l)), right(std::move(r)), op(op) {}
    ExprType type() const override { return ExprType::BINOP; }
};

struct UnaryOpNode : ExprNode {
    std::shared_ptr<ExprNode> input;
    UnaryOp op;
    UnaryOpNode(std::shared_ptr<ExprNode> in, UnaryOp op)
        : input(std::move(in)), op(op) {}
    ExprType type() const override { return ExprType::UNARYOP; }
};

struct AggNode : ExprNode {
    std::shared_ptr<ExprNode> input;
    AggType agg;
    AggNode(std::shared_ptr<ExprNode> in, AggType agg)
        : input(std::move(in)), agg(agg) {}
    ExprType type() const override { return ExprType::AGG; }
};

struct StrFuncNode : ExprNode {
    std::shared_ptr<ExprNode> input;
    StrFunc func;
    std::string arg;
    StrFuncNode(std::shared_ptr<ExprNode> in, StrFunc f, std::string a = "")
        : input(std::move(in)), func(f), arg(std::move(a)) {}
    ExprType type() const override { return ExprType::STRFUNC; }
};

// ── Expr: user-facing value type ───────────────────────────────────────────
//
// Copyable, movable value wrapper around a shared_ptr<ExprNode>.
// Supports dot-chaining: col("age").abs().alias("abs_age")
// All operators are overloaded to return new Expr values.

class Expr {
public:
    explicit Expr(std::shared_ptr<ExprNode> node) : node_(std::move(node)) {}

    // Access the underlying AST node (used by the evaluator and optimizer).
    const std::shared_ptr<ExprNode>& node() const { return node_; }

    ExprType type() const { return node_->type(); }

    // ── Alias ──────────────────────────────────────────────────────────────
    Expr alias(const std::string& name) const {
        return Expr(std::make_shared<AliasNode>(node_, name));
    }

    // ── Unary arithmetic ───────────────────────────────────────────────────
    Expr abs() const {
        return Expr(std::make_shared<UnaryOpNode>(node_, UnaryOp::ABS));
    }

    // ── Null checks ────────────────────────────────────────────────────────
    Expr is_null() const {
        return Expr(std::make_shared<UnaryOpNode>(node_, UnaryOp::IS_NULL));
    }
    Expr is_not_null() const {
        return Expr(std::make_shared<UnaryOpNode>(node_, UnaryOp::IS_NOT_NULL));
    }

    // ── String functions ───────────────────────────────────────────────────
    Expr length() const {
        return Expr(std::make_shared<StrFuncNode>(node_, StrFunc::LENGTH));
    }
    Expr contains(const std::string& s) const {
        return Expr(std::make_shared<StrFuncNode>(node_, StrFunc::CONTAINS, s));
    }
    Expr starts_with(const std::string& s) const {
        return Expr(std::make_shared<StrFuncNode>(node_, StrFunc::STARTS_WITH, s));
    }
    Expr ends_with(const std::string& s) const {
        return Expr(std::make_shared<StrFuncNode>(node_, StrFunc::ENDS_WITH, s));
    }
    Expr to_lower() const {
        return Expr(std::make_shared<StrFuncNode>(node_, StrFunc::TO_LOWER));
    }
    Expr to_upper() const {
        return Expr(std::make_shared<StrFuncNode>(node_, StrFunc::TO_UPPER));
    }

    // ── Aggregations ───────────────────────────────────────────────────────
    Expr sum() const {
        return Expr(std::make_shared<AggNode>(node_, AggType::SUM));
    }
    Expr mean() const {
        return Expr(std::make_shared<AggNode>(node_, AggType::MEAN));
    }
    Expr count() const {
        return Expr(std::make_shared<AggNode>(node_, AggType::COUNT));
    }
    Expr min() const {
        return Expr(std::make_shared<AggNode>(node_, AggType::MIN));
    }
    Expr max() const {
        return Expr(std::make_shared<AggNode>(node_, AggType::MAX));
    }

private:
    std::shared_ptr<ExprNode> node_;
};

// ── Operator overloads ─────────────────────────────────────────────────────

// Arithmetic
inline Expr operator+(Expr lhs, Expr rhs) {
    return Expr(std::make_shared<BinaryOpNode>(lhs.node(), rhs.node(), BinOp::ADD));
}
inline Expr operator-(Expr lhs, Expr rhs) {
    return Expr(std::make_shared<BinaryOpNode>(lhs.node(), rhs.node(), BinOp::SUB));
}
inline Expr operator*(Expr lhs, Expr rhs) {
    return Expr(std::make_shared<BinaryOpNode>(lhs.node(), rhs.node(), BinOp::MUL));
}
inline Expr operator/(Expr lhs, Expr rhs) {
    return Expr(std::make_shared<BinaryOpNode>(lhs.node(), rhs.node(), BinOp::DIV));
}
inline Expr operator%(Expr lhs, Expr rhs) {
    return Expr(std::make_shared<BinaryOpNode>(lhs.node(), rhs.node(), BinOp::MOD));
}

// Comparison
inline Expr operator==(Expr lhs, Expr rhs) {
    return Expr(std::make_shared<BinaryOpNode>(lhs.node(), rhs.node(), BinOp::EQ));
}
inline Expr operator!=(Expr lhs, Expr rhs) {
    return Expr(std::make_shared<BinaryOpNode>(lhs.node(), rhs.node(), BinOp::NEQ));
}
inline Expr operator<(Expr lhs, Expr rhs) {
    return Expr(std::make_shared<BinaryOpNode>(lhs.node(), rhs.node(), BinOp::LT));
}
inline Expr operator<=(Expr lhs, Expr rhs) {
    return Expr(std::make_shared<BinaryOpNode>(lhs.node(), rhs.node(), BinOp::LTE));
}
inline Expr operator>(Expr lhs, Expr rhs) {
    return Expr(std::make_shared<BinaryOpNode>(lhs.node(), rhs.node(), BinOp::GT));
}
inline Expr operator>=(Expr lhs, Expr rhs) {
    return Expr(std::make_shared<BinaryOpNode>(lhs.node(), rhs.node(), BinOp::GTE));
}

// Boolean
inline Expr operator&(Expr lhs, Expr rhs) {
    return Expr(std::make_shared<BinaryOpNode>(lhs.node(), rhs.node(), BinOp::AND));
}
inline Expr operator|(Expr lhs, Expr rhs) {
    return Expr(std::make_shared<BinaryOpNode>(lhs.node(), rhs.node(), BinOp::OR));
}
inline Expr operator~(Expr operand) {
    return Expr(std::make_shared<UnaryOpNode>(operand.node(), UnaryOp::NOT));
}

// ── Public API: col() and lit() ────────────────────────────────────────────

/// Create a column reference expression: col("age")
inline Expr col(const std::string& name) {
    return Expr(std::make_shared<ColNode>(name));
}

/// Create a literal expression: lit(42), lit(3.14f), lit(true), lit("hello")
/// T must be one of: int32_t, int64_t, float, double, bool, std::string.
template <typename T>
Expr lit(T value) {
    return Expr(std::make_shared<LitNode>(LitValue{std::move(value)}));
}

// ── Expression evaluator ───────────────────────────────────────────────────
//
// Evaluates an expression tree against an Arrow Table.
// Returns an arrow::Datum which may be a Scalar (aggregation result),
// Array, or ChunkedArray (element-wise result).
//
// Throws std::runtime_error on type mismatches or missing columns.
arrow::Datum evaluate(const Expr& expr, const std::shared_ptr<arrow::Table>& table);

} // namespace dataframelib
