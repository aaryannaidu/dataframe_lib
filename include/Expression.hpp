#pragma once

#include <memory>
#include <string>
#include <variant>

#include <arrow/api.h>

namespace dataframelib {

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

using LitValue = std::variant<int32_t, int64_t, float, double, bool, std::string>;

// ExprNode and its subclasses are the internal AST; users only interact with Expr.
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

// Value wrapper around shared_ptr<ExprNode>; supports dot-chaining like col("x").abs().alias("y").
class Expr {
public:
    explicit Expr(std::shared_ptr<ExprNode> node) : node_(std::move(node)) {}

    // Implicit construction from numeric literals so expressions like col("age") > 30 compile.
    // String constructors are explicit to avoid ambiguity in select({"a","b"}) vs select({col,col}).
    Expr(int v)                : node_(std::make_shared<LitNode>(LitValue{int32_t(v)})) {}
    Expr(int64_t v)            : node_(std::make_shared<LitNode>(LitValue{v})) {}
    Expr(double v)             : node_(std::make_shared<LitNode>(LitValue{v})) {}
    Expr(float v)              : node_(std::make_shared<LitNode>(LitValue{v})) {}
    explicit Expr(const std::string& v) : node_(std::make_shared<LitNode>(LitValue{v})) {}
    explicit Expr(const char* v)        : node_(std::make_shared<LitNode>(LitValue{std::string(v)})) {}

    // Access the underlying AST node (used by the evaluator and optimizer).
    const std::shared_ptr<ExprNode>& node() const { return node_; }

    ExprType type() const { return node_->type(); }

    Expr alias(const std::string& name) const {
        return Expr(std::make_shared<AliasNode>(node_, name));
    }

    Expr abs() const {
        return Expr(std::make_shared<UnaryOpNode>(node_, UnaryOp::ABS));
    }

    Expr is_null() const {
        return Expr(std::make_shared<UnaryOpNode>(node_, UnaryOp::IS_NULL));
    }
    Expr is_not_null() const {
        return Expr(std::make_shared<UnaryOpNode>(node_, UnaryOp::IS_NOT_NULL));
    }

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

// Helper used by operator overloads below.
namespace detail {
inline Expr bop(Expr l, Expr r, BinOp op) {
    return Expr(std::make_shared<BinaryOpNode>(l.node(), r.node(), op));
}
}

inline Expr operator+(Expr l, Expr r)  { return detail::bop(l, r, BinOp::ADD); }
inline Expr operator-(Expr l, Expr r)  { return detail::bop(l, r, BinOp::SUB); }
inline Expr operator*(Expr l, Expr r)  { return detail::bop(l, r, BinOp::MUL); }
inline Expr operator/(Expr l, Expr r)  { return detail::bop(l, r, BinOp::DIV); }
inline Expr operator%(Expr l, Expr r)  { return detail::bop(l, r, BinOp::MOD); }
inline Expr operator==(Expr l, Expr r) { return detail::bop(l, r, BinOp::EQ); }
inline Expr operator!=(Expr l, Expr r) { return detail::bop(l, r, BinOp::NEQ); }
inline Expr operator< (Expr l, Expr r) { return detail::bop(l, r, BinOp::LT); }
inline Expr operator<=(Expr l, Expr r) { return detail::bop(l, r, BinOp::LTE); }
inline Expr operator> (Expr l, Expr r) { return detail::bop(l, r, BinOp::GT); }
inline Expr operator>=(Expr l, Expr r) { return detail::bop(l, r, BinOp::GTE); }
inline Expr operator& (Expr l, Expr r) { return detail::bop(l, r, BinOp::AND); }
inline Expr operator| (Expr l, Expr r) { return detail::bop(l, r, BinOp::OR); }
inline Expr operator~(Expr e) {
    return Expr(std::make_shared<UnaryOpNode>(e.node(), UnaryOp::NOT));
}

inline Expr col(const std::string& name) {
    return Expr(std::make_shared<ColNode>(name));
}

template <typename T>
Expr lit(T value) {
    return Expr(std::make_shared<LitNode>(LitValue{std::move(value)}));
}

// String-literal comparisons: col("dept") == "HR" or col("dept") != "HR"
inline Expr operator==(Expr l, const char* r)        { return detail::bop(l, lit(std::string(r)), BinOp::EQ);  }
inline Expr operator!=(Expr l, const char* r)        { return detail::bop(l, lit(std::string(r)), BinOp::NEQ); }
inline Expr operator==(Expr l, const std::string& r) { return detail::bop(l, lit(r), BinOp::EQ);  }
inline Expr operator!=(Expr l, const std::string& r) { return detail::bop(l, lit(r), BinOp::NEQ); }

// Evaluates an expression against an Arrow Table; returns a Datum (scalar or array).
arrow::Datum evaluate(const Expr& expr, const std::shared_ptr<arrow::Table>& table);

} 
