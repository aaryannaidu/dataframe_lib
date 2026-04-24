#include "Expression.hpp"

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/array/concatenate.h>

#include <cassert>
#include <iostream>
#include <string>

using namespace dataframelib;

// ── Helpers ────────────────────────────────────────────────────────────────

// Build a shared_ptr<arrow::Table> from hand-crafted columns.
// x: int32  [1, 2, 3, null]
// y: float32 [1.5, 2.5, 3.5, 4.5]
// s: utf8    ["hello", "world", "foo", null]
// b: bool    [true, false, true, null]
static std::shared_ptr<arrow::Table> make_table() {
    // x: int32 with one null
    arrow::Int32Builder xb;
    xb.Append(1); xb.Append(2); xb.Append(3); xb.AppendNull();
    std::shared_ptr<arrow::Array> x_arr;
    xb.Finish(&x_arr);

    // y: float32, no nulls
    arrow::FloatBuilder yb;
    yb.Append(1.5f); yb.Append(2.5f); yb.Append(3.5f); yb.Append(4.5f);
    std::shared_ptr<arrow::Array> y_arr;
    yb.Finish(&y_arr);

    // s: string with one null
    arrow::StringBuilder sb;
    sb.Append("hello"); sb.Append("world"); sb.Append("foo"); sb.AppendNull();
    std::shared_ptr<arrow::Array> s_arr;
    sb.Finish(&s_arr);

    // b: bool with one null
    arrow::BooleanBuilder bb;
    bb.Append(true); bb.Append(false); bb.Append(true); bb.AppendNull();
    std::shared_ptr<arrow::Array> b_arr;
    bb.Finish(&b_arr);

    auto schema = arrow::schema({
        arrow::field("x", arrow::int32()),
        arrow::field("y", arrow::float32()),
        arrow::field("s", arrow::utf8()),
        arrow::field("b", arrow::boolean()),
    });

    return arrow::Table::Make(schema, {x_arr, y_arr, s_arr, b_arr});
}

// Extract a flat int32 array from a Datum (ChunkedArray or Array).
static std::shared_ptr<arrow::Int32Array> as_int32(const arrow::Datum& d) {
    if (d.is_chunked_array()) {
        auto combined = arrow::Concatenate(d.chunked_array()->chunks()).ValueOrDie();
        return std::static_pointer_cast<arrow::Int32Array>(combined);
    }
    return std::static_pointer_cast<arrow::Int32Array>(d.make_array());
}

static std::shared_ptr<arrow::BooleanArray> as_bool(const arrow::Datum& d) {
    if (d.is_chunked_array()) {
        auto combined = arrow::Concatenate(d.chunked_array()->chunks()).ValueOrDie();
        return std::static_pointer_cast<arrow::BooleanArray>(combined);
    }
    return std::static_pointer_cast<arrow::BooleanArray>(d.make_array());
}

static std::shared_ptr<arrow::DoubleArray> as_float64(const arrow::Datum& d) {
    if (d.is_chunked_array()) {
        auto combined = arrow::Concatenate(d.chunked_array()->chunks()).ValueOrDie();
        return std::static_pointer_cast<arrow::DoubleArray>(combined);
    }
    return std::static_pointer_cast<arrow::DoubleArray>(d.make_array());
}

// ── Tests ──────────────────────────────────────────────────────────────────

static void test_col_plus_lit(const std::shared_ptr<arrow::Table>& t) {
    // col("x") + lit(10)  →  [11, 12, 13, null]
    auto result = evaluate(col("x") + lit(int32_t{10}), t);
    auto arr = as_int32(result);
    assert(arr->length() == 4);
    assert(arr->Value(0) == 11);
    assert(arr->Value(1) == 12);
    assert(arr->Value(2) == 13);
    assert(arr->IsNull(3));
    std::cout << "[PASS] col(x) + lit(10)\n";
}

static void test_int_float_promotion(const std::shared_ptr<arrow::Table>& t) {
    // col("x") + col("y")  →  int + float32 = float64: [2.5, 4.5, 6.5, null]
    auto result = evaluate(col("x") + col("y"), t);
    auto arr = as_float64(result);
    assert(arr->length() == 4);
    assert(std::abs(arr->Value(0) - 2.5) < 1e-6);
    assert(std::abs(arr->Value(1) - 4.5) < 1e-6);
    assert(std::abs(arr->Value(2) - 6.5) < 1e-6);
    assert(arr->IsNull(3));
    std::cout << "[PASS] col(x) + col(y)  [int+float promotion]\n";
}

static void test_comparison(const std::shared_ptr<arrow::Table>& t) {
    // col("x") > lit(2)  →  [false, false, true, null]
    auto result = evaluate(col("x") > lit(int32_t{2}), t);
    auto arr = as_bool(result);
    assert(arr->length() == 4);
    assert(arr->Value(0) == false);
    assert(arr->Value(1) == false);
    assert(arr->Value(2) == true);
    assert(arr->IsNull(3));
    std::cout << "[PASS] col(x) > lit(2)\n";
}

static void test_boolean_and(const std::shared_ptr<arrow::Table>& t) {
    // (col("x") > lit(1)) & (col("x") < lit(3))  →  [false, true, false, null]
    auto result = evaluate(
        (col("x") > lit(int32_t{1})) & (col("x") < lit(int32_t{3})), t);
    auto arr = as_bool(result);
    assert(arr->length() == 4);
    assert(arr->Value(0) == false);
    assert(arr->Value(1) == true);
    assert(arr->Value(2) == false);
    // null & ? = null (Kleene logic)
    assert(arr->IsNull(3));
    std::cout << "[PASS] (col(x)>1) & (col(x)<3)\n";
}

static void test_boolean_not(const std::shared_ptr<arrow::Table>& t) {
    // ~col("b")  →  [false, true, false, null]
    auto result = evaluate(~col("b"), t);
    auto arr = as_bool(result);
    assert(arr->length() == 4);
    assert(arr->Value(0) == false);
    assert(arr->Value(1) == true);
    assert(arr->Value(2) == false);
    assert(arr->IsNull(3));
    std::cout << "[PASS] ~col(b)\n";
}

static void test_is_null(const std::shared_ptr<arrow::Table>& t) {
    // col("x").is_null()  →  [false, false, false, true]
    auto result = evaluate(col("x").is_null(), t);
    auto arr = as_bool(result);
    assert(arr->length() == 4);
    assert(arr->Value(0) == false);
    assert(arr->Value(3) == true);
    std::cout << "[PASS] col(x).is_null()\n";
}

static void test_abs(const std::shared_ptr<arrow::Table>& t) {
    // build a table with negatives: z = [-1, -2, 3, null]
    arrow::Int32Builder zb;
    zb.Append(-1); zb.Append(-2); zb.Append(3); zb.AppendNull();
    std::shared_ptr<arrow::Array> z_arr;
    zb.Finish(&z_arr);
    auto schema  = arrow::schema({arrow::field("z", arrow::int32())});
    auto ztable  = arrow::Table::Make(schema, {z_arr});

    auto result = evaluate(col("z").abs(), ztable);
    auto arr    = as_int32(result);
    assert(arr->Value(0) == 1);
    assert(arr->Value(1) == 2);
    assert(arr->Value(2) == 3);
    assert(arr->IsNull(3));
    std::cout << "[PASS] col(z).abs()\n";
}

static void test_modulo(const std::shared_ptr<arrow::Table>& t) {
    // col("x") % lit(2)  →  [1, 0, 1, null]
    auto result = evaluate(col("x") % lit(int32_t{2}), t);
    auto arr = as_int32(result);
    assert(arr->Value(0) == 1);
    assert(arr->Value(1) == 0);
    assert(arr->Value(2) == 1);
    assert(arr->IsNull(3));
    std::cout << "[PASS] col(x) % lit(2)\n";
}

static void test_aggregations(const std::shared_ptr<arrow::Table>& t) {
    // sum: 1+2+3 = 6 (null skipped)
    auto s = evaluate(col("x").sum(), t);
    assert(s.scalar()->CastTo(arrow::int64()).ValueOrDie()->ToString() == "6");
    std::cout << "[PASS] col(x).sum() = 6\n";

    // count: 3 non-null values
    auto c = evaluate(col("x").count(), t);
    assert(std::static_pointer_cast<arrow::Int64Scalar>(c.scalar())->value == 3);
    std::cout << "[PASS] col(x).count() = 3\n";

    // min: 1
    auto mn = evaluate(col("x").min(), t);
    assert(std::static_pointer_cast<arrow::Int32Scalar>(mn.scalar())->value == 1);
    std::cout << "[PASS] col(x).min() = 1\n";

    // max: 3
    auto mx = evaluate(col("x").max(), t);
    assert(std::static_pointer_cast<arrow::Int32Scalar>(mx.scalar())->value == 3);
    std::cout << "[PASS] col(x).max() = 3\n";
}

static void test_string_functions(const std::shared_ptr<arrow::Table>& t) {
    // length: ["hello"=5, "world"=5, "foo"=3, null]
    auto result = evaluate(col("s").length(), t);
    auto arr = as_int32(result);
    assert(arr->Value(0) == 5);
    assert(arr->Value(1) == 5);
    assert(arr->Value(2) == 3);
    assert(arr->IsNull(3));
    std::cout << "[PASS] col(s).length()\n";

    // contains("o"): [true, true, true, null]
    auto cont = as_bool(evaluate(col("s").contains("o"), t));
    assert(cont->Value(0) == true);   // "hello" has 'o'
    assert(cont->Value(1) == true);   // "world" has 'o'
    assert(cont->Value(2) == true);   // "foo" has 'o'
    assert(cont->IsNull(3));
    std::cout << "[PASS] col(s).contains(\"o\")\n";

    // starts_with("h"): [true, false, false, null]
    auto sw = as_bool(evaluate(col("s").starts_with("h"), t));
    assert(sw->Value(0) == true);
    assert(sw->Value(1) == false);
    assert(sw->IsNull(3));
    std::cout << "[PASS] col(s).starts_with(\"h\")\n";

    // ends_with("o"): "hello"→true, "world"→false, "foo"→true, null→null
    auto ew = as_bool(evaluate(col("s").ends_with("o"), t));
    assert(ew->Value(0) == true);   // "hello" ends with 'o'
    assert(ew->Value(1) == false);  // "world" does not
    assert(ew->Value(2) == true);   // "foo" ends with 'o'
    assert(ew->IsNull(3));
    std::cout << "[PASS] col(s).ends_with(\"o\")\n";

    // to_upper: ["HELLO", "WORLD", "FOO", null]
    auto up = evaluate(col("s").to_upper(), t);
    auto up_arr = std::static_pointer_cast<arrow::StringArray>(
        up.is_chunked_array()
            ? arrow::Concatenate(up.chunked_array()->chunks()).ValueOrDie()
            : up.make_array());
    assert(up_arr->GetString(0) == "HELLO");
    assert(up_arr->GetString(1) == "WORLD");
    assert(up_arr->IsNull(3));
    std::cout << "[PASS] col(s).to_upper()\n";
}

static void test_alias(const std::shared_ptr<arrow::Table>& t) {
    // alias is transparent at eval time — just evaluates inner expr
    auto result = evaluate(col("x").alias("renamed"), t);
    auto arr = as_int32(result);
    assert(arr->Value(0) == 1);
    assert(arr->Value(1) == 2);
    assert(arr->Value(2) == 3);
    assert(arr->IsNull(3));
    std::cout << "[PASS] col(x).alias(\"renamed\") evaluates correctly\n";
}

static void test_type_error(const std::shared_ptr<arrow::Table>& t) {
    // string + int should throw
    bool threw = false;
    try {
        evaluate(col("s") + col("x"), t);
    } catch (const std::runtime_error& e) {
        threw = true;
    }
    assert(threw);
    std::cout << "[PASS] col(s) + col(x) throws type error\n";
}

// ── main ───────────────────────────────────────────────────────────────────

int main() {
    auto t = make_table();

    test_col_plus_lit(t);
    test_int_float_promotion(t);
    test_comparison(t);
    test_boolean_and(t);
    test_boolean_not(t);
    test_is_null(t);
    test_abs(t);
    test_modulo(t);
    test_aggregations(t);
    test_string_functions(t);
    test_alias(t);
    test_type_error(t);

    std::cout << "\nAll evaluator tests passed.\n";
    return 0;
}
