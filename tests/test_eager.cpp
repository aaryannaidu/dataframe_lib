#include "EagerDataFrame.hpp"

#include <arrow/api.h>
#include <arrow/array/concatenate.h>

#include <cassert>
#include <iostream>
#include <set>
#include <string>

using namespace dataframelib;

// Build a small table:
//   x: int32   [3, 1, 2, null]
//   y: float32 [1.5, 3.5, 2.5, 4.5]
//   s: utf8    ["cat", "apple", "bat", null]
static EagerDataFrame make_df() {
    arrow::Int32Builder xb;
    xb.Append(3); xb.Append(1); xb.Append(2); xb.AppendNull();
    std::shared_ptr<arrow::Array> x_arr; xb.Finish(&x_arr);

    arrow::FloatBuilder yb;
    yb.Append(1.5f); yb.Append(3.5f); yb.Append(2.5f); yb.Append(4.5f);
    std::shared_ptr<arrow::Array> y_arr; yb.Finish(&y_arr);

    arrow::StringBuilder sb;
    sb.Append("cat"); sb.Append("apple"); sb.Append("bat"); sb.AppendNull();
    std::shared_ptr<arrow::Array> s_arr; sb.Finish(&s_arr);

    auto schema = arrow::schema({
        arrow::field("x", arrow::int32()),
        arrow::field("y", arrow::float32()),
        arrow::field("s", arrow::utf8()),
    });
    return EagerDataFrame(arrow::Table::Make(schema, {x_arr, y_arr, s_arr}));
}

static void test_basic(const EagerDataFrame& df) {
    assert(df.num_rows() == 4);
    assert(df.num_cols() == 3);
    std::cout << "[PASS] num_rows=4, num_cols=3\n";
}

static void test_select_names(const EagerDataFrame& df) {
    auto df2 = df.select(std::vector<std::string>{"x", "s"});
    assert(df2.num_cols() == 2);
    assert(df2.num_rows() == 4);
    assert(df2.table()->schema()->GetFieldIndex("x") == 0);
    assert(df2.table()->schema()->GetFieldIndex("s") == 1);
    std::cout << "[PASS] select by name keeps correct columns\n";
}

static void test_select_exprs(const EagerDataFrame& df) {
    // col("x") kept as-is, col("x")+lit(10) aliased
    auto df2 = df.select({col("x"), (col("x") + lit(int32_t{10})).alias("x_plus_10")});
    assert(df2.num_cols() == 2);
    assert(df2.table()->schema()->GetFieldIndex("x_plus_10") == 1);

    auto col_arr = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(df2.table()->GetColumnByName("x_plus_10")->chunks())
            .ValueOrDie());
    assert(col_arr->Value(0) == 13);
    assert(col_arr->Value(1) == 11);
    assert(col_arr->Value(2) == 12);
    assert(col_arr->IsNull(3));
    std::cout << "[PASS] select with expression + alias\n";
}

static void test_head(const EagerDataFrame& df) {
    auto df2 = df.head(2);
    assert(df2.num_rows() == 2);
    assert(df2.num_cols() == 3);
    std::cout << "[PASS] head(2) returns 2 rows\n";
}

static void test_sort(const EagerDataFrame& df) {
    // sort by x ascending: [null, 1, 2, 3]  (nulls sort last in Arrow default)
    auto df2 = df.sort({"x"}, {true});
    assert(df2.num_rows() == 4);
    auto x = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(df2.table()->GetColumnByName("x")->chunks()).ValueOrDie());
    assert(x->Value(0) == 1);
    assert(x->Value(1) == 2);
    assert(x->Value(2) == 3);
    std::cout << "[PASS] sort ascending: [1, 2, 3, null]\n";

    // sort by x descending: [3, 2, 1, null]
    auto df3 = df.sort({"x"}, {false});
    auto x2 = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(df3.table()->GetColumnByName("x")->chunks()).ValueOrDie());
    assert(x2->Value(0) == 3);
    assert(x2->Value(1) == 2);
    assert(x2->Value(2) == 1);
    std::cout << "[PASS] sort descending: [3, 2, 1, null]\n";
}

static void test_filter(const EagerDataFrame& df) {
    // keep rows where x > 1: rows with x=3 and x=2
    auto df2 = df.filter(col("x") > lit(int32_t{1}));
    assert(df2.num_rows() == 2);
    auto x = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(df2.table()->GetColumnByName("x")->chunks()).ValueOrDie());
    assert(x->Value(0) == 3);
    assert(x->Value(1) == 2);
    std::cout << "[PASS] filter(x > 1) keeps 2 rows\n";
}

static void test_select_missing_column(const EagerDataFrame& df) {
    bool threw = false;
    try { df.select(std::vector<std::string>{"nope"}); }
    catch (const std::runtime_error&) { threw = true; }
    assert(threw);
    std::cout << "[PASS] select missing column throws\n";
}

static void test_select_expr_no_alias(const EagerDataFrame& df) {
    bool threw = false;
    try { df.select({col("x") + lit(int32_t{1})}); }
    catch (const std::runtime_error&) { threw = true; }
    assert(threw);
    std::cout << "[PASS] select expr without alias throws\n";
}

static void test_with_column(const EagerDataFrame& df) {
    // Add a new column: x*2
    auto df2 = df.with_column("x2", (col("x") * lit(int32_t{2})).alias("x2"));
    assert(df2.num_cols() == 4);
    auto x2 = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(df2.table()->GetColumnByName("x2")->chunks()).ValueOrDie());
    assert(x2->Value(0) == 6);
    assert(x2->Value(1) == 2);
    assert(x2->IsNull(3));
    std::cout << "[PASS] with_column adds new column\n";

    // Replace an existing column: x = x+1
    auto df3 = df.with_column("x", (col("x") + lit(int32_t{1})).alias("x"));
    assert(df3.num_cols() == 3);
    auto x3 = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(df3.table()->GetColumnByName("x")->chunks()).ValueOrDie());
    assert(x3->Value(0) == 4);
    assert(x3->Value(1) == 2);
    std::cout << "[PASS] with_column replaces existing column\n";
}

static void test_group_by_aggregate(const EagerDataFrame& df) {
    // Table: x=[3,1,2,null], y=[1.5,3.5,2.5,4.5], s=[cat,apple,bat,null]
    // group by s (null forms its own group), sum x

    // Build a simple table with a clear grouping key:
    //   g: [A, A, B, B]
    //   v: [1, 3, 2, 4]
    arrow::StringBuilder gb;
    gb.Append("A"); gb.Append("A"); gb.Append("B"); gb.Append("B");
    std::shared_ptr<arrow::Array> g_arr; gb.Finish(&g_arr);

    arrow::Int32Builder vb;
    vb.Append(1); vb.Append(3); vb.Append(2); vb.Append(4);
    std::shared_ptr<arrow::Array> v_arr; vb.Finish(&v_arr);

    auto schema = arrow::schema({arrow::field("g", arrow::utf8()),
                                  arrow::field("v", arrow::int32())});
    EagerDataFrame gdf(arrow::Table::Make(schema, {g_arr, v_arr}));

    auto result = gdf.group_by({"g"}).aggregate({
        {"total", col("v").sum()},
        {"cnt",   col("v").count()},
    });

    // result rows are sorted by g: A, B
    assert(result.num_rows() == 2);
    assert(result.num_cols() == 3); // g, cnt, total (map order: cnt < total alphabetically)

    // Find the A row by checking the g column
    auto g_col = std::static_pointer_cast<arrow::StringArray>(
        arrow::Concatenate(result.table()->GetColumnByName("g")->chunks()).ValueOrDie());
    int a_row = (g_col->GetString(0) == "A") ? 0 : 1;
    int b_row = 1 - a_row;

    auto total_col = std::static_pointer_cast<arrow::Int64Array>(
        arrow::Concatenate(result.table()->GetColumnByName("total")->chunks()).ValueOrDie());
    assert(total_col->Value(a_row) == 4); // 1+3
    assert(total_col->Value(b_row) == 6); // 2+4
    std::cout << "[PASS] group_by + sum\n";

    auto cnt_col = std::static_pointer_cast<arrow::Int64Array>(
        arrow::Concatenate(result.table()->GetColumnByName("cnt")->chunks()).ValueOrDie());
    assert(cnt_col->Value(a_row) == 2);
    assert(cnt_col->Value(b_row) == 2);
    std::cout << "[PASS] group_by + count\n";
}

static void test_join_inner() {
    // Left:  id=[1,2,3], val=["a","b","c"]
    // Right: id=[2,3,4], score=[20,30,40]
    // Inner join on id → rows id=2 and id=3
    arrow::Int32Builder lb, rb;
    lb.Append(1); lb.Append(2); lb.Append(3);
    rb.Append(2); rb.Append(3); rb.Append(4);
    std::shared_ptr<arrow::Array> lid, rid;
    lb.Finish(&lid); rb.Finish(&rid);

    arrow::StringBuilder svb;
    svb.Append("a"); svb.Append("b"); svb.Append("c");
    std::shared_ptr<arrow::Array> val_arr; svb.Finish(&val_arr);

    arrow::Int32Builder scb;
    scb.Append(20); scb.Append(30); scb.Append(40);
    std::shared_ptr<arrow::Array> score_arr; scb.Finish(&score_arr);

    EagerDataFrame left(arrow::Table::Make(
        arrow::schema({arrow::field("id", arrow::int32()),
                        arrow::field("val", arrow::utf8())}),
        {lid, val_arr}));
    EagerDataFrame right(arrow::Table::Make(
        arrow::schema({arrow::field("id", arrow::int32()),
                        arrow::field("score", arrow::int32())}),
        {rid, score_arr}));

    auto joined = left.join(right, {"id"}, "inner");
    assert(joined.num_rows() == 2);
    assert(joined.num_cols() == 3); // id, val, score (id not duplicated)
    assert(joined.table()->schema()->GetFieldIndex("score") >= 0);

    auto id_col = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(joined.table()->GetColumnByName("id")->chunks()).ValueOrDie());
    // rows should be id=2 and id=3
    std::set<int32_t> ids{id_col->Value(0), id_col->Value(1)};
    std::set<int32_t> expected_ids{2, 3};
    assert(ids == expected_ids);
    std::cout << "[PASS] inner join returns correct rows and deduplicates key column\n";
}

static void test_join_left() {
    // Left: id=[1,2], name=["x","y"]
    // Right: id=[2], score=[99]
    // Left join → id=1 has null score
    arrow::Int32Builder lb, rb;
    lb.Append(1); lb.Append(2);
    rb.Append(2);
    std::shared_ptr<arrow::Array> lid, rid;
    lb.Finish(&lid); rb.Finish(&rid);

    arrow::StringBuilder nb;
    nb.Append("x"); nb.Append("y");
    std::shared_ptr<arrow::Array> name_arr; nb.Finish(&name_arr);

    arrow::Int32Builder sb;
    sb.Append(99);
    std::shared_ptr<arrow::Array> score_arr; sb.Finish(&score_arr);

    EagerDataFrame left(arrow::Table::Make(
        arrow::schema({arrow::field("id", arrow::int32()),
                        arrow::field("name", arrow::utf8())}),
        {lid, name_arr}));
    EagerDataFrame right(arrow::Table::Make(
        arrow::schema({arrow::field("id", arrow::int32()),
                        arrow::field("score", arrow::int32())}),
        {rid, score_arr}));

    auto joined = left.join(right, {"id"}, "left");
    assert(joined.num_rows() == 2);
    auto score_col = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(joined.table()->GetColumnByName("score")->chunks()).ValueOrDie());
    auto id_col = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(joined.table()->GetColumnByName("id")->chunks()).ValueOrDie());
    // find which row has id=1
    int row1 = (id_col->Value(0) == 1) ? 0 : 1;
    assert(score_col->IsNull(row1));
    std::cout << "[PASS] left join produces null for unmatched left rows\n";
}

int main() {
    auto df = make_df();

    test_basic(df);
    test_select_names(df);
    test_select_exprs(df);
    test_head(df);
    test_sort(df);
    test_filter(df);
    test_select_missing_column(df);
    test_select_expr_no_alias(df);
    test_with_column(df);
    test_group_by_aggregate(df);
    test_join_inner();
    test_join_left();

    std::cout << "\nAll EagerDataFrame tests passed.\n";
    return 0;
}
