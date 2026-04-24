#include "LazyDataFrame.hpp"

#include <arrow/api.h>
#include <arrow/array/concatenate.h>

#include <cassert>
#include <cstdio>
#include <iostream>
#include <string>

using namespace dataframelib;

// Paths for temp files created and cleaned up within this test.
static const char* CSV_PATH  = "/tmp/dfl_test.csv";
static const char* PQTPATH   = "/tmp/dfl_test.parquet";
static const char* SINK_PATH = "/tmp/dfl_sink_out.csv";

// Write a small CSV for scan_csv tests:
//   x,y,g
//   3,1.5,A
//   1,3.5,A
//   2,2.5,B
//   4,4.5,B
static void write_test_csv() {
    FILE* f = std::fopen(CSV_PATH, "w");
    std::fputs("x,y,g\n3,1.5,A\n1,3.5,A\n2,2.5,B\n4,4.5,B\n", f);
    std::fclose(f);
}

static std::shared_ptr<arrow::Int32Array> get_int32(
    const EagerDataFrame& df, const std::string& col_name) {
    return std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(df.table()->GetColumnByName(col_name)->chunks()).ValueOrDie());
}

static std::shared_ptr<arrow::StringArray> get_str(
    const EagerDataFrame& df, const std::string& col_name) {
    return std::static_pointer_cast<arrow::StringArray>(
        arrow::Concatenate(df.table()->GetColumnByName(col_name)->chunks()).ValueOrDie());
}

static void test_scan_filter_collect() {
    // Keep rows where x > 1: rows with x=3, x=2, x=4
    auto result = scan_csv(CSV_PATH)
                      .filter(col("x") > lit(int32_t{1}))
                      .collect();
    assert(result.num_rows() == 3);
    std::cout << "[PASS] scan_csv + filter + collect\n";
}

static void test_scan_select_collect() {
    // Select only x and g
    auto result = scan_csv(CSV_PATH)
                      .select(std::vector<std::string>{"x", "g"})
                      .collect();
    assert(result.num_cols() == 2);
    assert(result.table()->schema()->GetFieldIndex("x") >= 0);
    assert(result.table()->schema()->GetFieldIndex("g") >= 0);
    assert(result.table()->schema()->GetFieldIndex("y") < 0);
    std::cout << "[PASS] scan_csv + select(names) + collect\n";
}

static void test_chain_matches_eager() {
    // Lazy: filter x > 1, select x and g, sort by x asc
    auto lazy_result = scan_csv(CSV_PATH)
                           .filter(col("x") > lit(int32_t{1}))
                           .select(std::vector<std::string>{"x", "g"})
                           .sort({"x"}, {true})
                           .collect();

    // Eager equivalent
    auto eager_result = read_csv(CSV_PATH)
                            .filter(col("x") > lit(int32_t{1}))
                            .select(std::vector<std::string>{"x", "g"})
                            .sort({"x"}, {true});

    assert(lazy_result.num_rows() == eager_result.num_rows());
    assert(lazy_result.num_cols() == eager_result.num_cols());

    auto lx = get_int32(lazy_result, "x");
    auto ex = get_int32(eager_result, "x");
    for (int64_t i = 0; i < lx->length(); ++i)
        assert(lx->Value(i) == ex->Value(i));

    std::cout << "[PASS] lazy chain matches eager equivalent\n";
}

static void test_with_column_lazy() {
    auto result = scan_csv(CSV_PATH)
                      .with_column("x2", (col("x") * lit(int32_t{2})).alias("x2"))
                      .select(std::vector<std::string>{"x", "x2"})
                      .collect();
    assert(result.num_cols() == 2);
    auto x  = get_int32(result, "x");
    auto x2 = get_int32(result, "x2");
    for (int64_t i = 0; i < x->length(); ++i)
        assert(x2->Value(i) == x->Value(i) * 2);
    std::cout << "[PASS] lazy with_column\n";
}

static void test_head_lazy() {
    auto result = scan_csv(CSV_PATH).head(2).collect();
    assert(result.num_rows() == 2);
    std::cout << "[PASS] lazy head(2)\n";
}

static void test_group_by_aggregate_lazy() {
    // group by g, sum x: A→4 (3+1), B→6 (2+4)
    auto result = scan_csv(CSV_PATH)
                      .group_by({"g"})
                      .aggregate({{"total", col("x").sum()}})
                      .sort({"g"}, {true})
                      .collect();

    assert(result.num_rows() == 2);
    auto g     = get_str(result, "g");
    auto total = std::static_pointer_cast<arrow::Int64Array>(
        arrow::Concatenate(result.table()->GetColumnByName("total")->chunks()).ValueOrDie());

    assert(g->GetString(0) == "A");
    assert(total->Value(0) == 4);
    assert(g->GetString(1) == "B");
    assert(total->Value(1) == 6);
    std::cout << "[PASS] lazy group_by + aggregate\n";
}

static void test_join_lazy() {
    // Left: scan CSV (has x, y, g)
    // Right: a tiny in-memory table written to parquet first
    // We'll use scan_csv twice on the same file and join on g
    // Left side: filter g=A; Right: no filter — join on g inner
    // Both sides from same CSV, inner join on g should give 4 rows (A×A = 2×2)

    auto left  = scan_csv(CSV_PATH).filter(col("g") == lit(std::string{"A"}));
    auto right = scan_csv(CSV_PATH)
                     .select({col("g"), (col("x") * lit(int32_t{10})).alias("rx")});

    auto result = left.join(right, {"g"}, "inner").collect();
    // left A rows: [3, 1]; right all rows matched on g value
    // inner join on g: A(left)×A(right) = 2×2 = 4 rows
    assert(result.num_rows() == 4);
    assert(result.table()->schema()->GetFieldIndex("rx") >= 0);
    std::cout << "[PASS] lazy join\n";
}

static void test_sink_csv() {
    scan_csv(CSV_PATH)
        .filter(col("x") > lit(int32_t{1}))
        .sink_csv(SINK_PATH);

    // Read it back and verify row count
    auto result = read_csv(SINK_PATH);
    assert(result.num_rows() == 3);
    std::cout << "[PASS] sink_csv writes filtered rows\n";
}

static void test_scan_parquet() {
    // Write CSV as Parquet, then scan_parquet and verify
    read_csv(CSV_PATH).write_parquet(PQTPATH);

    auto result = scan_parquet(PQTPATH)
                      .filter(col("x") > lit(int32_t{2}))
                      .collect();
    assert(result.num_rows() == 2); // x=3, x=4
    std::cout << "[PASS] scan_parquet + filter + collect\n";
}

static void test_explain() {
    auto left  = scan_csv(CSV_PATH).filter(col("g") == lit(std::string{"A"}));
    auto right = scan_csv(CSV_PATH)
                     .select({col("g"), (col("x") * lit(int32_t{10})).alias("rx")});

    auto query = left.join(right, {"g"}, "inner")
                     .group_by({"g"})
                     .aggregate({{"sum_x", col("x").sum()}});

    query.explain("test_explain.png");
    std::cout << "[PASS] explain generates PNG\n";
}

int main() {
    write_test_csv();

    test_scan_filter_collect();
    test_scan_select_collect();
    test_chain_matches_eager();
    test_with_column_lazy();
    test_head_lazy();
    test_group_by_aggregate_lazy();
    test_join_lazy();
    test_sink_csv();
    test_scan_parquet();
    test_explain();

    std::remove(CSV_PATH);
    std::remove(PQTPATH);
    std::remove(SINK_PATH);

    std::cout << "\nAll LazyDataFrame tests passed.\n";
}
