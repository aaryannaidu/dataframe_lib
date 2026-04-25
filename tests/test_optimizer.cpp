#include "LazyDataFrame.hpp"
#include "QueryOptimizer.hpp"

#include <arrow/api.h>
#include <arrow/array/concatenate.h>

#include <cassert>
#include <cstdio>
#include <iostream>
#include <string>

using namespace dataframelib;

static const char* CSV_PATH = "/tmp/dfl_opt_test.csv";

static void write_test_csv() {
    FILE* f = std::fopen(CSV_PATH, "w");
    std::fputs("x,y,g\n3,1.5,A\n1,3.5,A\n2,2.5,B\n4,4.5,B\n", f);
    std::fclose(f);
}

static std::shared_ptr<arrow::Int32Array> get_int32(
    const EagerDataFrame& df, const std::string& name) {
    return std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(df.table()->GetColumnByName(name)->chunks()).ValueOrDie());
}

// Walk the DAG chain (via input()) and return the node at depth `depth` (0 = root).
static std::shared_ptr<DAGNode> chain_at(const std::shared_ptr<DAGNode>& root, int depth) {
    auto cur = root;
    for (int i = 0; i < depth && cur; ++i) cur = cur->input();
    return cur;
}

// Structure: verify constant folding folds lit(2)*lit(1) into a single LIT.
static void test_constant_folding_structure() {
    // FILTER(x > 2*1)(SCAN) — optimizer should fold 2*1 → 2
    auto dag = scan_csv(CSV_PATH)
                   .filter(col("x") > lit(int32_t{2}) * lit(int32_t{1}))
                   .node();
    auto opt = optimize(dag);

    // opt should still be FILTER at root
    assert(opt->type() == NodeType::FILTER);
    auto f = static_cast<const FilterNode*>(opt.get());
    // RHS of predicate (x > ...) should now be a literal
    auto binop = static_cast<const BinaryOpNode*>(f->predicate.node().get());
    assert(binop->right->type() == ExprType::LIT);
    auto lit_node = static_cast<const LitNode*>(binop->right.get());
    assert(std::get<int32_t>(lit_node->value) == 2);
    std::cout << "[PASS] constant folding: 2*1 -> 2\n";
}

// Structure: verify x+0 is simplified to x.
static void test_simplify_add_zero_structure() {
    auto dag = scan_csv(CSV_PATH)
                   .filter((col("x") + lit(int32_t{0})) > lit(int32_t{1}))
                   .node();
    auto opt = optimize(dag);

    assert(opt->type() == NodeType::FILTER);
    auto f = static_cast<const FilterNode*>(opt.get());
    auto binop = static_cast<const BinaryOpNode*>(f->predicate.node().get());
    // LHS should now be COL("x"), not a BINOP
    assert(binop->left->type() == ExprType::COL);
    std::cout << "[PASS] simplification: x+0 -> x\n";
}

// Structure: verify x*1 is simplified to x.
static void test_simplify_mul_one_structure() {
    auto dag = scan_csv(CSV_PATH)
                   .filter((col("x") * lit(int32_t{1})) > lit(int32_t{0}))
                   .node();
    auto opt = optimize(dag);

    assert(opt->type() == NodeType::FILTER);
    auto f = static_cast<const FilterNode*>(opt.get());
    auto binop = static_cast<const BinaryOpNode*>(f->predicate.node().get());
    assert(binop->left->type() == ExprType::COL);
    std::cout << "[PASS] simplification: x*1 -> x\n";
}

// Structure: FILTER over SORT should become SORT(FILTER(SCAN)) after pushdown.
static void test_predicate_pushdown_past_sort_structure() {
    auto dag = scan_csv(CSV_PATH)
                   .sort({"x"}, {true})
                   .filter(col("x") > lit(int32_t{1}))
                   .node();
    auto opt = optimize(dag);

    // root should be SORT, child should be FILTER
    assert(opt->type() == NodeType::SORT);
    assert(opt->input() != nullptr);
    // walk down past any injected SELECT nodes from projection pushdown
    auto cur = opt->input();
    while (cur && cur->type() == NodeType::SELECT) cur = cur->input();
    assert(cur && cur->type() == NodeType::FILTER);
    std::cout << "[PASS] predicate pushdown: FILTER past SORT\n";
}

// Structure: HEAD(SELECT(SCAN)) should become SELECT(HEAD(SCAN)) after limit pushdown.
static void test_limit_pushdown_structure() {
    auto dag = scan_csv(CSV_PATH)
                   .select(std::vector<std::string>{"x", "g"})
                   .head(2)
                   .node();
    auto opt = optimize(dag);

    // root should be SELECT, its input should be HEAD
    assert(opt->type() == NodeType::SELECT);
    assert(opt->input() != nullptr);
    assert(opt->input()->type() == NodeType::HEAD);
    std::cout << "[PASS] limit pushdown: HEAD past SELECT\n";
}

// Correctness: filter with folded constant should return same rows as direct filter.
static void test_correctness_constant_folding() {
    auto direct = scan_csv(CSV_PATH)
                      .filter(col("x") > lit(int32_t{3}))
                      .collect();
    auto folded = scan_csv(CSV_PATH)
                      .filter(col("x") > lit(int32_t{2}) + lit(int32_t{1}))
                      .collect();
    assert(direct.num_rows() == folded.num_rows());
    auto dx = get_int32(direct, "x");
    auto fx = get_int32(folded, "x");
    for (int64_t i = 0; i < dx->length(); ++i)
        assert(dx->Value(i) == fx->Value(i));
    std::cout << "[PASS] correctness: constant folding (2+1 == 3)\n";
}

// Correctness: x*1 simplification should return same rows as plain x filter.
static void test_correctness_simplification() {
    auto direct = scan_csv(CSV_PATH)
                      .filter(col("x") > lit(int32_t{2}))
                      .collect();
    auto simplified = scan_csv(CSV_PATH)
                          .filter((col("x") * lit(int32_t{1})) > lit(int32_t{2}))
                          .collect();
    assert(direct.num_rows() == simplified.num_rows());
    std::cout << "[PASS] correctness: simplification (x*1 -> x)\n";
}

// Correctness: predicate pushed past sort should produce same sorted results.
static void test_correctness_predicate_pushdown() {
    auto pushed = scan_csv(CSV_PATH)
                      .sort({"x"}, {true})
                      .filter(col("x") > lit(int32_t{1}))
                      .collect();
    auto direct = scan_csv(CSV_PATH)
                      .filter(col("x") > lit(int32_t{1}))
                      .sort({"x"}, {true})
                      .collect();
    assert(pushed.num_rows() == direct.num_rows());
    auto px = get_int32(pushed, "x");
    auto dx = get_int32(direct, "x");
    for (int64_t i = 0; i < px->length(); ++i)
        assert(px->Value(i) == dx->Value(i));
    std::cout << "[PASS] correctness: predicate pushdown past sort\n";
}

// Correctness: projection pushdown should not change results.
static void test_correctness_projection_pushdown() {
    auto lazy = scan_csv(CSV_PATH)
                    .select(std::vector<std::string>{"x", "g"})
                    .filter(col("x") > lit(int32_t{2}))
                    .collect();
    auto eager = read_csv(CSV_PATH)
                     .select(std::vector<std::string>{"x", "g"})
                     .filter(col("x") > lit(int32_t{2}));
    assert(lazy.num_rows() == eager.num_rows());
    assert(lazy.num_cols() == eager.num_cols());
    std::cout << "[PASS] correctness: projection pushdown\n";
}

int main() {
    write_test_csv();

    test_constant_folding_structure();
    test_simplify_add_zero_structure();
    test_simplify_mul_one_structure();
    test_predicate_pushdown_past_sort_structure();
    test_limit_pushdown_structure();
    test_correctness_constant_folding();
    test_correctness_simplification();
    test_correctness_predicate_pushdown();
    test_correctness_projection_pushdown();

    std::remove(CSV_PATH);
    std::cout << "\nAll QueryOptimizer tests passed.\n";
}
