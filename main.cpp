#include "dataframelib/dataframelib.h"

#include <cstdio>
#include <iostream>
#include <stdexcept>

using namespace dataframelib;

static const char* CSV = "/tmp/dfl_test.csv";

static void write_test_csv() {
    FILE* f = std::fopen(CSV, "w");
    std::fputs("id,x,g\n1,3,A\n2,1,A\n3,2,B\n4,4,B\n", f);
    std::fclose(f);
}

// Reproduces test_optimizer.png:
//   Before: SCAN → SELECT[g,x] → SORT[g] → FILTER((x+0)>(2*1))
//   After:  SCAN → FILTER(x>2) → SELECT[g,x] → SORT[g]
//           (constant folding + predicate pushdown + projection merge)
static void gen_optimizer_png() {
    auto ldf = scan_csv(CSV)
                   .select(std::vector<std::string>{"g", "x"})
                   .sort({"g"}, std::vector<bool>{true})
                   .filter((col("x") + lit(int32_t{0})) > lit(int32_t{2}) * lit(int32_t{1}));

    ldf.explain("test_optimizer.png");
    std::cout << "[OK] test_optimizer.png\n";
}

// Reproduces test_explain.png:
//   Before: left SCAN→SELECT[g,(x*10) AS rx] joined with right SCAN→FILTER(g=='A')
//           then GROUP_BY(g)→AGGREGATE(sum_x:SUM(x))
//   After:  projection pushdown + filter pushdown into join branch
static void gen_explain_png() {
    auto left  = scan_csv(CSV)
                     .select(std::vector<Expr>{col("g"), (col("x") * lit(int32_t{10})).alias("rx")});
    auto right = scan_csv(CSV)
                     .filter(col("g") == "A");

    auto ldf = left.join(right, {"g"}, "inner")
                   .group_by({"g"})
                   .aggregate({{"rx", "sum"}});

    ldf.explain("test_explain.png");
    std::cout << "[OK] test_explain.png\n";
}

int main() {
    write_test_csv();
    try {
        gen_optimizer_png();
        gen_explain_png();
    } catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << "\n";
        return 1;
    }
    std::cout << "Both PNGs regenerated.\n";
    return 0;
}
