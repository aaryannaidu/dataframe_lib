# DataFrameLib

A C++17 DataFrame library built on [Apache Arrow](https://arrow.apache.org/). It offers both an eager API (operations execute immediately) and a lazy API (operations build a query plan that is optimized and executed only on `.collect()`).

---

## Dependencies

| Library | Install |
|---|---|
| Apache Arrow + Compute | `brew install apache-arrow` |
| Parquet (bundled with Arrow) | included above |
| Graphviz (for `explain()`) | `brew install graphviz` |
| CMake ≥ 3.14 | `brew install cmake` |

---

## Build

```bash
mkdir build && cd build
cmake ..
cmake --build . --parallel
```

This produces:
- `libdataframelib.a` — the library
- `dataframe_test` — visualization demo (`main.cpp`)
- `dfl_test_expressions`, `dfl_test_eager`, `dfl_test_lazy`, `dfl_test_optimizer` — unit tests

---

## Quick Start

Include the single public header:

```cpp
#include <dataframelib/dataframelib.h>
using namespace dataframelib;
```

### Eager API

Operations run immediately and return a new `EagerDataFrame`.

```cpp
// Read
auto df = read_csv("data.csv");

// Select, filter, sort
auto result = df.select({"name", "salary", "department"})
                .filter(col("salary") > 50000)
                .sort({"salary"}, false);   // false = descending

// Add / replace a column
auto df2 = df.with_column("bonus", col("salary") * 0.1);

// Group-by and aggregate
auto summary = df.group_by({"department"})
                 .aggregate({{"salary", "mean"}, {"salary", "count"}});
// Output columns: department, salary_mean, salary_count

// Join
auto joined = df.join(other, {"id"}, "inner");   // inner | left | right | outer

// Write
result.write_csv("out.csv");
result.write_parquet("out.parquet");
```

### Lazy API

Operations build a query plan. Call `.collect()` to execute.

```cpp
auto ldf = scan_csv("data.csv")
               .filter(col("department") == "Engineering")
               .select({"name", "salary"})
               .sort({"salary"}, false)
               .head(10);

EagerDataFrame top10 = ldf.collect();

// Visualise the query plan (before + after optimization) as a PNG
ldf.explain("plan.png");

// Sink directly to file without materializing
ldf.sink_csv("top10.csv");
```

### Expressions

```cpp
col("x") + col("y")          // arithmetic: + - * / %
col("age") >= 18              // comparison: == != < <= > >=
col("active") & ~col("banned")// boolean: & | ~
col("name").to_upper()        // strings: to_lower/upper, length, contains, starts_with, ends_with
col("salary").is_null()       // null checks: is_null, is_not_null
col("x").abs()
(col("x") * 2).alias("double_x")  // alias for computed columns in select()
```

### Build from arrays

```cpp
auto id_arr   = /* arrow::Int32Array */;
auto name_arr = /* arrow::StringArray */;

auto df = from_columns({{"id", id_arr}, {"name", name_arr}});
```

---

## Project Structure

```
include/
  dataframelib/dataframelib.h   public entry-point header
  Expression.hpp                expression AST and operator overloads
  EagerDataFrame.hpp            eager execution API
  LazyDataFrame.hpp             lazy / query-plan API
  DAGNode.hpp                   query plan node types
  QueryOptimizer.hpp            optimizer interface
  IO.hpp                        internal I/O helpers

src/
  Expression.cpp                expression evaluator (Arrow compute kernels)
  EagerDataFrame.cpp            eager operations (filter, join, group-by …)
  LazyDataFrame.cpp             lazy plan builder + DAG executor
  DAGNode.cpp                   node ID management
  QueryOptimizer.cpp            query optimizer passes
  IO.cpp                        CSV / Parquet read & write

tests/
  test_expressions.cpp
  test_eager.cpp
  test_lazy.cpp
  test_optimizer.cpp
```

---

## Query Optimizer

The lazy API automatically applies the following optimizations before execution:

| Pass | What it does |
|---|---|
| **Projection pushdown** | Inserts early `SELECT` nodes near scans to drop unused columns as soon as possible |
| **Projection merging** | Collapses consecutive redundant `SELECT` nodes into one |
| **Predicate pushdown** | Moves `FILTER` below `SORT` and below `SELECT` (when safe) to reduce rows early |
| **Limit pushdown** | Moves `HEAD` below `SELECT` so fewer rows are projected |
| **Constant folding** | Evaluates literal arithmetic at plan time (`2 * 3` → `6`) |
| **Expression simplification** | Rewrites `x + 0` → `x`, `x * 1` → `x`, `x * 0` → `0`, `x - x` → `0` |

Call `.explain("plan.png")` on any `LazyDataFrame` to produce a side-by-side PNG of the unoptimized and optimized query plans.

---

## Running Tests

```bash
cd build
./dfl_test_expressions
./dfl_test_eager
./dfl_test_lazy
./dfl_test_optimizer
```
