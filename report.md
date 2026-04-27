# DataFrameLib — Technical Report

---

## 1. Overview

DataFrameLib is a C++17 DataFrame library that provides both an **eager API** (operations execute immediately) and a **lazy API** (operations build a query plan that is optimized and then executed on `.collect()`).

Apache Arrow is used **only for data storage and I/O** (reading/writing CSV and Parquet files). All computation — arithmetic, comparisons, aggregation, sorting, filtering, string operations — is implemented from scratch in pure C++. There is no dependency on the Arrow Compute module.

---

## 2. Architecture Overview

The library has four logical layers:

```
┌─────────────────────────────────────────────────────┐
│                   Public API                        │
│    EagerDataFrame   LazyDataFrame   free functions  │
├─────────────────────────────────────────────────────┤
│                Expression System                    │
│     AST nodes (ColNode, LitNode, BinaryOpNode …)    │
├─────────────────────────────────────────────────────┤
│                  Compute Layer                      │
│  NativeValue · apply_binop · apply_agg · sort …     │
├─────────────────────────────────────────────────────┤
│             Arrow Data Layer (I/O only)             │
│    arrow::Table · arrow::ChunkedArray · Scalar      │
│    CSV reader/writer · Parquet reader/writer        │
└─────────────────────────────────────────────────────┘
```

The **Lazy API** adds a fifth concern — the query plan layer:

```
LazyDataFrame  →  DAG of DAGNodes  →  QueryOptimizer  →  execute()
```

---

## 3. File Structure and Responsibilities

### Headers (`include/`)

| File | Purpose |
|---|---|
| `dataframelib/dataframelib.h` | Single public entry-point. Include this and nothing else. |
| `Expression.hpp` | Expression AST node types (`ColNode`, `LitNode`, `BinaryOpNode`, etc.), the `Expr` wrapper class with operator overloads, and `col()`/`lit()` helper functions. |
| `Compute.hpp` | Declares the `NativeValue` type and all compute function signatures (`apply_binop`, `apply_agg`, `sort_indices`, etc.). |
| `EagerDataFrame.hpp` | The `EagerDataFrame` class (wraps `arrow::Table`) and `GroupedDataFrame`. |
| `LazyDataFrame.hpp` | The `LazyDataFrame` class (wraps a DAG node pointer). |
| `DAGNode.hpp` | Enum `NodeType` and all DAG node types (`ScanNode`, `FilterNode`, `SelectNode`, etc.). |
| `QueryOptimizer.hpp` | Declares the public `optimize(root)` function. |
| `IO.hpp` | Internal I/O helpers for CSV and Parquet. |

### Sources (`src/`)

| File | Purpose |
|---|---|
| `Compute.cpp` | Implements all computation. Converts Arrow scalars to/from `NativeValue`, applies binary/unary ops row-by-row, aggregates columns, and provides `sort_indices`, `filter_indices`, `take_rows`. |
| `Expression.cpp` | Walks the expression AST and delegates each node type to the corresponding `compute::` function. Contains `evaluate(expr, table)`. |
| `EagerDataFrame.cpp` | Implements all `EagerDataFrame` methods. `filter` and `sort` call into `Compute`. `join` and `group_by/aggregate` are implemented directly using Arrow builders. |
| `LazyDataFrame.cpp` | Each method appends a new node to the DAG. `collect()` calls the optimizer then executes. `explain()` renders the before/after DAG as a PNG via Graphviz. |
| `DAGNode.cpp` | Global atomic counter for unique node IDs. |
| `QueryOptimizer.cpp` | Two-phase optimizer: projection pushdown (top-down), then fixed-point rule application (bottom-up) for predicate pushdown, limit pushdown, constant folding, and expression simplification. |
| `IO.cpp` | Arrow CSV and Parquet reader/writer wrappers. |

---

## 4. The Compute Layer

### 4.1 NativeValue

All computation is built around a single variant type:

```cpp
using NativeValue = std::variant<std::monostate, int64_t, double, std::string, bool>;
```

`std::monostate` represents SQL null. Every operation propagates nulls: if either input is null, the output is null (except `IS_NULL` and `IS_NOT_NULL`).

### 4.2 Conversion

`to_native(arrow::Scalar)` converts any Arrow scalar to `NativeValue`:
- All integer types (int8 through uint64) → `int64_t`
- float / double → `double`
- string / large_string → `std::string`
- bool → `bool`
- invalid (null) scalar → `std::monostate`

`from_native(NativeValue, target_type)` converts back, respecting the original Arrow type (e.g. an `int64_t` value goes back to `int32` if the column type is `int32`).

### 4.3 Element-wise Operations

`apply_binop`, `apply_unary`, `apply_strfunc` each:
1. Determine the output Arrow type from the input types.
2. Iterate row by row using `datum_at()`, which handles scalar broadcasting (a `LitNode` evaluates to a scalar Datum; the operation broadcasts it across all rows of the array operand).
3. Apply the operation on `NativeValue` variants.
4. Collect results and build a new `ChunkedArray` via an `ArrayBuilder`.

### 4.4 Aggregation

`apply_agg` iterates the column once:
- `COUNT` — counts non-null entries, returns `int64`.
- `SUM` — accumulates as `int64` for integer columns, promotes to `double` if any element is floating-point.
- `MEAN` — always returns `double`.
- `MIN` / `MAX` — tracks the running best value using native `<` / `>` on `NativeValue`.

### 4.5 Sort

`compute::sort_indices` builds an index vector `[0, 1, …, n-1]` and calls `std::stable_sort` with a comparator that:
- Fetches `NativeValue` for each sort key at positions `a` and `b`.
- Promotes `int64`/`double` mixes to `double` before comparing.
- Places nulls last in both ascending and descending order.
- Breaks ties by moving to the next sort key.

`compute::take_rows` then rebuilds every column by reading rows in the sorted index order.

### 4.6 Filter

`compute::filter_indices` iterates a boolean `ChunkedArray` and collects the indices where the value is `true`. `EagerDataFrame::filter` calls `evaluate(predicate, table_)` to produce the mask, then calls `take_rows` with those indices.

---

## 5. Expression System

### 5.1 AST Nodes

Every expression is a tree of `ExprNode` subclasses:

| Node | Fields | Meaning |
|---|---|---|
| `ColNode` | `name` | Reference to a column by name |
| `LitNode` | `value` (variant) | Literal constant |
| `AliasNode` | `input`, `name` | Rename a computed column |
| `BinaryOpNode` | `left`, `right`, `op` | Arithmetic, comparison, or boolean |
| `UnaryOpNode` | `input`, `op` | abs, negate, NOT, IS_NULL, IS_NOT_NULL |
| `AggNode` | `input`, `agg` | SUM, MEAN, COUNT, MIN, MAX |
| `StrFuncNode` | `input`, `func`, `arg` | String operations |

### 5.2 Expr Wrapper

Users interact only with the `Expr` class, not the AST nodes directly. `Expr` overloads all standard operators:

```cpp
col("salary") > lit(50000)          // BinaryOpNode(GT)
col("x") * lit(2.0) + col("y")      // nested BinaryOpNodes
col("name").to_upper().contains("A")// StrFuncNode chain
~col("active")                      // UnaryOpNode(NOT)
```

Numeric literals convert implicitly (`col("x") + 1` works), but string literals require `lit(std::string("val"))` or comparison operators (`col("g") == "A"`).

### 5.3 Evaluation

`evaluate(expr, table)` in `Expression.cpp` walks the AST recursively and returns an `arrow::Datum`:
- `ColNode` → returns the column's `ChunkedArray` as a Datum.
- `LitNode` → returns an Arrow `Scalar` Datum (broadcasts during binary operations).
- All other nodes → delegate to `compute::apply_binop / apply_unary / apply_agg / apply_strfunc`.

---

## 6. EagerDataFrame API

Every method executes immediately and returns a new `EagerDataFrame`. The internal state is an immutable `shared_ptr<arrow::Table>`.

```cpp
// I/O
auto df = read_csv("data.csv");
auto df = read_parquet("data.parquet");
df.write_csv("out.csv");
df.write_parquet("out.parquet");

// Construction from arrays
auto df = from_columns({{"id", id_array}, {"name", name_array}});

// Projection
df.select({"col_a", "col_b"})             // by name
df.select({col("x"), (col("x")*2).alias("x2")})  // by expression

// Filtering
df.filter(col("age") >= 18)

// Sorting
df.sort({"salary", "name"}, {false, true})  // descending salary, ascending name

// Limiting
df.head(10)

// Adding/replacing a column
df.with_column("bonus", col("salary") * 0.1)

// Aggregation
df.group_by({"department"})
  .aggregate({{"salary", "mean"}, {"salary", "count"}})
// produces columns: department, salary_mean, salary_count

// Join (inner | left | right | outer)
df.join(other_df, {"id"}, "inner")
```

### Join Semantics

The join is implemented as a hash join:
1. Build a hash index on the right table keyed by join columns using a length-prefixed encoding to avoid separator collisions.
2. Probe the left table against the index.
3. For outer/right joins, key columns are coalesced: if only the right row is present, the right key value is used (not null).

---

## 7. LazyDataFrame API

Calling any operation on a `LazyDataFrame` creates a new `DAGNode` and returns a new `LazyDataFrame` pointing to it. Nothing executes until `.collect()`.

```cpp
auto ldf = scan_csv("data.csv")
               .filter(col("department") == "Engineering")
               .select({"name", "salary"})
               .sort({"salary"}, {false})
               .head(10);

EagerDataFrame result = ldf.collect();    // optimize → execute
ldf.sink_csv("out.csv");                  // collect + write
ldf.explain("plan.png");                  // visualize before/after optimization
```

`scan_parquet` is the Parquet equivalent of `scan_csv`.

### DAG Execution

`collect()` calls `optimize(root_node)`, then walks the optimized DAG in post-order (leaves first), executing each node by calling the corresponding `EagerDataFrame` method on its child's result.

### explain()

`explain(path)` captures the unoptimized DAG, runs the optimizer, generates a Graphviz `.dot` file with two subgraphs (`cluster_before`, `cluster_after`) side by side, and calls `dot -Tpng` to produce a PNG. Run `./dataframe_test` in the build directory to generate example PNGs:

- `build/test_optimizer.png` — predicate pushdown + constant folding example
- `build/test_explain.png` — join + group-by pipeline example

---

## 8. Query Optimizer

The optimizer runs in two phases inside `optimize(root)`:

1. **Projection pushdown** — one top-down pass (`pushdown_projections`).
2. **Fixed-point rule application** — bottom-up passes (`apply_rules`) repeated until no rule fires.

---

### 8.1 Predicate Pushdown

**Transformation.** A `FILTER` node above a `SORT` or `SELECT` is moved below it.

- *FILTER above SORT:* `SORT(FILTER(input, p))` — the sort result is identical but the sort operates on fewer rows.
- *FILTER above SELECT:* `SELECT(FILTER(input, p))` — valid only when every column referenced in `p` is a plain `col()` reference in the SELECT (not a computed expression).

**Correctness.** `SORT` and `SELECT` (of plain columns) are order-preserving and column-preserving with respect to the filter predicate. Filtering before or after sorting yields the same set of rows (sort is a permutation, filter is a mask — they commute). For SELECT, since the predicate columns pass through unchanged, the filter result is identical whether applied before or after projection.

**Example.**

```
Before:  SCAN → SORT[salary] → FILTER(salary > 50000)
After:   SCAN → FILTER(salary > 50000) → SORT[salary]
```

If the table has 1,000,000 rows and only 10,000 satisfy the predicate, sorting 10,000 rows instead of 1,000,000 reduces sort time from O(N log N) to O(n log n) where n ≪ N.

---

### 8.2 Projection Pushdown

**Transformation.** A top-down pass computes the set of columns actually needed at each point in the plan, then inserts a `SELECT` node immediately after each `SCAN` to drop unused columns as early as possible.

**Correctness.** Columns not referenced by any downstream operator do not affect the result. Dropping them at the scan simply means reading and storing less data. The column set propagated downward always includes every column referenced by filters, projections, join keys, sort keys, and aggregation expressions above.

**Example.**

```
Table has columns: id, name, salary, department, hire_date

Query:  SCAN → GROUP_BY(department) → AGG(salary: mean)
After pushdown:
        SCAN → SELECT[department, salary] → GROUP_BY(department) → AGG(salary: mean)
```

Reading only `department` and `salary` instead of all five columns reduces memory usage and speeds up every downstream operation.

---

### 8.3 Projection Merging

**Transformation.** When a pure column-reference `SELECT` sits above another `SELECT` (possibly with `SORT` or `FILTER` in between), and the inner SELECT already produces all the columns the outer needs, the outer `SELECT` is redundant and is removed.

**Correctness.** A pure-column SELECT is a subset operation. If the inner SELECT already emits exactly the needed columns, applying the outer SELECT again is a no-op. `SORT` and `FILTER` between them do not add or remove columns, so the coverage check remains valid.

**Example.**

```
Before:  SCAN → SELECT[g, x] → SORT[g] → SELECT[g, x]
After:   SCAN → SELECT[g, x] → SORT[g]
```

Eliminates a full pass over the table that would copy every row into a new Arrow table unnecessarily.

---

### 8.4 Limit Pushdown

**Transformation.** A `HEAD(n)` node above a `SELECT` is moved below it: `SELECT(HEAD(n, input))`.

**Correctness.** `HEAD(n)` keeps the first `n` rows. `SELECT` reorders or renames columns but does not change the row order. Applying `HEAD` before `SELECT` yields the same `n` rows, projected. The final result is identical.

**Example.**

```
Before:  SCAN → SELECT[name, salary] → HEAD(10)
After:   SCAN → HEAD(10) → SELECT[name, salary]
```

`SELECT` is now applied to only 10 rows instead of the full table. For a large table this eliminates almost the entire projection cost.

---

### 8.5 Constant Folding

**Transformation.** A `BinaryOpNode` whose both children are `LitNode` is evaluated at plan construction time and replaced with a single `LitNode` holding the result.

Currently folds: `+`, `-`, `*`, `/`, `%` on `int32` literal pairs.

**Correctness.** Both operands are compile-time constants. Evaluating `v1 op v2` at plan time yields the same result as evaluating it per-row at runtime. Division by zero is guarded and left unevaluated.

**Example.**

```
Expression:  col("x") > lit(2) * lit(1)
After fold:  col("x") > lit(2)
```

Avoids multiplying two constants once per row across every row of the table. For a query that runs repeatedly, the fold happens once at planning time.

---

### 8.6 Expression Simplification

**Transformation.** Algebraic identities are applied to expression trees to remove redundant operations:

| Pattern | Simplified to | Identity |
|---|---|---|
| `x * lit(1)` | `x` | multiplicative identity |
| `lit(1) * x` | `x` | commutative form |
| `x + lit(0)` | `x` | additive identity |
| `lit(0) + x` | `x` | commutative form |
| `x * lit(0)` | `lit(0)` | zero product |
| `x - x` | `lit(0)` | self-subtraction (same column name) |
| `x - lit(0)` | `x` | subtract zero |

**Correctness.** Each rule is a standard algebraic identity that holds for all numeric values. The `x - x` rule applies only when both operands are `ColNode` with the same column name, ensuring the same value is subtracted from itself.

**Example.**

```
Expression:  (col("x") + lit(0)) > lit(2) * lit(1)
After:       col("x") > lit(2)          (simplification + constant fold)
```

Removes two redundant arithmetic operations per row, and opens the door for further constant folding.

---

### Optimizer Pass Order

```
optimize(root)
  1. pushdown_projections(root)    ← top-down, single pass
  2. while changed:
       apply_rules(curr)           ← bottom-up, fixed-point
         • expression simplification  (inside every node's expression)
         • constant folding           (inside every node's expression)
         • predicate pushdown         (FILTER over SORT or SELECT)
         • projection merging         (redundant SELECT removal)
         • limit pushdown             (HEAD over SELECT)
```

Projection pushdown runs first because it inserts new `SELECT` nodes, which the subsequent fixed-point rules can then simplify or merge. The fixed-point loop ensures that one rule firing can enable another (e.g., constant folding enables simplification on the resulting literal).

---

## 9. Design Decisions

**Arrow for data storage only.** Using Arrow's `Table`/`ChunkedArray`/`Scalar` types as the data representation gives efficient columnar storage, zero-copy slicing, and correct handling of nulls via the validity bitmap, without coupling the library to Arrow's compute module. This makes the compute logic fully transparent and self-contained.

**NativeValue variant.** Representing a cell value as `std::variant<std::monostate, int64_t, double, std::string, bool>` gives a uniform interface for all operations. All integer types are widened to `int64_t` on read; this trades some memory for simplicity and avoids a combinatorial explosion of type-pair dispatch.

**Immutable DataFrames.** Every `EagerDataFrame` operation returns a new frame. Shared ownership via `shared_ptr<arrow::Table>` means operations like `head()` and `Slice()` are zero-copy — the original data is not copied.

**Lazy DAG as a linked list of nodes.** Each `LazyDataFrame` holds a single pointer to the most recent `DAGNode`, which in turn holds a pointer to its input. This makes building the plan allocation-cheap and the optimizer straightforward to reason about.

**Fixed-point optimizer.** Rather than a fixed number of passes, rules run in a loop until nothing changes. This handles cases where one rule enables another without requiring the programmer to manually sequence passes.

**`std::stable_sort` for deterministic ordering.** Using `stable_sort` ensures that rows with identical sort-key values retain their original relative order, which matches the behavior users expect from SQL `ORDER BY`.

**Length-prefixed join keys.** Join keys are encoded as `<len>\0<bytes>` rather than using a separator character. This prevents collisions between multi-column keys like `("a", "bc")` and `("ab", "c")` which would both hash to the same string with a naive concatenation.

---

## 10. Running Tests

```bash
cd build
./dfl_test_expressions    # expression evaluator: arithmetic, comparison, aggregation, strings
./dfl_test_eager          # EagerDataFrame: select, filter, sort, join, group_by
./dfl_test_lazy           # LazyDataFrame: collect, sink, explain, parquet
./dfl_test_optimizer      # QueryOptimizer: structure checks + correctness checks
```

All 4 suites must pass (total 37 individual test cases).

To regenerate the DAG visualization PNGs:

```bash
cd build
./dataframe_test
# produces: test_optimizer.png  test_explain.png
```
