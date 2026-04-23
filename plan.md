# DataFrameLib – Implementation Plan

## Project Structure

```
project/
├── CMakeLists.txt
├── README.md
├── report.pdf
├── include/
│   ├── dataframelib.hpp          (single public header that includes everything)
│   ├── Expression.hpp
│   ├── EagerDataFrame.hpp
│   ├── LazyDataFrame.hpp
│   ├── DAGNode.hpp
│   ├── QueryOptimizer.hpp
│   └── IO.hpp
├── src/
│   ├── Expression.cpp
│   ├── EagerDataFrame.cpp
│   ├── LazyDataFrame.cpp
│   ├── DAGNode.cpp
│   ├── QueryOptimizer.cpp
│   └── IO.cpp
└── tests/
    ├── test_expressions.cpp
    ├── test_eager.cpp
    ├── test_lazy.cpp
    └── test_optimizer.cpp
```

---

## Step 0: Setup

- [ ] Install Apache Arrow, Graphviz, CMake via Homebrew
- [ ] Create project folder with the structure above
- [ ] Write CMakeLists.txt that links Arrow, Parquet, Graphviz
- [ ] Write a tiny main.cpp that reads a CSV using Arrow — confirm everything compiles
- [ ] Create a small test CSV with mixed types (int, float, string, bool, nulls)

---

## Step 1: Expression System

**File: `Expression.hpp` / `Expression.cpp`**

This is the foundation — everything else depends on it.

### 1a. Base Expression class
- [ ] Create an abstract `Expr` base class (use `shared_ptr<Expr>` everywhere)
- [ ] Add an enum for expression types: `COL`, `LIT`, `BINOP`, `UNARYOP`, `AGG`, `STRFUNC`, `ALIAS`

### 1b. Core expression types
- [ ] `ColExpr` — stores a column name string
- [ ] `LitExpr` — stores a value (use `std::variant<int32_t, int64_t, float, double, bool, std::string>`)
- [ ] `AliasExpr` — wraps an expression + a new name; exposed as `.alias(name)` method on `Expr`

### 1c. Arithmetic expressions
- [ ] `BinaryOpExpr` — stores left expr, right expr, operator enum (+, -, *, /, %)
- [ ] `UnaryOpExpr` — stores input expr, operator enum (abs, negate)
- [ ] Overload `operator+`, `-`, `*`, `/`, `%` on Expr so users can write `col("x") + col("y")`

### 1d. Comparison and boolean expressions
- [ ] Add comparison operators to BinaryOpExpr: ==, !=, <, <=, >, >=
- [ ] Add boolean operators: &, |, ~
- [ ] Overload these operators on Expr

### 1e. Null check expressions
- [ ] `is_null()` and `is_not_null()` as methods on Expr

### 1f. String function expressions
- [ ] `StrFuncExpr` — stores input expr + function name + optional argument
- [ ] Methods: `length()`, `contains(s)`, `starts_with(s)`, `ends_with(s)`, `to_lower()`, `to_upper()`

### 1g. Aggregation expressions
- [ ] `AggExpr` — stores input expr + aggregation type
- [ ] Methods: `sum()`, `mean()`, `count()`, `min()`, `max()`

### 1h. Helper functions
- [ ] `col(name)` — free function that returns a `shared_ptr<ColExpr>`
- [ ] `lit(value)` — free function that returns a `shared_ptr<LitExpr>`

### Test checkpoint
- [ ] Verify you can build expression trees: `col("age") > lit(30)` compiles and creates the right tree

---

## Step 2: Expression Evaluator

**Add to: `Expression.hpp` / `Expression.cpp`**

The evaluator takes an expression tree + actual Arrow data → produces an Arrow array result.

- [ ] Write an `evaluate(expr, table)` function that:
  - `ColExpr` → look up column in the Arrow table, return it
  - `LitExpr` → create an Arrow array filled with that value (matching row count)
  - `BinaryOpExpr` → evaluate left & right, apply the operator element-wise
  - `UnaryOpExpr` → evaluate input, apply operation
  - `AggExpr` → evaluate input, compute aggregate (returns single-value array)
  - `StrFuncExpr` → evaluate input, apply string function
  - `AliasExpr` → evaluate inner, just rename
- [ ] Handle type checking: int+float→float, string+int→error
- [ ] Handle null propagation: any null input → null output

### Test checkpoint
- [ ] Create an Arrow table manually, evaluate `col("x") + lit(10)`, verify result

---

## Step 3: I/O Functions

**File: `IO.hpp` / `IO.cpp`**

These are internal helpers. The public-facing free functions (`read_csv`, `read_parquet`, `scan_csv`, `scan_parquet`, `from_columns`) live in `dataframelib.hpp` and return the appropriate DataFrame type.

- [ ] `read_csv(path)` → uses Arrow CSV reader → returns `EagerDataFrame`
- [ ] `read_parquet(path)` → uses Arrow Parquet reader → returns `EagerDataFrame`
- [ ] `scan_csv(path)` → creates a `ScanNode` → returns `LazyDataFrame` (free function, not a method)
- [ ] `scan_parquet(path)` → same as above for Parquet
- [ ] `write_csv(table, path)` → writes Arrow Table to CSV
- [ ] `write_parquet(table, path)` → writes Arrow Table to Parquet
- [ ] `from_columns(map<string, shared_ptr<arrow::Array>>)` → builds Arrow Table, wraps in `EagerDataFrame` and returns it

### Test checkpoint
- [ ] Read a CSV, print column names and types, write it back as Parquet, read Parquet back

---

## Step 4: EagerDataFrame

**File: `EagerDataFrame.hpp` / `EagerDataFrame.cpp`**

Stores an `std::shared_ptr<arrow::Table>` internally. Each method runs immediately and returns a new EagerDataFrame.

### 4a. Basic structure
- [ ] Constructor from `shared_ptr<arrow::Table>`
- [ ] Internal `table()` getter

### 4b. Simple operations
- [ ] `select(vector<string> columns)` — pick columns by name from the table
- [ ] `select(vector<shared_ptr<Expr>> expressions)` — evaluate each expression, build new table from results
- [ ] `head(n)` — slice first n rows
- [ ] `sort(columns, ascending)` — sort by given columns

### 4c. Filter
- [ ] `filter(predicate)` — evaluate the predicate expression → get boolean array → apply as mask

### 4d. With column
- [ ] `with_column(name, expr)` — evaluate expr, append/replace column in table

### 4e. Group by + Aggregate
- [ ] `group_by(keys)` — returns an intermediate `GroupedDataFrame` object
- [ ] `aggregate(map<string, shared_ptr<Expr>> agg_map)` — output column name → expression (e.g. `col("salary").mean()`)
  - The expression must be an `AggExpr`; throw if it is not
  - For each group, evaluate each aggregation expression over the group's rows
- [ ] This is the hardest part of EagerDataFrame — take your time

### 4f. Join
- [ ] `join(other, on_columns, how)` — implement inner join first, then left/right/outer
- [ ] Use Arrow's hash-join or implement manually with hash maps

### 4g. I/O wrappers
- [ ] `write_csv(path)` — calls IO::write_csv on internal table
- [ ] `write_parquet(path)` — calls IO::write_parquet on internal table

### Test checkpoint
- [ ] Full pipeline test: read CSV → filter → select → group_by → aggregate → write CSV

---

## Step 5: DAG Node System

**File: `DAGNode.hpp` / `DAGNode.cpp`**

Each node represents one operation in the lazy computation plan.

### 5a. Node types
- [ ] Create an enum: `SCAN`, `FILTER`, `SELECT`, `WITH_COLUMN`, `GROUP_BY`, `AGGREGATE`, `JOIN`, `SORT`, `HEAD`

### 5b. DAGNode class
- [ ] Stores: node type, input node(s), parameters (expressions, column names, etc.)
- [ ] Each node has a unique ID
- [ ] `ScanNode` — stores file path + file type (CSV/Parquet)
- [ ] `FilterNode` — stores predicate expression + input node
- [ ] `SelectNode` — stores column list + input node
- [ ] `WithColumnNode` — stores name + expression + input node
- [ ] `GroupByNode` — stores key columns + input node
- [ ] `AggregateNode` — stores `map<string, shared_ptr<Expr>>` agg_map + input node
- [ ] `JoinNode` — stores two input nodes + join columns + join type
- [ ] `SortNode` — stores sort columns + ascending flag + input node
- [ ] `HeadNode` — stores n + input node

---

## Step 6: LazyDataFrame

**File: `LazyDataFrame.hpp` / `LazyDataFrame.cpp`**

Each LazyDataFrame is just a pointer to the latest DAG node. Methods create new nodes and return new LazyDataFrames.

### 6a. Construction
- [ ] `scan_csv(path)` and `scan_parquet(path)` are free functions (defined in `dataframelib.hpp`) that create a `ScanNode` and return a `LazyDataFrame` — they are NOT methods on `LazyDataFrame`

### 6b. Operations (each returns a new LazyDataFrame)
- [ ] `filter(predicate)` → creates FilterNode pointing to current node
- [ ] `select(columns)` → creates SelectNode
- [ ] `with_column(name, expr)` → creates WithColumnNode
- [ ] `group_by(keys)` → creates GroupByNode
- [ ] `aggregate(map<string, shared_ptr<Expr>> agg_map)` → creates AggregateNode
  - Same signature as EagerDataFrame: output name → aggregation expression
- [ ] `join(other, on, how)` → creates JoinNode with two inputs
- [ ] `sort(columns, asc)` → creates SortNode
- [ ] `head(n)` → creates HeadNode

### 6c. Execution
- [ ] `collect()` → calls the optimizer, then executes the optimized DAG
- [ ] DAG executor: walk from root to leaves (post-order), execute each node using EagerDataFrame operations
- [ ] Return final EagerDataFrame

### 6d. Sink operations
- [ ] `sink_csv(path)` → collect + write CSV
- [ ] `sink_parquet(path)` → collect + write Parquet

### Test checkpoint
- [ ] Build a lazy pipeline, call collect(), verify result matches eager equivalent

---

## Step 7: DAG Visualization

**Add to: `LazyDataFrame`**

- [ ] `explain(path)` method on LazyDataFrame
- [ ] Capture the unoptimized DAG before running the optimizer
- [ ] Run the optimizer to get the optimized DAG
- [ ] Generate a single `.dot` file with two subgraphs side-by-side: `subgraph cluster_before` and `subgraph cluster_after`
  - Each node → a labeled box (node type + key parameters)
  - Each edge → an arrow from child to parent
- [ ] Call `dot -Tpng plan.dot -o <path>` via `system()`

### Test checkpoint
- [ ] Build a complex lazy query, call explain(), open the PNG and verify both the unoptimized and optimized DAGs look correct

---

## Step 8: Query Optimizer

**File: `QueryOptimizer.hpp` / `QueryOptimizer.cpp`**

Takes a DAG root node → returns an optimized DAG root node. Implement as a series of passes over the tree.

### 8a. Framework
- [ ] `optimize(root_node)` → applies all rules → returns new root
- [ ] Each rule is a function that takes a node and returns a (possibly rewritten) node
- [ ] Apply rules repeatedly until the DAG stops changing (fixed-point)

### 8b. Predicate Pushdown
- [ ] If a Filter is above a Join, try to push it down into one side of the join
- [ ] If a Filter is above a Select, swap them (filter first, then select — if filter columns are available)
- [ ] If a Filter is above a Sort, push it below the Sort (filter first, then sort the smaller result)

### 8c. Projection Pushdown
- [ ] Track which columns are actually needed downstream
- [ ] Modify Scan nodes to only read those columns
- [ ] Add Select nodes after scans to drop unneeded columns early

### 8d. Constant Folding
- [ ] Walk expression trees — if both children of a BinaryOp are literals, evaluate at plan time
- [ ] Replace the subtree with a single LitExpr

### 8e. Expression Simplification
- [ ] `x * 1` → `x`
- [ ] `x + 0` → `x`
- [ ] `x * 0` → `0`
- [ ] `x - x` → `0`

### 8f. Limit Pushdown
- [ ] If Head is above a Select, push Head below Select
- [ ] If Head is above a Sort, keep it above (can't push past sort)

### Test checkpoint
- [ ] Build a lazy query with suboptimal ordering
- [ ] Call explain() and verify the optimized DAG has pushed filters/projections down
- [ ] Benchmark both plans — optimized should be faster

---

## Step 9: Final Polish

### 9a. Edge cases and robustness
- [ ] Empty DataFrames
- [ ] Single-row DataFrames
- [ ] All-null columns
- [ ] Type mismatch errors with clear messages
- [ ] Missing column name errors

### 9b. Memory management
- [ ] Use `shared_ptr` everywhere for Arrow tables and DAG nodes
- [ ] No raw `new`/`delete`
- [ ] Use move semantics where possible

### 9c. Documentation
- [ ] Document every public method with brief comments
- [ ] Write README with build and usage instructions

### 9d. Report (report.pdf)
- [ ] Architecture overview
- [ ] For each optimization: description, correctness proof, example, expected speedup
- [ ] Any design decisions worth explaining

### 9e. Submission prep
- [ ] Delete `build/` directory
- [ ] Delete any generated data files
- [ ] `tar -cvf <entry_number>.tar project`
- [ ] Double check the tar contains everything
- [ ] Submit on Moodle

---

## Priority if running out of time

1. Expression system (everything depends on it)
2. EagerDataFrame (45% correctness grade)
3. LazyDataFrame with collect() working
4. At least 2 optimizer rules (predicate + projection pushdown)
5. explain() visualization
6. Remaining optimizer rules
