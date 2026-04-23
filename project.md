## Page 1

DataFrameLib
COP290 – Assignment 4

# Assignment 4: DataFrameLib
Build a High-Performance DataFrame Library in C++
COP290 – Design Practices

**Deadline:** 27 April 2026, 11:59 PM.

## Contents
1 Background 2
2 Project Description 2
3 Required Operations 3
    3.1 I/O Functions 3
    3.2 DataFrame Operations 3
    3.3 Expression System 3
        3.3.1 Core Expressions 3
        3.3.2 Arithmetic Operations 3
        3.3.3 Comparison and Boolean Operations 3
        3.3.4 String Functions 4
        3.3.5 Aggregation Functions 4
4 Lazy Evaluation 4
5 Query Optimization 4
6 Example Usage 5
    6.1 Eager Mode 5
    6.2 Lazy Mode 5
7 Evaluation Rubric 5
    7.1 Evaluation Criteria 5
    7.2 Evaluation Method 5
8 Submission Instructions 5

&lt;page_number&gt;Page 1&lt;/page_number&gt;

---


## Page 2

<header>DataFrameLib</header>
<header>COP290 – Assignment 4</header>

# 1 Background

Tabular data processing is a core task in data engineering and data science. Libraries such as **Pandas** (Python) and **Polars** (Rust/Python) have become standard tools for working with structured, column-oriented datasets. Pandas provides an intuitive, *eager* execution model — every operation runs immediately and returns a materialised result. Polars, on the other hand, exposes a *lazy* API that builds a computation plan first and then optimises and executes it on demand, enabling significant performance gains through query optimisation.

In this assignment you will build **DataFrameLib**, a C++ library that replicates the core ideas behind both paradigms: an eager execution path for immediate, interactive use, and a lazy execution path backed by a query optimiser. We recommend getting familiar with Polars and Pandas if you have not used them before.

# 2 Project Description

Build **DataFrameLib**, a C++ library for tabular data processing with two execution modes and a query optimiser:

*   **EagerDataFrame** — immediate execution with materialised data.
*   **LazyDataFrame** — deferred execution via a computation DAG.
*   **QueryOptimizer** — optimises lazy execution plans before materialisation.

## Apache Arrow

Use Apache Arrow exclusively for all data storage and I/O:

*   In-memory columnar data representation.
*   Reading and writing CSV and Parquet files.
*   Type-safe column operations.

**Why Arrow?** Language-independent format, zero-copy sharing, cache-friendly columnar layout, and built-in I/O support. Documentation: https://arrow.apache.org/docs/cpp/

## Type Safety

The library must enforce strict type safety:

*   Each column has an immutable type: `int32`, `int64`, `float32`, `float64`, `string`, or `boolean`.
*   Missing values are represented as `null` only — not as NaN.
*   Type promotion rule: `int + float = float`.
*   Incompatible operations (e.g. adding a numeric and a non-numeric column) must throw errors immediately. Any operation involving a `null` operand must produce a `null` result.

&lt;page_number&gt;Page 2&lt;/page_number&gt;

---


## Page 3

<header>DataFrameLib</header>
<header>COP290 – Assignment 4</header>

# 3 Required Operations

## 3.1 I/O Functions

<table>
  <thead>
    <tr>
      <th>Function</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>read_csv(path)</td>
      <td>Load CSV into an EagerDataFrame</td>
    </tr>
    <tr>
      <td>read_parquet(path)</td>
      <td>Load Parquet into an EagerDataFrame</td>
    </tr>
    <tr>
      <td>scan_csv(path)</td>
      <td>Create a LazyDataFrame from CSV</td>
    </tr>
    <tr>
      <td>scan_parquet(path)</td>
      <td>Create a LazyDataFrame from Parquet</td>
    </tr>
    <tr>
      <td>write_csv(path)</td>
      <td>Write an EagerDataFrame to CSV</td>
    </tr>
    <tr>
      <td>write_parquet(path)</td>
      <td>Write an EagerDataFrame to Parquet</td>
    </tr>
    <tr>
      <td>sink_csv(path)</td>
      <td>Write a LazyDataFrame result to CSV</td>
    </tr>
    <tr>
      <td>sink_parquet(path)</td>
      <td>Write a LazyDataFrame result to Parquet</td>
    </tr>
    <tr>
      <td>from_columns(map)</td>
      <td>Build a DataFrame from a column map</td>
    </tr>
  </tbody>
</table>

## 3.2 DataFrame Operations

Both EagerDataFrame and LazyDataFrame must support:

<table>
  <thead>
    <tr>
      <th>Operation</th>
      <th>Description</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>select(columns)</td>
      <td>Select columns/expressions</td>
      <td>df.select({"a","b"})</td>
    </tr>
    <tr>
      <td>filter(predicate)</td>
      <td>Filter rows by a predicate</td>
      <td>df.filter(col("x") > 0)</td>
    </tr>
    <tr>
      <td>with_column(name, expr)</td>
      <td>Add or replace a column</td>
      <td>df.with_column("z", col("a")+col("b"))</td>
    </tr>
    <tr>
      <td>group_by(keys)</td>
      <td>Group by one or more columns</td>
      <td>df.group_by({"dept"})</td>
    </tr>
    <tr>
      <td>aggregate(agg_map)</td>
      <td>Aggregate grouped data</td>
      <td>df.aggregate({"x": "sum"})</td>
    </tr>
    <tr>
      <td>join(other, on, how)</td>
      <td>Join two DataFrames</td>
      <td>df.join(df2,{"id"},"inner")</td>
    </tr>
    <tr>
      <td>sort(columns, asc)</td>
      <td>Sort rows</td>
      <td>df.sort({"age"},true)</td>
    </tr>
    <tr>
      <td>head(n)</td>
      <td>First n rows</td>
      <td>df.head(10)</td>
    </tr>
    <tr>
      <td>collect()</td>
      <td>[Lazy only] Execute and materialise</td>
      <td>df.collect()</td>
    </tr>
  </tbody>
</table>

## 3.3 Expression System

### 3.3.1 Core Expressions

<table>
  <thead>
    <tr>
      <th>Expression</th>
      <th>Description</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>col(name)</td>
      <td>Column reference</td>
      <td>col("age")</td>
    </tr>
    <tr>
      <td>lit(value)</td>
      <td>Literal value</td>
      <td>lit(100)</td>
    </tr>
    <tr>
      <td>alias(name)</td>
      <td>Rename result</td>
      <td>col("x").alias("y")</td>
    </tr>
  </tbody>
</table>

### 3.3.2 Arithmetic Operations

<table>
  <thead>
    <tr>
      <th>Operation</th>
      <th>Description</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>+ - * /</td>
      <td>Element-wise arithmetic</td>
      <td>col("x") * col("y")</td>
    </tr>
    <tr>
      <td>%</td>
      <td>Modulo</td>
      <td>col("id") % 10</td>
    </tr>
    <tr>
      <td>abs()</td>
      <td>Absolute value</td>
      <td>col("x").abs()</td>
    </tr>
  </tbody>
</table>

### 3.3.3 Comparison and Boolean Operations

<table>
  <thead>
    <tr>
      <th>Operation</th>
      <th>Description</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>== != < <= > >=</td>
      <td>Comparisons</td>
      <td>col("age") > 30</td>
    </tr>
    <tr>
      <td>& | ~</td>
      <td>Boolean AND, OR, NOT</td>
      <td>(a > 0) & (b < 5)</td>
    </tr>
    <tr>
      <td>is_null()</td>
      <td>Check for nulls</td>
      <td>col("x").is_null()</td>
    </tr>
    <tr>
      <td>is_not_null()</td>
      <td>Check for non-nulls</td>
      <td>col("x").is_not_null()</td>
    </tr>
  </tbody>
</table>

&lt;page_number&gt;Page 3&lt;/page_number&gt;

---


## Page 4

DataFrameLib
COP290 – Assignment 4

### 3.3.4 String Functions

<table>
  <thead>
    <tr>
      <th>Function</th>
      <th>Description</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>length()</td>
      <td>String length</td>
      <td>col("name").length()</td>
    </tr>
    <tr>
      <td>contains(s)</td>
      <td>Substring check</td>
      <td>col("email").contains("@")</td>
    </tr>
    <tr>
      <td>starts_with(s)</td>
      <td>Prefix check</td>
      <td>col("code").starts_with("A")</td>
    </tr>
    <tr>
      <td>ends_with(s)</td>
      <td>Suffix check</td>
      <td>col("file").ends_with(".txt")</td>
    </tr>
    <tr>
      <td>to_lower()</td>
      <td>Lowercase</td>
      <td>col("city").to_lower()</td>
    </tr>
    <tr>
      <td>to_upper()</td>
      <td>Uppercase</td>
      <td>col("city").to_upper()</td>
    </tr>
  </tbody>
</table>

### 3.3.5 Aggregation Functions

<table>
  <thead>
    <tr>
      <th>Function</th>
      <th>Description</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>sum()</td>
      <td>Sum of values</td>
      <td>col("salary").sum()</td>
    </tr>
    <tr>
      <td>mean()</td>
      <td>Mean value</td>
      <td>col("score").mean()</td>
    </tr>
    <tr>
      <td>count()</td>
      <td>Count of non-null values</td>
      <td>col("id").count()</td>
    </tr>
    <tr>
      <td>min()</td>
      <td>Minimum value</td>
      <td>col("age").min()</td>
    </tr>
    <tr>
      <td>max()</td>
      <td>Maximum value</td>
      <td>col("age").max()</td>
    </tr>
  </tbody>
</table>

## 4 Lazy Evaluation

LazyDataFrame builds a directed acyclic graph (DAG) where:

*   **Nodes** — operations (scan, filter, select, join, ...)
*   **Edges** — data dependencies between operations
*   **Leaf nodes** — data sources (CSV / Parquet scans)

No data is processed during plan construction; execution is triggered only by `collect()`. You must also implement a graph-rendering utility — using Graphviz or Boost.Graph — that dumps the computation DAG to a `.png` file when `explain(path)` is called.

## 5 Query Optimization

For the same query, different execution plans can be generated, giving rise to optimisation opportunities. For example, joining two large DataFrames and then applying a filter is far more expensive than filtering first and then joining, even though the final output is identical. You must implement rule-based transformations that preserve correctness.

Part of the grading in this section is based on the running time of a fixed set of query benchmarks (i.e, the speedups the query optimizer introduces). A baseline is established for unoptimised plans; any submission that performs worse than the baseline receives zero for this section.

Some standard optimisations are listed below. You are free to explore and implement additional ones.

<table>
  <thead>
    <tr>
      <th>Optimisation</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Predicate Pushdown</td>
      <td>Move filter operations closer to scan nodes</td>
    </tr>
    <tr>
      <td>Projection Pushdown</td>
      <td>Read only the columns required by downstream operations</td>
    </tr>
    <tr>
      <td>Constant Folding</td>
      <td>Evaluate constant expressions at plan-construction time</td>
    </tr>
    <tr>
      <td>Expression Simplification</td>
      <td>Simplify trivial expressions, e.g. x*1+0 → x</td>
    </tr>
    <tr>
      <td>Limit Pushdown</td>
      <td>Apply row-count limits as early as possible</td>
    </tr>
  </tbody>
</table>

For each optimisation you implement, your report must: describe the transformation, prove its correctness, provide a concrete example, and explain the expected performance benefit.

&lt;page_number&gt;Page 4&lt;/page_number&gt;

---


## Page 5

DataFrameLib
COP290 – Assignment 4

# 6 Example Usage

## 6.1 Eager Mode

```cpp
auto df = read_parquet("data.parquet");
auto result = df.filter(col("age") > 30)
    .select({"name", "salary"})
    .write_csv("output.csv");
```

## 6.2 Lazy Mode

```cpp
auto df = scan_parquet("data.parquet");
auto result = df
    .filter(col("age") > 30)
    .select({"name", "salary"})
    .group_by({"dept"})
    .aggregate({{"avg_sal", col("salary").mean()}});
std::string plan_path = "plan.png";
result.explain(plan_path); // Dump the optimised DAG
auto output = result.collect(); // Execute
```

# 7 Evaluation Rubric

## 7.1 Evaluation Criteria

<table>
  <thead>
    <tr>
      <th>Component</th>
      <th>Weight</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Correctness</td>
      <td>45%</td>
      <td>Autograded against TA-designed test cases covering all required operations.</td>
    </tr>
    <tr>
      <td>Query Optimisation</td>
      <td>15%</td>
      <td>Graded by benchmark running time relative to other submissions. Performance below the unoptimised baseline receives zero.</td>
    </tr>
    <tr>
      <td>Memory Management & Performance</td>
      <td>15%</td>
      <td>Assessed for proper use of RAII, smart pointers, move semantics, and absence of memory leaks.</td>
    </tr>
    <tr>
      <td>Code Quality & Documentation</td>
      <td>10%</td>
      <td>Clarity, organisation, and standard practices. The public API must be thoroughly documented.</td>
    </tr>
    <tr>
      <td>Viva</td>
      <td>15%</td>
      <td></td>
    </tr>
  </tbody>
</table>

Note: A comprehensive list of tested operations and a set of sample test cases will be shared shortly.

## 7.2 Evaluation Method

Correctness is evaluated through TA-designed test cases that exercise the library via its public API. Please follow the API signatures in this document exactly — any deviation may cause tests to fail. There will additionally be a demo and viva; if the implementation is correct but viva performance falls short of expectations, marks may be significantly reduced.

# 8 Submission Instructions

1. You must submit:
    * Complete source code implementing EagerDataFrame, LazyDataFrame, and QueryOptimizer.
    * Build and installation instructions (README).

&lt;page_number&gt;Page 5&lt;/page_number&gt;

---


## Page 6

DataFrameLib
COP290 – Assignment 4

*   A design document (report.pdf) describing your architecture and all optimisations implemented.

2.  The project must compile using standard CMake. **Do not** modify the build system or introduce external libraries beyond Apache Arrow and Graphviz / Boost.Graph.

3.  Before packaging, remove all compiled binaries and build artefacts (e.g. delete the build/ directory). Submissions containing unnecessary build files may receive a penalty.

4.  Do not include generated output directories or data files in your archive.

5.  We will use modern AI-based plagiarism checkers. A match will result in **zero** for this assignment and another assignment of our choosing.

6.  There may additionally be a demo and viva. It is possible that the assignment works correctly but viva performance falls short of expectations, in which case marks may be significantly reduced.

7.  All submissions will be via **Moodle**. Create a tar file as follows:
    (a) Place all your files in a directory named project.
    (b) Navigate to the parent directory of project.
    (c) Run: tar -cvf <entry_number>.tar project
    (d) An entry number is of the form 2025ANZ8223 — it is **not** your user ID or email address.
    (e) Submit the resulting tar file on Moodle.

8.  Failure to follow the required submission format may result in a penalty.

9.  Submit several hours before the deadline and double-check your submission.

10. **No emails will be entertained. Kindly use Piazza for all discussions, clarifications and announcements.**

&lt;page_number&gt;Page 6&lt;/page_number&gt;