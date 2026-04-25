#pragma once

#include "Expression.hpp"
#include "IO.hpp"

#include <arrow/api.h>

#include <map>
#include <string>
#include <utility>
#include <vector>

namespace dataframelib {

class GroupedDataFrame;

class EagerDataFrame {
public:
    explicit EagerDataFrame(std::shared_ptr<arrow::Table> table);

    const std::shared_ptr<arrow::Table>& table() const;

    int64_t num_rows() const;
    int     num_cols() const;
    int     num_columns() const { return num_cols(); }

    // Select columns by name.
    EagerDataFrame select(const std::vector<std::string>& columns) const;

    // Braced-init-list overload: preferred over vector<Expr> for {"a","b"} literals.
    EagerDataFrame select(std::initializer_list<std::string> columns) const {
        return select(std::vector<std::string>(columns));
    }

    // Select / compute columns from expressions; non-col expressions require .alias().
    EagerDataFrame select(const std::vector<Expr>& exprs) const;

    // Return the first n rows.
    EagerDataFrame head(int64_t n) const;

    // Sort by columns; ascending[i] applies to columns[i].
    EagerDataFrame sort(const std::vector<std::string>& columns,
                        const std::vector<bool>& ascending) const;

    // Sort all columns with the same direction.
    EagerDataFrame sort(const std::vector<std::string>& columns, bool ascending) const {
        return sort(columns, std::vector<bool>(columns.size(), ascending));
    }

    // Keep only rows where predicate evaluates to true.
    EagerDataFrame filter(const Expr& predicate) const;

    // Append or replace a column evaluated from an expression.
    EagerDataFrame with_column(const std::string& name, const Expr& expr) const;

    // Begin a group-by operation; call .aggregate() on the result.
    GroupedDataFrame group_by(const std::vector<std::string>& keys) const;

    // Hash join with another DataFrame. how: "inner" | "left" | "right" | "outer".
    EagerDataFrame join(const EagerDataFrame& other,
                        const std::vector<std::string>& on,
                        const std::string& how = "inner") const;

    void write_csv(const std::string& path) const;
    void write_parquet(const std::string& path) const;

private:
    std::shared_ptr<arrow::Table> table_;
};

// Intermediate object returned by group_by(); call aggregate() to materialise.
class GroupedDataFrame {
public:
    GroupedDataFrame(std::shared_ptr<arrow::Table> table,
                     std::vector<std::string> keys);

    // Compute aggregations per group. Keys: output column name; values: agg expression.
    EagerDataFrame aggregate(const std::map<std::string, Expr>& agg_map) const;

    // String-based aggregation: specs are {col, "sum"|"mean"|"count"|"min"|"max"}.
    // Output column is named col_agg (e.g. "salary_sum").
    EagerDataFrame aggregate(const std::vector<std::pair<std::string, std::string>>& specs) const;

private:
    std::shared_ptr<arrow::Table> table_;
    std::vector<std::string>      keys_;
};

// Top-level free functions that return EagerDataFrame.
EagerDataFrame read_csv(const std::string& path);
EagerDataFrame read_parquet(const std::string& path);
EagerDataFrame from_columns(
    const std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>& columns);

} // namespace dataframelib
