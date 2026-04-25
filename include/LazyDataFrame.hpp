#pragma once

#include "DAGNode.hpp"
#include "EagerDataFrame.hpp"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace dataframelib {

class LazyDataFrame {
public:
    explicit LazyDataFrame(std::shared_ptr<DAGNode> node);

    const std::shared_ptr<DAGNode>& node() const;

    LazyDataFrame filter(const Expr& predicate) const;

    // Select by column names.
    LazyDataFrame select(const std::vector<std::string>& columns) const;

    // Braced-init-list overload: preferred over vector<Expr> for {"a","b"} literals.
    LazyDataFrame select(std::initializer_list<std::string> columns) const {
        return select(std::vector<std::string>(columns));
    }

    // Select by expressions; non-col expressions require .alias().
    LazyDataFrame select(const std::vector<Expr>& exprs) const;

    LazyDataFrame with_column(const std::string& name, const Expr& expr) const;

    // Begins a group-by; must be followed by .aggregate().
    LazyDataFrame group_by(const std::vector<std::string>& keys) const;

    // Creates an AggregateNode; the current node must be a GroupByNode.
    LazyDataFrame aggregate(const std::map<std::string, Expr>& agg_map) const;

    // String-based aggregation: specs are {col, "sum"|"mean"|"count"|"min"|"max"}.
    LazyDataFrame aggregate(const std::vector<std::pair<std::string, std::string>>& specs) const;

    LazyDataFrame join(const LazyDataFrame& other,
                       const std::vector<std::string>& on,
                       const std::string& how = "inner") const;

    LazyDataFrame sort(const std::vector<std::string>& columns,
                       const std::vector<bool>& ascending) const;

    LazyDataFrame sort(const std::vector<std::string>& columns, bool ascending) const {
        return sort(columns, std::vector<bool>(columns.size(), ascending));
    }

    LazyDataFrame head(int64_t n) const;

    // Run the optimizer then execute the DAG, returning a materialised EagerDataFrame.
    EagerDataFrame collect() const;

    // Visualise the query plan using Graphviz.
    void explain(const std::string& path) const;

    void sink_csv(const std::string& path) const;
    void sink_parquet(const std::string& path) const;

private:
    std::shared_ptr<DAGNode> node_;
};

// Entry points for building a lazy plan from a file.
LazyDataFrame scan_csv(const std::string& path);
LazyDataFrame scan_parquet(const std::string& path);

}
