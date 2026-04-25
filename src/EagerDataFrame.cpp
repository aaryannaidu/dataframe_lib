#include "EagerDataFrame.hpp"

#include <arrow/compute/api.h>

#include <set>
#include <stdexcept>
#include <unordered_map>

namespace dataframelib {

namespace {


// Resolve the output column name for a select expression.
std::string expr_output_name(const Expr& e) {
    if (e.type() == ExprType::COL)
        return static_cast<const ColNode&>(*e.node()).name;
    if (e.type() == ExprType::ALIAS)
        return static_cast<const AliasNode&>(*e.node()).name;
    throw std::runtime_error("select: non-column expression must have .alias(name)");
}

// Wrap any array-like Datum as a ChunkedArray.
std::shared_ptr<arrow::ChunkedArray> to_chunked(const arrow::Datum& d,
                                                  const std::string& ctx) {
    if (d.is_chunked_array()) return d.chunked_array();
    if (d.is_array())
        return std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{d.make_array()});
    throw std::runtime_error(ctx + ": expression did not produce an array");
}

// Get the scalar value at a global row index from a ChunkedArray.
std::shared_ptr<arrow::Scalar> scalar_at(
    const std::shared_ptr<arrow::ChunkedArray>& col, int64_t row) {
    int64_t offset = row;
    for (const auto& chunk : col->chunks()) {
        if (offset < chunk->length())
            return chunk->GetScalar(offset).ValueOrDie();
        offset -= chunk->length();
    }
    throw std::runtime_error("scalar_at: row index out of bounds");
}

// Build an Arrow Array from a vector of scalars (null scalars → null entries).
std::shared_ptr<arrow::Array> array_from_scalars(
    const std::shared_ptr<arrow::DataType>& type,
    const std::vector<std::shared_ptr<arrow::Scalar>>& scalars) {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    auto st = arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder);
    if (!st.ok()) throw std::runtime_error("array_from_scalars: " + st.message());
    for (const auto& s : scalars) {
        st = builder->AppendScalar(*s);
        if (!st.ok()) throw std::runtime_error("array_from_scalars: " + st.message());
    }
    std::shared_ptr<arrow::Array> arr;
    st = builder->Finish(&arr);
    if (!st.ok()) throw std::runtime_error("array_from_scalars: " + st.message());
    return arr;
}

// Take rows from a ChunkedArray by index; index -1 produces a null entry.
std::shared_ptr<arrow::Array> take_with_nulls(
    const std::shared_ptr<arrow::ChunkedArray>& col,
    const std::vector<int64_t>& indices) {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    auto st = arrow::MakeBuilder(arrow::default_memory_pool(), col->type(), &builder);
    if (!st.ok()) throw std::runtime_error("take_with_nulls: " + st.message());
    for (int64_t idx : indices) {
        st = (idx < 0) ? builder->AppendNull()
                       : builder->AppendScalar(*scalar_at(col, idx));
        if (!st.ok()) throw std::runtime_error("take_with_nulls: " + st.message());
    }
    std::shared_ptr<arrow::Array> arr;
    st = builder->Finish(&arr);
    if (!st.ok()) throw std::runtime_error("take_with_nulls: " + st.message());
    return arr;
}

// Find the starting row index of each group in a table already sorted by key columns.
std::vector<int64_t> find_group_starts(
    const std::shared_ptr<arrow::Table>& table,
    const std::vector<std::string>& keys) {
    if (table->num_rows() == 0) return {};
    std::vector<std::shared_ptr<arrow::ChunkedArray>> key_cols;
    for (const auto& k : keys) {
        auto c = table->GetColumnByName(k);
        if (!c) throw std::runtime_error("group_by: key column not found: \"" + k + "\"");
        key_cols.push_back(c);
    }
    std::vector<int64_t> starts = {0};
    for (int64_t i = 1; i < table->num_rows(); ++i) {
        for (const auto& kc : key_cols) {
            if (!scalar_at(kc, i - 1)->Equals(*scalar_at(kc, i))) {
                starts.push_back(i);
                break;
            }
        }
    }
    return starts;
}

// Build a string key from the join columns of a single row (used for hash join).
std::string row_key(const std::shared_ptr<arrow::Table>& table,
                     int64_t row,
                     const std::vector<std::string>& cols) {
    std::string key;
    for (const auto& c : cols) {
        key += scalar_at(table->GetColumnByName(c), row)->ToString();
        key += '\0';
    }
    return key;
}

} // anonymous namespace


EagerDataFrame::EagerDataFrame(std::shared_ptr<arrow::Table> table)
    : table_(std::move(table)) {}

const std::shared_ptr<arrow::Table>& EagerDataFrame::table() const { return table_; }
int64_t EagerDataFrame::num_rows() const { return table_->num_rows(); }
int     EagerDataFrame::num_cols() const { return table_->num_columns(); }


EagerDataFrame EagerDataFrame::select(const std::vector<std::string>& columns) const {
    std::vector<int> indices;
    indices.reserve(columns.size());
    for (const auto& name : columns) {
        int idx = table_->schema()->GetFieldIndex(name);
        if (idx < 0)
            throw std::runtime_error("select: column not found: \"" + name + "\"");
        indices.push_back(idx);
    }
    auto result = table_->SelectColumns(indices);
    if (!result.ok())
        throw std::runtime_error("select: " + result.status().message());
    return EagerDataFrame(result.ValueOrDie());
}

EagerDataFrame EagerDataFrame::select(const std::vector<Expr>& exprs) const {
    std::vector<std::shared_ptr<arrow::Field>>        fields;
    std::vector<std::shared_ptr<arrow::ChunkedArray>> arrays;
    fields.reserve(exprs.size());
    arrays.reserve(exprs.size());
    for (const auto& e : exprs) {
        std::string name = expr_output_name(e);
        auto col = to_chunked(evaluate(e, table_), "select(\"" + name + "\")");
        fields.push_back(arrow::field(name, col->type()));
        arrays.push_back(std::move(col));
    }
    return EagerDataFrame(arrow::Table::Make(arrow::schema(fields), arrays));
}


EagerDataFrame EagerDataFrame::head(int64_t n) const {
    return EagerDataFrame(table_->Slice(0, std::min(n, table_->num_rows())));
}

EagerDataFrame EagerDataFrame::sort(const std::vector<std::string>& columns,
                                     const std::vector<bool>& ascending) const {
    if (columns.size() != ascending.size())
        throw std::runtime_error("sort: columns and ascending vectors must have the same size");
    std::vector<arrow::compute::SortKey> keys;
    keys.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        keys.emplace_back(columns[i],
            ascending[i] ? arrow::compute::SortOrder::Ascending
                         : arrow::compute::SortOrder::Descending);
    auto indices = arrow::compute::SortIndices(arrow::Datum(table_),
                                                arrow::compute::SortOptions(keys));
    if (!indices.ok()) throw std::runtime_error("sort: " + indices.status().message());
    auto taken = arrow::compute::Take(arrow::Datum(table_), arrow::Datum(indices.ValueOrDie()));
    if (!taken.ok()) throw std::runtime_error("sort: " + taken.status().message());
    return EagerDataFrame(taken.ValueOrDie().table());
}

EagerDataFrame EagerDataFrame::filter(const Expr& predicate) const {
    auto mask   = evaluate(predicate, table_);
    auto result = arrow::compute::CallFunction("filter", {arrow::Datum(table_), mask});
    if (!result.ok()) throw std::runtime_error("filter: " + result.status().message());
    return EagerDataFrame(result.ValueOrDie().table());
}


EagerDataFrame EagerDataFrame::with_column(const std::string& name, const Expr& expr) const {
    auto col   = to_chunked(evaluate(expr, table_), "with_column(\"" + name + "\")");
    auto field = arrow::field(name, col->type());
    int  idx   = table_->schema()->GetFieldIndex(name);
    std::shared_ptr<arrow::Table> new_table;
    if (idx >= 0) {
        auto r = table_->SetColumn(idx, field, col);
        if (!r.ok()) throw std::runtime_error("with_column: " + r.status().message());
        new_table = r.ValueOrDie();
    } else {
        auto r = table_->AddColumn(table_->num_columns(), field, col);
        if (!r.ok()) throw std::runtime_error("with_column: " + r.status().message());
        new_table = r.ValueOrDie();
    }
    return EagerDataFrame(new_table);
}


GroupedDataFrame EagerDataFrame::group_by(const std::vector<std::string>& keys) const {
    return GroupedDataFrame(table_, keys);
}


EagerDataFrame EagerDataFrame::join(const EagerDataFrame& other,
                                     const std::vector<std::string>& on,
                                     const std::string& how) const {
    if (how != "inner" && how != "left" && how != "right" && how != "outer")
        throw std::runtime_error("join: unknown join type \"" + how + "\"");
    for (const auto& k : on) {
        if (!table_->GetColumnByName(k))
            throw std::runtime_error("join: key \"" + k + "\" not found in left table");
        if (!other.table()->GetColumnByName(k))
            throw std::runtime_error("join: key \"" + k + "\" not found in right table");
    }

    // Build hash index on the right table.
    std::unordered_map<std::string, std::vector<int64_t>> right_index;
    int64_t right_rows = other.table()->num_rows();
    for (int64_t i = 0; i < right_rows; ++i)
        right_index[row_key(other.table(), i, on)].push_back(i);

    // Probe left table; collect (left_idx, right_idx) pairs (-1 = null side).
    std::vector<int64_t> left_take, right_take;
    std::vector<bool> right_matched(right_rows, false);

    for (int64_t i = 0; i < table_->num_rows(); ++i) {
        auto it = right_index.find(row_key(table_, i, on));
        if (it != right_index.end()) {
            for (int64_t j : it->second) {
                left_take.push_back(i);
                right_take.push_back(j);
                right_matched[j] = true;
            }
        } else if (how == "left" || how == "outer") {
            left_take.push_back(i);
            right_take.push_back(-1);
        }
    }
    if (how == "right" || how == "outer") {
        for (int64_t j = 0; j < right_rows; ++j) {
            if (!right_matched[j]) {
                left_take.push_back(-1);
                right_take.push_back(j);
            }
        }
    }

    // Build result: all left columns + right columns that are not join keys.
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;

    for (int c = 0; c < table_->num_columns(); ++c) {
        fields.push_back(table_->schema()->field(c));
        arrays.push_back(take_with_nulls(table_->column(c), left_take));
    }
    std::set<std::string> key_set(on.begin(), on.end());
    for (int c = 0; c < other.table()->num_columns(); ++c) {
        auto f = other.table()->schema()->field(c);
        if (key_set.count(f->name())) continue;
        fields.push_back(f);
        arrays.push_back(take_with_nulls(other.table()->column(c), right_take));
    }

    return EagerDataFrame(arrow::Table::Make(arrow::schema(fields), arrays));
}


void EagerDataFrame::write_csv(const std::string& path) const     { io::write_csv(table_, path); }
void EagerDataFrame::write_parquet(const std::string& path) const { io::write_parquet(table_, path); }


GroupedDataFrame::GroupedDataFrame(std::shared_ptr<arrow::Table> table,
                                    std::vector<std::string> keys)
    : table_(std::move(table)), keys_(std::move(keys)) {}

EagerDataFrame GroupedDataFrame::aggregate(
    const std::map<std::string, Expr>& agg_map) const {
    if (agg_map.empty())
        throw std::runtime_error("aggregate: agg_map must not be empty");
    for (const auto& [name, expr] : agg_map)
        if (expr.type() != ExprType::AGG)
            throw std::runtime_error(
                "aggregate: expression for \"" + name + "\" must be an aggregation (sum/mean/count/min/max)");

    // Sort by key columns so that equal groups are contiguous.
    std::vector<bool> asc(keys_.size(), true);
    auto sorted = EagerDataFrame(table_).sort(keys_, asc).table();
    auto starts = find_group_starts(sorted, keys_);
    int64_t num_groups = static_cast<int64_t>(starts.size());

    // Accumulators: key column scalars + aggregation result scalars.
    std::map<std::string, std::vector<std::shared_ptr<arrow::Scalar>>> key_scalars;
    std::map<std::string, std::vector<std::shared_ptr<arrow::Scalar>>> agg_scalars;
    for (const auto& k : keys_)         key_scalars[k] = {};
    for (const auto& [n, _] : agg_map)  agg_scalars[n] = {};

    for (int64_t g = 0; g < num_groups; ++g) {
        int64_t start  = starts[g];
        int64_t length = (g + 1 < num_groups) ? starts[g + 1] - start
                                               : sorted->num_rows() - start;
        auto group = sorted->Slice(start, length);

        for (const auto& k : keys_)
            key_scalars[k].push_back(scalar_at(group->GetColumnByName(k), 0));

        for (const auto& [name, expr] : agg_map) {
            auto datum = evaluate(expr, group);
            if (!datum.is_scalar())
                throw std::runtime_error(
                    "aggregate: \"" + name + "\" did not produce a scalar");
            agg_scalars[name].push_back(datum.scalar());
        }
    }

    // Assemble result table: key columns first, then aggregation columns.
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;

    for (const auto& k : keys_) {
        auto type = sorted->schema()->GetFieldByName(k)->type();
        fields.push_back(arrow::field(k, type));
        arrays.push_back(array_from_scalars(type, key_scalars[k]));
    }
    for (const auto& [name, expr] : agg_map) {
        const auto& scalars = agg_scalars[name];
        auto type = scalars.empty() ? arrow::null() : scalars[0]->type;
        fields.push_back(arrow::field(name, type));
        arrays.push_back(array_from_scalars(type, scalars));
    }

    return EagerDataFrame(arrow::Table::Make(arrow::schema(fields), arrays));
}


EagerDataFrame read_csv(const std::string& path)    { return EagerDataFrame(io::read_csv(path)); }
EagerDataFrame read_parquet(const std::string& path) { return EagerDataFrame(io::read_parquet(path)); }
EagerDataFrame from_columns(
    const std::map<std::string, std::shared_ptr<arrow::Array>>& columns) {
    return EagerDataFrame(io::from_columns(columns));
}

}
