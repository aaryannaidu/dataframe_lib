#pragma once

#include <arrow/api.h>
#include <variant>
#include <string>
#include <vector>

#include "Expression.hpp"

namespace dataframelib {

// Single cell value; monostate = null.
using NativeValue = std::variant<std::monostate, int64_t, double, std::string, bool>;

namespace compute {

// ── Scalar ↔ NativeValue ──────────────────────────────────────────────────

// Get Arrow scalar at a given row in a ChunkedArray.
std::shared_ptr<arrow::Scalar> scalar_at(const std::shared_ptr<arrow::ChunkedArray>& col,
                                          int64_t row);

NativeValue to_native(const std::shared_ptr<arrow::Scalar>& s);

std::shared_ptr<arrow::Scalar> from_native(const NativeValue& v,
                                            const std::shared_ptr<arrow::DataType>& type);

// ── Element-wise operations on Datums ────────────────────────────────────
// Datums may be Scalar (broadcast) or ChunkedArray. Return type is inferred.

arrow::Datum apply_binop(const arrow::Datum& left, const arrow::Datum& right, BinOp op);
arrow::Datum apply_unary(const arrow::Datum& input, UnaryOp op);
arrow::Datum apply_agg(const arrow::Datum& input, AggType agg);
arrow::Datum apply_strfunc(const arrow::Datum& input, StrFunc func, const std::string& arg);

// ── Table-level helpers ───────────────────────────────────────────────────

// Rebuild table keeping only the rows at `indices` (all >= 0).
std::shared_ptr<arrow::Table> take_rows(const std::shared_ptr<arrow::Table>& table,
                                         const std::vector<int64_t>& indices);

// Return sorted row indices for the given columns and directions.
std::vector<int64_t> sort_indices(const std::shared_ptr<arrow::Table>& table,
                                   const std::vector<std::string>& columns,
                                   const std::vector<bool>& ascending);

// Return row indices where the boolean mask Datum is true.
std::vector<int64_t> filter_indices(const arrow::Datum& mask);

} // namespace compute
} // namespace dataframelib
