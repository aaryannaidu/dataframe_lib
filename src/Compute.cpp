#include "Compute.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <numeric>
#include <optional>
#include <stdexcept>

namespace dataframelib {
namespace compute {

// ── scalar_at ────────────────────────────────────────────────────────────

std::shared_ptr<arrow::Scalar> scalar_at(const std::shared_ptr<arrow::ChunkedArray>& col,
                                          int64_t row) {
    int64_t offset = row;
    for (const auto& chunk : col->chunks()) {
        if (offset < chunk->length())
            return chunk->GetScalar(offset).ValueOrDie();
        offset -= chunk->length();
    }
    throw std::runtime_error("scalar_at: row index out of bounds");
}

// ── NativeValue conversion ────────────────────────────────────────────────

NativeValue to_native(const std::shared_ptr<arrow::Scalar>& s) {
    if (!s || !s->is_valid) return std::monostate{};
    switch (s->type->id()) {
        case arrow::Type::INT8:
            return (int64_t)std::static_pointer_cast<arrow::Int8Scalar>(s)->value;
        case arrow::Type::INT16:
            return (int64_t)std::static_pointer_cast<arrow::Int16Scalar>(s)->value;
        case arrow::Type::INT32:
            return (int64_t)std::static_pointer_cast<arrow::Int32Scalar>(s)->value;
        case arrow::Type::INT64:
            return std::static_pointer_cast<arrow::Int64Scalar>(s)->value;
        case arrow::Type::UINT8:
            return (int64_t)std::static_pointer_cast<arrow::UInt8Scalar>(s)->value;
        case arrow::Type::UINT16:
            return (int64_t)std::static_pointer_cast<arrow::UInt16Scalar>(s)->value;
        case arrow::Type::UINT32:
            return (int64_t)std::static_pointer_cast<arrow::UInt32Scalar>(s)->value;
        case arrow::Type::UINT64:
            return (int64_t)std::static_pointer_cast<arrow::UInt64Scalar>(s)->value;
        case arrow::Type::FLOAT:
            return (double)std::static_pointer_cast<arrow::FloatScalar>(s)->value;
        case arrow::Type::DOUBLE:
            return std::static_pointer_cast<arrow::DoubleScalar>(s)->value;
        case arrow::Type::BOOL:
            return std::static_pointer_cast<arrow::BooleanScalar>(s)->value;
        case arrow::Type::STRING: {
            auto ss = std::static_pointer_cast<arrow::StringScalar>(s);
            return std::string(reinterpret_cast<const char*>(ss->value->data()),
                               ss->value->size());
        }
        case arrow::Type::LARGE_STRING: {
            auto ss = std::static_pointer_cast<arrow::LargeStringScalar>(s);
            return std::string(reinterpret_cast<const char*>(ss->value->data()),
                               ss->value->size());
        }
        default:
            throw std::runtime_error("to_native: unsupported type: " + s->type->ToString());
    }
}

std::shared_ptr<arrow::Scalar> from_native(const NativeValue& v,
                                             const std::shared_ptr<arrow::DataType>& type) {
    return std::visit([&](auto&& val) -> std::shared_ptr<arrow::Scalar> {
        using T = std::decay_t<decltype(val)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            return arrow::MakeNullScalar(type);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            switch (type->id()) {
                case arrow::Type::INT8:   return std::make_shared<arrow::Int8Scalar>((int8_t)val);
                case arrow::Type::INT16:  return std::make_shared<arrow::Int16Scalar>((int16_t)val);
                case arrow::Type::INT32:  return std::make_shared<arrow::Int32Scalar>((int32_t)val);
                case arrow::Type::INT64:  return std::make_shared<arrow::Int64Scalar>(val);
                case arrow::Type::UINT8:  return std::make_shared<arrow::UInt8Scalar>((uint8_t)val);
                case arrow::Type::UINT16: return std::make_shared<arrow::UInt16Scalar>((uint16_t)val);
                case arrow::Type::UINT32: return std::make_shared<arrow::UInt32Scalar>((uint32_t)val);
                case arrow::Type::UINT64: return std::make_shared<arrow::UInt64Scalar>((uint64_t)val);
                case arrow::Type::FLOAT:  return std::make_shared<arrow::FloatScalar>((float)val);
                case arrow::Type::DOUBLE: return std::make_shared<arrow::DoubleScalar>((double)val);
                default:                  return std::make_shared<arrow::Int64Scalar>(val);
            }
        } else if constexpr (std::is_same_v<T, double>) {
            if (type->id() == arrow::Type::FLOAT)
                return std::make_shared<arrow::FloatScalar>((float)val);
            return std::make_shared<arrow::DoubleScalar>(val);
        } else if constexpr (std::is_same_v<T, std::string>) {
            return arrow::MakeScalar(val);
        } else if constexpr (std::is_same_v<T, bool>) {
            return std::make_shared<arrow::BooleanScalar>(val);
        }
    }, v);
}

// ── Internal Datum helpers ────────────────────────────────────────────────

static std::shared_ptr<arrow::DataType> datum_type(const arrow::Datum& d) {
    if (d.is_scalar())        return d.scalar()->type;
    if (d.is_array())         return d.array()->type;
    if (d.is_chunked_array()) return d.chunked_array()->type();
    throw std::runtime_error("datum_type: unsupported Datum kind");
}

static int64_t datum_length(const arrow::Datum& d) {
    if (d.is_scalar())        return 1;
    if (d.is_array())         return d.array()->length;
    if (d.is_chunked_array()) return d.chunked_array()->length();
    throw std::runtime_error("datum_length: unsupported Datum kind");
}

// Get NativeValue at row i. If d is a Scalar it is broadcast (i is ignored).
static NativeValue datum_at(const arrow::Datum& d, int64_t i) {
    if (d.is_scalar())        return to_native(d.scalar());
    if (d.is_array())         return to_native(d.make_array()->GetScalar(i).ValueOrDie());
    if (d.is_chunked_array()) return to_native(scalar_at(d.chunked_array(), i));
    throw std::runtime_error("datum_at: unsupported Datum kind");
}

// Build a single-chunk ChunkedArray from a vector of NativeValues.
static std::shared_ptr<arrow::ChunkedArray>
build_array(const std::vector<NativeValue>& values,
            const std::shared_ptr<arrow::DataType>& type) {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    auto st = arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder);
    if (!st.ok()) throw std::runtime_error("build_array: " + st.message());
    for (const auto& v : values) {
        st = builder->AppendScalar(*from_native(v, type));
        if (!st.ok()) throw std::runtime_error("build_array append: " + st.message());
    }
    std::shared_ptr<arrow::Array> arr;
    st = builder->Finish(&arr);
    if (!st.ok()) throw std::runtime_error("build_array finish: " + st.message());
    return std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arr});
}

// ── Type inference ────────────────────────────────────────────────────────

static std::shared_ptr<arrow::DataType>
infer_binop_type(const std::shared_ptr<arrow::DataType>& lt,
                 const std::shared_ptr<arrow::DataType>& rt,
                 BinOp op) {
    if (op == BinOp::EQ || op == BinOp::NEQ || op == BinOp::LT  ||
        op == BinOp::LTE || op == BinOp::GT  || op == BinOp::GTE ||
        op == BinOp::AND || op == BinOp::OR)
        return arrow::boolean();

    // Arithmetic
    bool l_float = arrow::is_floating(lt->id());
    bool r_float = arrow::is_floating(rt->id());
    bool l_int   = arrow::is_integer(lt->id());
    bool r_int   = arrow::is_integer(rt->id());

    if (!l_int && !l_float)
        throw std::runtime_error("apply_binop: arithmetic requires numeric types, got " +
                                 lt->ToString());
    if (!r_int && !r_float)
        throw std::runtime_error("apply_binop: arithmetic requires numeric types, got " +
                                 rt->ToString());

    if (l_float || r_float) return arrow::float64();
    return arrow::int64();
}

// ── NativeValue binary operation ─────────────────────────────────────────

static NativeValue apply_native_binop(NativeValue lv, NativeValue rv, BinOp op) {
    // Null propagation
    if (std::holds_alternative<std::monostate>(lv) ||
        std::holds_alternative<std::monostate>(rv))
        return std::monostate{};

    // int64 ↔ double promotion
    if (std::holds_alternative<int64_t>(lv) && std::holds_alternative<double>(rv))
        lv = (double)std::get<int64_t>(lv);
    else if (std::holds_alternative<double>(lv) && std::holds_alternative<int64_t>(rv))
        rv = (double)std::get<int64_t>(rv);

    // Boolean ops
    if (op == BinOp::AND || op == BinOp::OR) {
        if (!std::holds_alternative<bool>(lv) || !std::holds_alternative<bool>(rv))
            throw std::runtime_error("apply_binop: AND/OR requires boolean operands");
        bool l = std::get<bool>(lv), r = std::get<bool>(rv);
        return op == BinOp::AND ? (l && r) : (l || r);
    }

    // Integer operations
    if (std::holds_alternative<int64_t>(lv) && std::holds_alternative<int64_t>(rv)) {
        int64_t l = std::get<int64_t>(lv), r = std::get<int64_t>(rv);
        switch (op) {
            case BinOp::ADD: return l + r;
            case BinOp::SUB: return l - r;
            case BinOp::MUL: return l * r;
            case BinOp::DIV: return r == 0 ? NativeValue{std::monostate{}} : NativeValue{l / r};
            case BinOp::MOD: return r == 0 ? NativeValue{std::monostate{}} : NativeValue{l % r};
            case BinOp::EQ:  return l == r;
            case BinOp::NEQ: return l != r;
            case BinOp::LT:  return l <  r;
            case BinOp::LTE: return l <= r;
            case BinOp::GT:  return l >  r;
            case BinOp::GTE: return l >= r;
            default: break;
        }
    }

    // Double operations
    if (std::holds_alternative<double>(lv) && std::holds_alternative<double>(rv)) {
        double l = std::get<double>(lv), r = std::get<double>(rv);
        switch (op) {
            case BinOp::ADD: return l + r;
            case BinOp::SUB: return l - r;
            case BinOp::MUL: return l * r;
            case BinOp::DIV: return l / r;
            case BinOp::MOD: return std::fmod(l, r);
            case BinOp::EQ:  return l == r;
            case BinOp::NEQ: return l != r;
            case BinOp::LT:  return l <  r;
            case BinOp::LTE: return l <= r;
            case BinOp::GT:  return l >  r;
            case BinOp::GTE: return l >= r;
            default: break;
        }
    }

    // String comparisons
    if (std::holds_alternative<std::string>(lv) && std::holds_alternative<std::string>(rv)) {
        const auto& l = std::get<std::string>(lv);
        const auto& r = std::get<std::string>(rv);
        switch (op) {
            case BinOp::EQ:  return l == r;
            case BinOp::NEQ: return l != r;
            case BinOp::LT:  return l <  r;
            case BinOp::LTE: return l <= r;
            case BinOp::GT:  return l >  r;
            case BinOp::GTE: return l >= r;
            default: throw std::runtime_error("apply_binop: arithmetic not supported on strings");
        }
    }

    // Bool equality
    if (std::holds_alternative<bool>(lv) && std::holds_alternative<bool>(rv)) {
        bool l = std::get<bool>(lv), r = std::get<bool>(rv);
        if (op == BinOp::EQ)  return l == r;
        if (op == BinOp::NEQ) return l != r;
        throw std::runtime_error("apply_binop: unsupported op on bool types");
    }

    throw std::runtime_error("apply_binop: incompatible operand types");
}

// ── NativeValue unary operation ───────────────────────────────────────────

static NativeValue apply_native_unary(const NativeValue& v, UnaryOp op) {
    if (op == UnaryOp::IS_NULL)     return std::holds_alternative<std::monostate>(v);
    if (op == UnaryOp::IS_NOT_NULL) return !std::holds_alternative<std::monostate>(v);
    if (std::holds_alternative<std::monostate>(v)) return v; // null propagation

    switch (op) {
        case UnaryOp::ABS:
            if (std::holds_alternative<int64_t>(v)) return std::abs(std::get<int64_t>(v));
            if (std::holds_alternative<double>(v))  return std::abs(std::get<double>(v));
            throw std::runtime_error("abs: requires numeric type");
        case UnaryOp::NEGATE:
            if (std::holds_alternative<int64_t>(v)) return -std::get<int64_t>(v);
            if (std::holds_alternative<double>(v))  return -std::get<double>(v);
            throw std::runtime_error("negate: requires numeric type");
        case UnaryOp::NOT:
            if (std::holds_alternative<bool>(v)) return !std::get<bool>(v);
            throw std::runtime_error("NOT: requires boolean type");
        default: break;
    }
    throw std::runtime_error("apply_unary: unknown op");
}

// ── Public element-wise functions ─────────────────────────────────────────

arrow::Datum apply_binop(const arrow::Datum& left, const arrow::Datum& right, BinOp op) {
    auto out_type = infer_binop_type(datum_type(left), datum_type(right), op);

    // Both scalars → return a scalar Datum (constant folding at runtime)
    if (left.is_scalar() && right.is_scalar()) {
        auto result = apply_native_binop(to_native(left.scalar()),
                                         to_native(right.scalar()), op);
        return arrow::Datum(from_native(result, out_type));
    }

    int64_t n = left.is_scalar() ? datum_length(right) : datum_length(left);
    std::vector<NativeValue> results;
    results.reserve(n);
    for (int64_t i = 0; i < n; ++i)
        results.push_back(apply_native_binop(datum_at(left, i), datum_at(right, i), op));
    return arrow::Datum(build_array(results, out_type));
}

arrow::Datum apply_unary(const arrow::Datum& input, UnaryOp op) {
    std::shared_ptr<arrow::DataType> out_type;
    if (op == UnaryOp::IS_NULL || op == UnaryOp::IS_NOT_NULL || op == UnaryOp::NOT)
        out_type = arrow::boolean();
    else
        out_type = datum_type(input); // ABS and NEGATE preserve the column type

    if (input.is_scalar()) {
        auto result = apply_native_unary(to_native(input.scalar()), op);
        return arrow::Datum(from_native(result, out_type));
    }

    int64_t n = datum_length(input);
    std::vector<NativeValue> results;
    results.reserve(n);
    for (int64_t i = 0; i < n; ++i)
        results.push_back(apply_native_unary(datum_at(input, i), op));
    return arrow::Datum(build_array(results, out_type));
}

arrow::Datum apply_agg(const arrow::Datum& input, AggType agg) {
    int64_t n       = datum_length(input);
    auto    in_type = datum_type(input);

    if (agg == AggType::COUNT) {
        int64_t count = 0;
        for (int64_t i = 0; i < n; ++i)
            if (!std::holds_alternative<std::monostate>(datum_at(input, i))) ++count;
        return arrow::Datum(std::make_shared<arrow::Int64Scalar>(count));
    }

    if (agg == AggType::SUM || agg == AggType::MEAN) {
        bool    has_double = arrow::is_floating(in_type->id());
        double  dsum = 0.0;
        int64_t isum = 0;
        int64_t cnt  = 0;
        for (int64_t i = 0; i < n; ++i) {
            auto v = datum_at(input, i);
            if (std::holds_alternative<std::monostate>(v)) continue;
            ++cnt;
            if (std::holds_alternative<double>(v)) {
                if (!has_double) { dsum = (double)isum; has_double = true; }
                dsum += std::get<double>(v);
            } else if (std::holds_alternative<int64_t>(v)) {
                if (has_double) dsum += (double)std::get<int64_t>(v);
                else            isum += std::get<int64_t>(v);
            }
        }
        if (cnt == 0)
            return arrow::Datum(arrow::MakeNullScalar(
                agg == AggType::MEAN ? arrow::float64() : in_type));
        if (agg == AggType::MEAN) {
            double result = has_double ? dsum / cnt : (double)isum / cnt;
            return arrow::Datum(std::make_shared<arrow::DoubleScalar>(result));
        }
        // SUM: return int64 for integer input, double for float input
        if (has_double)
            return arrow::Datum(std::make_shared<arrow::DoubleScalar>(dsum));
        return arrow::Datum(from_native(NativeValue{isum}, arrow::int64()));
    }

    if (agg == AggType::MIN || agg == AggType::MAX) {
        std::optional<NativeValue> best;
        for (int64_t i = 0; i < n; ++i) {
            auto v = datum_at(input, i);
            if (std::holds_alternative<std::monostate>(v)) continue;
            if (!best) { best = v; continue; }

            // Promote int64/double for comparison
            NativeValue bv = *best, cv = v;
            if (std::holds_alternative<int64_t>(bv) && std::holds_alternative<double>(cv))
                bv = (double)std::get<int64_t>(bv);
            else if (std::holds_alternative<double>(bv) && std::holds_alternative<int64_t>(cv))
                cv = (double)std::get<int64_t>(cv);

            bool update = false;
            if (std::holds_alternative<int64_t>(bv) && std::holds_alternative<int64_t>(cv)) {
                int64_t a = std::get<int64_t>(bv), b = std::get<int64_t>(cv);
                update = agg == AggType::MIN ? b < a : b > a;
            } else if (std::holds_alternative<double>(bv) && std::holds_alternative<double>(cv)) {
                double a = std::get<double>(bv), b = std::get<double>(cv);
                update = agg == AggType::MIN ? b < a : b > a;
            } else if (std::holds_alternative<std::string>(bv) && std::holds_alternative<std::string>(cv)) {
                update = agg == AggType::MIN
                    ? std::get<std::string>(cv) < std::get<std::string>(bv)
                    : std::get<std::string>(cv) > std::get<std::string>(bv);
            }
            if (update) best = v;
        }
        if (!best) return arrow::Datum(arrow::MakeNullScalar(in_type));
        return arrow::Datum(from_native(*best, in_type));
    }

    throw std::runtime_error("apply_agg: unknown AggType");
}

arrow::Datum apply_strfunc(const arrow::Datum& input, StrFunc func, const std::string& arg) {
    auto in_type = datum_type(input);
    std::shared_ptr<arrow::DataType> out_type;
    switch (func) {
        case StrFunc::LENGTH:      out_type = arrow::int64();   break;
        case StrFunc::TO_LOWER:
        case StrFunc::TO_UPPER:    out_type = in_type;          break; // preserve string type
        case StrFunc::CONTAINS:
        case StrFunc::STARTS_WITH:
        case StrFunc::ENDS_WITH:   out_type = arrow::boolean(); break;
    }

    auto transform = [&](const NativeValue& v) -> NativeValue {
        if (std::holds_alternative<std::monostate>(v)) return v;
        if (!std::holds_alternative<std::string>(v))
            throw std::runtime_error("apply_strfunc: requires a string column");
        const auto& s = std::get<std::string>(v);
        switch (func) {
            case StrFunc::LENGTH:
                return (int64_t)s.size();
            case StrFunc::TO_LOWER: {
                std::string r = s;
                std::transform(r.begin(), r.end(), r.begin(),
                               [](unsigned char c) { return (char)std::tolower(c); });
                return r;
            }
            case StrFunc::TO_UPPER: {
                std::string r = s;
                std::transform(r.begin(), r.end(), r.begin(),
                               [](unsigned char c) { return (char)std::toupper(c); });
                return r;
            }
            case StrFunc::CONTAINS:
                return s.find(arg) != std::string::npos;
            case StrFunc::STARTS_WITH:
                return s.size() >= arg.size() && s.substr(0, arg.size()) == arg;
            case StrFunc::ENDS_WITH:
                return s.size() >= arg.size() &&
                       s.substr(s.size() - arg.size()) == arg;
        }
        throw std::runtime_error("apply_strfunc: unknown func");
    };

    if (input.is_scalar()) {
        return arrow::Datum(from_native(transform(to_native(input.scalar())), out_type));
    }

    int64_t n = datum_length(input);
    std::vector<NativeValue> results;
    results.reserve(n);
    for (int64_t i = 0; i < n; ++i)
        results.push_back(transform(datum_at(input, i)));
    return arrow::Datum(build_array(results, out_type));
}

// ── Table helpers ─────────────────────────────────────────────────────────

static std::shared_ptr<arrow::Array>
take_column(const std::shared_ptr<arrow::ChunkedArray>& col,
            const std::vector<int64_t>& indices) {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    auto st = arrow::MakeBuilder(arrow::default_memory_pool(), col->type(), &builder);
    if (!st.ok()) throw std::runtime_error("take_column: " + st.message());
    for (int64_t idx : indices) {
        st = builder->AppendScalar(*scalar_at(col, idx));
        if (!st.ok()) throw std::runtime_error("take_column append: " + st.message());
    }
    std::shared_ptr<arrow::Array> arr;
    st = builder->Finish(&arr);
    if (!st.ok()) throw std::runtime_error("take_column finish: " + st.message());
    return arr;
}

std::shared_ptr<arrow::Table>
take_rows(const std::shared_ptr<arrow::Table>& table,
          const std::vector<int64_t>& indices) {
    std::vector<std::shared_ptr<arrow::Field>>        fields;
    std::vector<std::shared_ptr<arrow::ChunkedArray>> arrays;
    fields.reserve(table->num_columns());
    arrays.reserve(table->num_columns());
    for (int c = 0; c < table->num_columns(); ++c) {
        fields.push_back(table->schema()->field(c));
        auto arr = take_column(table->column(c), indices);
        arrays.push_back(std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arr}));
    }
    return arrow::Table::Make(arrow::schema(fields), arrays);
}

std::vector<int64_t>
sort_indices(const std::shared_ptr<arrow::Table>& table,
             const std::vector<std::string>& columns,
             const std::vector<bool>& ascending) {
    int64_t n = table->num_rows();
    std::vector<int64_t> indices(n);
    std::iota(indices.begin(), indices.end(), 0);

    std::vector<std::shared_ptr<arrow::ChunkedArray>> sort_cols;
    sort_cols.reserve(columns.size());
    for (const auto& name : columns) {
        auto col = table->GetColumnByName(name);
        if (!col) throw std::runtime_error("sort: column not found: \"" + name + "\"");
        sort_cols.push_back(col);
    }

    std::stable_sort(indices.begin(), indices.end(), [&](int64_t a, int64_t b) {
        for (size_t k = 0; k < sort_cols.size(); ++k) {
            auto av = to_native(scalar_at(sort_cols[k], a));
            auto bv = to_native(scalar_at(sort_cols[k], b));

            // Nulls sort last
            bool a_null = std::holds_alternative<std::monostate>(av);
            bool b_null = std::holds_alternative<std::monostate>(bv);
            if (a_null && b_null) continue;
            if (a_null) return !(bool)ascending[k]; // null is "big" → last in ASC
            if (b_null) return  (bool)ascending[k];

            // Promote int/double
            if (std::holds_alternative<int64_t>(av) && std::holds_alternative<double>(bv))
                av = (double)std::get<int64_t>(av);
            else if (std::holds_alternative<double>(av) && std::holds_alternative<int64_t>(bv))
                bv = (double)std::get<int64_t>(bv);

            int cmp = 0;
            if (std::holds_alternative<int64_t>(av) && std::holds_alternative<int64_t>(bv)) {
                int64_t ia = std::get<int64_t>(av), ib = std::get<int64_t>(bv);
                cmp = ia < ib ? -1 : ia > ib ? 1 : 0;
            } else if (std::holds_alternative<double>(av) && std::holds_alternative<double>(bv)) {
                double da = std::get<double>(av), db = std::get<double>(bv);
                cmp = da < db ? -1 : da > db ? 1 : 0;
            } else if (std::holds_alternative<std::string>(av) && std::holds_alternative<std::string>(bv)) {
                cmp = std::get<std::string>(av).compare(std::get<std::string>(bv));
            } else if (std::holds_alternative<bool>(av) && std::holds_alternative<bool>(bv)) {
                bool ba = std::get<bool>(av), bb = std::get<bool>(bv);
                cmp = ba == bb ? 0 : (!ba ? -1 : 1);
            }

            if (cmp != 0) return ascending[k] ? cmp < 0 : cmp > 0;
        }
        return false; // all keys equal
    });

    return indices;
}

std::vector<int64_t> filter_indices(const arrow::Datum& mask) {
    // Scalar mask is handled at the call site in EagerDataFrame::filter.
    if (!mask.is_array() && !mask.is_chunked_array())
        throw std::runtime_error("filter_indices: mask must be an array, not a scalar");

    int64_t n = datum_length(mask);
    std::vector<int64_t> indices;
    indices.reserve(n);
    for (int64_t i = 0; i < n; ++i) {
        auto v = datum_at(mask, i);
        if (std::holds_alternative<bool>(v) && std::get<bool>(v))
            indices.push_back(i);
    }
    return indices;
}

} // namespace compute
} // namespace dataframelib
