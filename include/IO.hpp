#pragma once

#include <arrow/api.h>
#include <map>
#include <memory>
#include <string>

namespace dataframelib {
namespace io {

// Read a CSV file into an Arrow Table.
std::shared_ptr<arrow::Table> read_csv(const std::string &path);

// Read a Parquet file into an Arrow Table.
std::shared_ptr<arrow::Table> read_parquet(const std::string &path);

// Write an Arrow Table to a CSV file.
void write_csv(const std::shared_ptr<arrow::Table> &table,
               const std::string &path);

// Write an Arrow Table to a Parquet file.
void write_parquet(const std::shared_ptr<arrow::Table> &table,
                   const std::string &path);

// Build an Arrow Table from a name→array map; column order follows map
// insertion order.
std::shared_ptr<arrow::Table> from_columns(
    const std::map<std::string, std::shared_ptr<arrow::Array>> &columns);

} 
} 
