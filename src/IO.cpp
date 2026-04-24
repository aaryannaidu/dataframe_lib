#include "IO.hpp"

#include <arrow/csv/api.h>
#include <arrow/csv/writer.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <stdexcept>

namespace dataframelib {
namespace io {

// Read a CSV file into an Arrow Table.
std::shared_ptr<arrow::Table> read_csv(const std::string &path) {
  auto file = arrow::io::ReadableFile::Open(path);
  if (!file.ok())
    throw std::runtime_error("read_csv: cannot open \"" + path +
                             "\": " + file.status().message());

  auto reader = arrow::csv::TableReader::Make(
      arrow::io::default_io_context(), file.ValueOrDie(),
      arrow::csv::ReadOptions::Defaults(), arrow::csv::ParseOptions::Defaults(),
      arrow::csv::ConvertOptions::Defaults());
  if (!reader.ok())
    throw std::runtime_error("read_csv: failed to create reader: " +
                             reader.status().message());

  auto table = reader.ValueOrDie()->Read();
  if (!table.ok())
    throw std::runtime_error("read_csv: failed to read \"" + path +
                             "\": " + table.status().message());

  return table.ValueOrDie();
}

// Read a Parquet file into an Arrow Table.
std::shared_ptr<arrow::Table> read_parquet(const std::string &path) {
  auto file = arrow::io::ReadableFile::Open(path);
  if (!file.ok())
    throw std::runtime_error("read_parquet: cannot open \"" + path +
                             "\": " + file.status().message());

  auto reader_result =
      parquet::arrow::OpenFile(file.ValueOrDie(), arrow::default_memory_pool());
  if (!reader_result.ok())
    throw std::runtime_error("read_parquet: failed to open \"" + path +
                             "\": " + reader_result.status().message());
  auto reader = std::move(reader_result).ValueOrDie();

  std::shared_ptr<arrow::Table> table;
  auto status = reader->ReadTable(&table);
  if (!status.ok())
    throw std::runtime_error("read_parquet: failed to read \"" + path +
                             "\": " + status.message());

  return table;
}

// Write an Arrow Table to a CSV file.
void write_csv(const std::shared_ptr<arrow::Table> &table,
               const std::string &path) {
  auto file = arrow::io::FileOutputStream::Open(path);
  if (!file.ok())
    throw std::runtime_error("write_csv: cannot open \"" + path +
                             "\": " + file.status().message());

  auto options = arrow::csv::WriteOptions::Defaults();
  auto status = arrow::csv::WriteCSV(*table, options, file.ValueOrDie().get());
  if (!status.ok())
    throw std::runtime_error("write_csv: failed to write \"" + path +
                             "\": " + status.message());
}

// Write an Arrow Table to a Parquet file.
void write_parquet(const std::shared_ptr<arrow::Table> &table,
                   const std::string &path) {
  auto file = arrow::io::FileOutputStream::Open(path);
  if (!file.ok())
    throw std::runtime_error("write_parquet: cannot open \"" + path +
                             "\": " + file.status().message());

  auto props = parquet::WriterProperties::Builder().build();
  auto status =
      parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                 file.ValueOrDie(), table->num_rows(), props);
  if (!status.ok())
    throw std::runtime_error("write_parquet: failed to write \"" + path +
                             "\": " + status.message());
}

// Build an Arrow Table from a name→array map; column order follows map
// insertion order.
std::shared_ptr<arrow::Table> from_columns(
    const std::map<std::string, std::shared_ptr<arrow::Array>> &columns) {
  if (columns.empty())
    throw std::runtime_error("from_columns: column map is empty");

  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::Array>> arrays;

  int64_t expected_rows = -1;
  for (const auto &[name, arr] : columns) {
    if (expected_rows == -1)
      expected_rows = arr->length();
    else if (arr->length() != expected_rows)
      throw std::runtime_error("from_columns: column \"" + name + "\" has " +
                               std::to_string(arr->length()) +
                               " rows, expected " +
                               std::to_string(expected_rows));
    fields.push_back(arrow::field(name, arr->type()));
    arrays.push_back(arr);
  }

  return arrow::Table::Make(arrow::schema(fields), arrays);
}

}
} 
