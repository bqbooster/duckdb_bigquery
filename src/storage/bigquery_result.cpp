#include "duckdb.hpp"

#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"
#include <arrow/api.h>
#include <arrow/array/data.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include "bigquery_result.hpp"

namespace bigquery_storage = ::google::cloud::bigquery_storage_v1;
namespace bigquery_storage_read = ::google::cloud::bigquery::storage::v1;

namespace duckdb {

std::shared_ptr<arrow::RecordBatch> BigQueryResult::GetArrowRecordBatch(
    ::google::cloud::bigquery::storage::v1::ArrowRecordBatch const&
        record_batch_in,
    std::shared_ptr<arrow::Schema> schema) {
  std::shared_ptr<arrow::Buffer> buffer = std::make_shared<arrow::Buffer>(
      record_batch_in.serialized_record_batch());
  arrow::io::BufferReader buffer_reader(buffer);
  arrow::ipc::DictionaryMemo dictionary_memo;
  arrow::ipc::IpcReadOptions read_options;
  auto result = arrow::ipc::ReadRecordBatch(schema, &dictionary_memo,
                                            read_options, &buffer_reader);
  if (!result.ok()) {
    std::cout << "Unable to parse record batch\n";
    throw result.status();
  }
  std::shared_ptr<arrow::RecordBatch> record_batch = result.ValueOrDie();
  return record_batch;
}

void BigQueryResult::PrintColumnNames(std::shared_ptr<arrow::RecordBatch> record_batch) {
  // Print each column name for the record batch.
  std::cout << std::setfill(' ') << std::setw(7) << "";
  for (std::int64_t col = 0; col < record_batch->num_columns(); ++col) {
    std::cout << std::left << std::setw(16) << record_batch->column_name(col);
  }
  std::cout << "\n";
}

void BigQueryResult::ProcessRecordBatch(std::shared_ptr<arrow::Schema> schema,
                        std::shared_ptr<arrow::RecordBatch> record_batch,
                        std::int64_t num_rows) {
  // If you want to see what the result looks like without parsing the
  // datatypes, use `record_batch->ToString()` for quick debugging.
  // Note: you might need to adjust the formatting depending on how big the data
  // in your table is.
  for (std::int64_t row = 0; row < record_batch->num_rows(); ++row) {
    Printer::Print("Row: " + std::to_string(num_rows + row));

    for (std::int64_t col = 0; col < record_batch->num_columns(); ++col) {
      std::shared_ptr<arrow::Array> column = record_batch->column(col);
      arrow::Result<std::shared_ptr<arrow::Scalar> > result =
          column->GetScalar(row);
      if (!result.ok()) {
        std::cout << "Unable to parse scalar\n";
        throw result.status();
      }

      std::shared_ptr<arrow::Scalar> scalar = result.ValueOrDie();
      switch (scalar->type->id()) {
        case arrow::Type::INT64:
          std::cout
              << std::left << std::setw(15)
              << std::dynamic_pointer_cast<arrow::Int64Scalar>(scalar)->value
              << " ";
          break;
        case arrow::Type::STRING:
          std::cout
              << std::left << std::setw(15)
              << std::dynamic_pointer_cast<arrow::StringScalar>(scalar)->view()
              << " ";
          break;
        // Depending on the table you are reading, you might need to add cases
        // for other datatypes here. The schema will tell you what datatypes
        // need to be handled.
        default:
          std::cout << std::left << std::setw(15) << "UNDEFINED ";
      }
    }
    std::cout << "\n";
  }
}

void BigQueryResult::process(){
//   Printer::Print("BigQueryResult");
//   // Get schema.
//   std::shared_ptr<arrow::Schema> schema =
//       GetArrowSchema(read_session->arrow_schema());

//   // Read rows from the ReadSession.
//   constexpr int kRowOffset = 0;
//   auto read_rows = client->ReadRows(read_session->streams(0).name(), kRowOffset);

//   std::int64_t num_rows = 0;
//   std::int64_t record_batch_count = 0;
//   for (auto const& read_rows_response : read_rows) {
//     if (read_rows_response.ok()) {
//       std::shared_ptr<arrow::RecordBatch> record_batch =
//           GetArrowRecordBatch(read_rows_response->arrow_record_batch(), schema);

//       if (record_batch_count == 0) {
//         PrintColumnNames(record_batch);
//       }

//       ProcessRecordBatch(schema, record_batch, num_rows);
//       num_rows += read_rows_response->row_count();
//       ++record_batch_count;
//     }
//   }
  return;
}

} // namespace duckdb
