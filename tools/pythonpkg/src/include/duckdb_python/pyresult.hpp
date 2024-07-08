//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyresult.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/numpy/numpy_result_conversion.hpp"
#include "duckdb.hpp"
#include "duckdb_python/nanobind/nb_wrapper.hpp"
#include "duckdb_python/python_objects.hpp"
#include "duckdb_python/nanobind/dataframe.hpp"

namespace duckdb {

struct DuckDBPyResult {
public:
	explicit DuckDBPyResult(unique_ptr<QueryResult> result);
	~DuckDBPyResult();

public:
	Optional<nb::tuple> Fetchone();

	nb::list Fetchmany(idx_t size);

	nb::list Fetchall();

	nb::dict FetchNumpy();

	nb::dict FetchNumpyInternal(bool stream = false, idx_t vectors_per_chunk = 1,
	                            unique_ptr<NumpyResultConversion> conversion = nullptr);

	PandasDataFrame FetchDF(bool date_as_object);

	duckdb::pyarrow::Table FetchArrowTable(idx_t rows_per_batch, bool to_polars);

	PandasDataFrame FetchDFChunk(const idx_t vectors_per_chunk = 1, bool date_as_object = false);

	nb::dict FetchPyTorch();

	nb::dict FetchTF();

	duckdb::pyarrow::RecordBatchReader FetchRecordBatchReader(idx_t rows_per_batch);

	static nb::list GetDescription(const vector<string> &names, const vector<LogicalType> &types);

	void Close();

	bool IsClosed() const;

	unique_ptr<DataChunk> FetchChunk();

	const vector<string> &GetNames();
	const vector<LogicalType> &GetTypes();

private:
	nb::list FetchAllArrowChunks(idx_t rows_per_batch, bool to_polars);

	void FillNumpy(nb::dict &res, idx_t col_idx, NumpyResultConversion &conversion, const char *name);

	bool FetchArrowChunk(ChunkScanState &scan_state, nb::list &batches, idx_t rows_per_batch, bool to_polars);

	PandasDataFrame FrameFromNumpy(bool date_as_object, const nb::handle &o);

	void ChangeToTZType(PandasDataFrame &df);
	void ChangeDateToDatetime(PandasDataFrame &df);
	unique_ptr<DataChunk> FetchNext(QueryResult &result);
	unique_ptr<DataChunk> FetchNextRaw(QueryResult &result);
	unique_ptr<NumpyResultConversion> InitializeNumpyConversion(bool pandas = false);

private:
	idx_t chunk_offset = 0;

	unique_ptr<QueryResult> result;
	unique_ptr<DataChunk> current_chunk;
	// Holds the categories of Categorical/ENUM types
	unordered_map<idx_t, nb::list> categories;
	// Holds the categorical type of Categorical/ENUM types
	unordered_map<idx_t, nb::object> categories_type;
	bool result_closed = false;
};

} // namespace duckdb
