#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb_python/python_objects.hpp"

#include "duckdb_python/arrow/arrow_array_stream.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb_python/arrow/arrow_export_utils.hpp"
#include "duckdb/main/chunk_scan_state/query_result.hpp"

namespace duckdb {

DuckDBPyResult::DuckDBPyResult(unique_ptr<QueryResult> result_p) : result(std::move(result_p)) {
	if (!result) {
		throw InternalException("PyResult created without a result object");
	}
}

DuckDBPyResult::~DuckDBPyResult() {
	try {
		nb::gil_scoped_release gil;
		result.reset();
		current_chunk.reset();
	} catch (...) { // NOLINT
	}
}

const vector<string> &DuckDBPyResult::GetNames() {
	if (!result) {
		throw InternalException("Calling GetNames without a result object");
	}
	return result->names;
}

const vector<LogicalType> &DuckDBPyResult::GetTypes() {
	if (!result) {
		throw InternalException("Calling GetTypes without a result object");
	}
	return result->types;
}

unique_ptr<DataChunk> DuckDBPyResult::FetchChunk() {
	if (!result) {
		throw InternalException("FetchChunk called without a result object");
	}
	return FetchNext(*result);
}

unique_ptr<DataChunk> DuckDBPyResult::FetchNext(QueryResult &query_result) {
	if (!result_closed && query_result.type == QueryResultType::STREAM_RESULT &&
	    !query_result.Cast<StreamQueryResult>().IsOpen()) {
		result_closed = true;
		return nullptr;
	}
	auto chunk = query_result.Fetch();
	if (query_result.HasError()) {
		query_result.ThrowError();
	}
	return chunk;
}

unique_ptr<DataChunk> DuckDBPyResult::FetchNextRaw(QueryResult &query_result) {
	if (!result_closed && query_result.type == QueryResultType::STREAM_RESULT &&
	    !query_result.Cast<StreamQueryResult>().IsOpen()) {
		result_closed = true;
		return nullptr;
	}
	auto chunk = query_result.FetchRaw();
	if (query_result.HasError()) {
		query_result.ThrowError();
	}
	return chunk;
}

Optional<nb::tuple> DuckDBPyResult::Fetchone() {
	{
		nb::gil_scoped_release release;
		if (!result) {
			throw InvalidInputException("result closed");
		}
		if (!current_chunk || chunk_offset >= current_chunk->size()) {
			current_chunk = FetchNext(*result);
			chunk_offset = 0;
		}
	}

	if (!current_chunk || current_chunk->size() == 0) {
		return nb::none();
	}
	nb::tuple res(result->types.size());

	for (idx_t col_idx = 0; col_idx < result->types.size(); col_idx++) {
		auto &mask = FlatVector::Validity(current_chunk->data[col_idx]);
		if (!mask.RowIsValid(chunk_offset)) {
			res[col_idx] = nb::none();
			continue;
		}
		auto val = current_chunk->data[col_idx].GetValue(chunk_offset);
		res[col_idx] = PythonObject::FromValue(val, result->types[col_idx], result->client_properties);
	}
	chunk_offset++;
	return res;
}

nb::list DuckDBPyResult::Fetchmany(idx_t size) {
	nb::list res;
	for (idx_t i = 0; i < size; i++) {
		auto fres = Fetchone();
		if (fres.is_none()) {
			break;
		}
		res.append(fres);
	}
	return res;
}

nb::list DuckDBPyResult::Fetchall() {
	nb::list res;
	while (true) {
		auto fres = Fetchone();
		if (fres.is_none()) {
			break;
		}
		res.append(fres);
	}
	return res;
}

nb::dict DuckDBPyResult::FetchNumpy() {
	return FetchNumpyInternal();
}

void DuckDBPyResult::FillNumpy(nb::dict &res, idx_t col_idx, NumpyResultConversion &conversion, const char *name) {
	if (result->types[col_idx].id() == LogicalTypeId::ENUM) {
		// first we (might) need to create the categorical type
		if (categories_type.find(col_idx) == categories_type.end()) {
			// Equivalent to: pandas.CategoricalDtype(['a', 'b'], ordered=True)
			categories_type[col_idx] = nb::module_::import_("pandas").attr("CategoricalDtype")(categories[col_idx], true);
		}
		// Equivalent to: pandas.Categorical.from_codes(codes=[0, 1, 0, 1], dtype=dtype)
		res[name] = py::module_::import_("pandas")
		                .attr("Categorical")
		                .attr("from_codes")(conversion.ToArray(col_idx), nb::arg("dtype") = categories_type[col_idx]);
	} else {
		res[name] = conversion.ToArray(col_idx);
	}
}

void InsertCategory(QueryResult &result, unordered_map<idx_t, nb::list> &categories) {
	for (idx_t col_idx = 0; col_idx < result.types.size(); col_idx++) {
		auto &type = result.types[col_idx];
		if (type.id() == LogicalTypeId::ENUM) {
			// It's an ENUM type, in addition to converting the codes we must convert the categories
			if (categories.find(col_idx) == categories.end()) {
				auto &categories_list = EnumType::GetValuesInsertOrder(type);
				auto categories_size = EnumType::GetSize(type);
				for (idx_t i = 0; i < categories_size; i++) {
					categories[col_idx].append(nb::cast(categories_list.GetValue(i).ToString()));
				}
			}
		}
	}
}

unique_ptr<NumpyResultConversion> DuckDBPyResult::InitializeNumpyConversion(bool pandas) {
	if (!result) {
		throw InvalidInputException("result closed");
	}

	idx_t initial_capacity = STANDARD_VECTOR_SIZE * 2ULL;
	if (result->type == QueryResultType::MATERIALIZED_RESULT) {
		// materialized query result: we know exactly how much space we need
		auto &materialized = result->Cast<MaterializedQueryResult>();
		initial_capacity = materialized.RowCount();
	}

	auto conversion =
	    make_uniq<NumpyResultConversion>(result->types, initial_capacity, result->client_properties, pandas);
	return conversion;
}

nb::dict DuckDBPyResult::FetchNumpyInternal(bool stream, idx_t vectors_per_chunk,
                                            unique_ptr<NumpyResultConversion> conversion_p) {
	if (!result) {
		throw InvalidInputException("result closed");
	}
	if (!conversion_p) {
		conversion_p = InitializeNumpyConversion();
	}
	auto &conversion = *conversion_p;

	if (result->type == QueryResultType::MATERIALIZED_RESULT) {
		auto &materialized = result->Cast<MaterializedQueryResult>();
		for (auto &chunk : materialized.Collection().Chunks()) {
			conversion.Append(chunk);
		}
		InsertCategory(materialized, categories);
		materialized.Collection().Reset();
	} else {
		D_ASSERT(result->type == QueryResultType::STREAM_RESULT);
		if (!stream) {
			vectors_per_chunk = NumericLimits<idx_t>::Maximum();
		}
		auto &stream_result = result->Cast<StreamQueryResult>();
		for (idx_t count_vec = 0; count_vec < vectors_per_chunk; count_vec++) {
			if (!stream_result.IsOpen()) {
				break;
			}
			unique_ptr<DataChunk> chunk;
			{
				nb::gil_scoped_release release;
				chunk = FetchNextRaw(stream_result);
			}
			if (!chunk || chunk->size() == 0) {
				//! finished
				break;
			}
			conversion.Append(*chunk);
			InsertCategory(stream_result, categories);
		}
	}

	// now that we have materialized the result in contiguous arrays, construct the actual NumPy arrays or categorical
	// types
	nb::dict res;
	auto names = result->names;
	QueryResult::DeduplicateColumns(names);
	for (idx_t col_idx = 0; col_idx < result->names.size(); col_idx++) {
		auto &name = names[col_idx];
		FillNumpy(res, col_idx, conversion, name.c_str());
	}
	return res;
}

// TODO: unify these with an enum/flag to indicate which conversions to do
void DuckDBPyResult::ChangeToTZType(PandasDataFrame &df) {
	auto names = df.attr("columns").cast<vector<string>>();

	for (idx_t i = 0; i < result->ColumnCount(); i++) {
		if (result->types[i] == LogicalType::TIMESTAMP_TZ) {
			// first localize to UTC then convert to timezone_config
			auto utc_local = df[names[i].c_str()].attr("dt").attr("tz_localize")("UTC");
			df.attr("__setitem__")(names[i].c_str(),
			                       utc_local.attr("dt").attr("tz_convert")(result->client_properties.time_zone));
		}
	}
}

// TODO: unify these with an enum/flag to indicate which conversions to perform
void DuckDBPyResult::ChangeDateToDatetime(PandasDataFrame &df) {
	auto names = df.attr("columns").cast<vector<string>>();

	for (idx_t i = 0; i < result->ColumnCount(); i++) {
		if (result->types[i] == LogicalType::DATE) {
			df.attr("__setitem__")(names[i].c_str(), df[names[i].c_str()].attr("dt").attr("date"));
		}
	}
}

PandasDataFrame DuckDBPyResult::FrameFromNumpy(bool date_as_object, const nb::handle &o) {
	PandasDataFrame df = nb::cast<PandasDataFrame>(nb::module_::import_("pandas").attr("DataFrame").attr("from_dict")(o));
	// Unfortunately we have to do a type change here for timezones since these types are not supported by numpy
	ChangeToTZType(df);
	if (date_as_object) {
		ChangeDateToDatetime(df);
	}
	return df;
}

PandasDataFrame DuckDBPyResult::FetchDF(bool date_as_object) {
	auto conversion = InitializeNumpyConversion(true);
	return FrameFromNumpy(date_as_object, FetchNumpyInternal(false, 1, std::move(conversion)));
}

PandasDataFrame DuckDBPyResult::FetchDFChunk(idx_t num_of_vectors, bool date_as_object) {
	auto conversion = InitializeNumpyConversion(true);
	return FrameFromNumpy(date_as_object, FetchNumpyInternal(true, num_of_vectors, std::move(conversion)));
}

nb::dict DuckDBPyResult::FetchPyTorch() {
	auto result_dict = FetchNumpyInternal();
	auto from_numpy = nb::module_::import_("torch").attr("from_numpy");
	for (auto &item : result_dict) {
		result_dict[item.first] = from_numpy(item.second);
	}
	return result_dict;
}

nb::dict DuckDBPyResult::FetchTF() {
	auto result_dict = FetchNumpyInternal();
	auto convert_to_tensor = nb::module_::import_("tensorflow").attr("convert_to_tensor");
	for (auto &item : result_dict) {
		result_dict[item.first] = convert_to_tensor(item.second);
	}
	return result_dict;
}

bool DuckDBPyResult::FetchArrowChunk(ChunkScanState &scan_state, nb::list &batches, idx_t rows_per_batch,
                                     bool to_polars) {
	ArrowArray data;
	idx_t count;
	auto &query_result = *result.get();
	{
		nb::gil_scoped_release release;
		count = ArrowUtil::FetchChunk(scan_state, query_result.client_properties, rows_per_batch, &data);
	}
	if (count == 0) {
		return false;
	}
	ArrowSchema arrow_schema;
	auto names = query_result.names;
	if (to_polars) {
		QueryResult::DeduplicateColumns(names);
	}
	ArrowConverter::ToArrowSchema(&arrow_schema, query_result.types, names, query_result.client_properties);
	TransformDuckToArrowChunk(arrow_schema, data, batches);
	return true;
}

nb::list DuckDBPyResult::FetchAllArrowChunks(idx_t rows_per_batch, bool to_polars) {
	if (!result) {
		throw InvalidInputException("result closed");
	}
	auto pyarrow_lib_module = nb::module_::import_("pyarrow").attr("lib");

	nb::list batches;
	QueryResultChunkScanState scan_state(*result.get());
	while (FetchArrowChunk(scan_state, batches, rows_per_batch, to_polars)) {
	}
	return batches;
}

duckdb::pyarrow::Table DuckDBPyResult::FetchArrowTable(idx_t rows_per_batch, bool to_polars) {
	if (!result) {
		throw InvalidInputException("There is no query result");
	}
	auto names = result->names;
	if (to_polars) {
		QueryResult::DeduplicateColumns(names);
	}
	return pyarrow::ToArrowTable(result->types, names, FetchAllArrowChunks(rows_per_batch, to_polars),
	                             result->client_properties);
}

duckdb::pyarrow::RecordBatchReader DuckDBPyResult::FetchRecordBatchReader(idx_t rows_per_batch) {
	if (!result) {
		throw InvalidInputException("There is no query result");
	}
	nb::gil_scoped_acquire acquire;
	auto pyarrow_lib_module = nb::module_::import_("pyarrow").attr("lib");
	auto record_batch_reader_func = pyarrow_lib_module.attr("RecordBatchReader").attr("_import_from_c");
	//! We have to construct an Arrow Array Stream
	ResultArrowArrayStreamWrapper *result_stream = new ResultArrowArrayStreamWrapper(std::move(result), rows_per_batch);
	nb::object record_batch_reader = record_batch_reader_func((uint64_t)&result_stream->stream); // NOLINT
	return nb::cast<duckdb::pyarrow::RecordBatchReader>(record_batch_reader);
}

nb::str GetTypeToPython(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return nb::str("bool");
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL: {
		return nb::str("NUMBER");
	}
	case LogicalTypeId::VARCHAR: {
		if (type.HasAlias() && type.GetAlias() == "JSON") {
			return nb::str("JSON");
		} else {
			return nb::str("STRING");
		}
	}
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT:
		return nb::str("BINARY");
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC: {
		return nb::str("DATETIME");
	}
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ: {
		return nb::str("Time");
	}
	case LogicalTypeId::DATE: {
		return nb::str("Date");
	}
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
		return nb::str("dict");
	case LogicalTypeId::LIST: {
		return nb::str("list");
	}
	case LogicalTypeId::INTERVAL: {
		return nb::str("TIMEDELTA");
	}
	case LogicalTypeId::UUID: {
		return nb::str("UUID");
	}
	default:
		return nb::str(type.ToString());
	}
}

nb::list DuckDBPyResult::GetDescription(const vector<string> &names, const vector<LogicalType> &types) {
	nb::list desc;

	for (idx_t col_idx = 0; col_idx < names.size(); col_idx++) {
		auto py_name = nb::str(names[col_idx]);
		auto py_type = GetTypeToPython(types[col_idx]);
		desc.append(nb::make_tuple(py_name, py_type, nb::none(), nb::none(), nb::none(), nb::none(), nb::none()));
	}
	return desc;
}

void DuckDBPyResult::Close() {
	result = nullptr;
}

bool DuckDBPyResult::IsClosed() const {
	return result_closed;
}

} // namespace duckdb
