//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/arrow/arrow_array_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_properties.hpp"

namespace duckdb {

namespace pyarrow {

class RecordBatchReader : public nb::object {
public:
	RecordBatchReader(const nb::object &o) : nb::object(o, nb::detail::borrow_t {}) {
	}
	using nb::object::object;

public:
	static bool check_(const nb::handle &object) {
		return !nb::none().is(object);
	}
};
class Table : public nb::object {
public:
	Table(const nb::object &o) : nb::object(o, nb::detail::borrow_t {}) {
	}
	using nb::object::object;

public:
	static bool check_(const nb::handle &object) {
		return !nb::none().is(object);
	}
};

} // namespace pyarrow

enum class PyArrowObjectType { Invalid, Table, RecordBatchReader, Scanner, Dataset };

void TransformDuckToArrowChunk(ArrowSchema &arrow_schema, ArrowArray &data, nb::list &batches);

PyArrowObjectType GetArrowType(const nb::handle &obj);

class PythonTableArrowArrayStreamFactory {
public:
	explicit PythonTableArrowArrayStreamFactory(PyObject *arrow_table, const ClientProperties &client_properties_p)
	    : arrow_object(arrow_table), client_properties(client_properties_p) {};

	//! Produces an Arrow Scanner, should be only called once when initializing Scan States
	static unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory, ArrowStreamParameters &parameters);

	//! Get the schema of the arrow object
	static void GetSchemaInternal(nb::handle arrow_object, ArrowSchemaWrapper &schema);
	static void GetSchema(uintptr_t factory_ptr, ArrowSchemaWrapper &schema);

	//! Arrow Object (i.e., Scanner, Record Batch Reader, Table, Dataset)
	PyObject *arrow_object;

	const ClientProperties client_properties;

private:
	//! We transform a TableFilterSet to an Arrow Expression Object
	static nb::object TransformFilter(TableFilterSet &filters, std::unordered_map<idx_t, string> &columns,
	                                  unordered_map<idx_t, idx_t> filter_to_col,
	                                  const ClientProperties &client_properties, const ArrowTableType &arrow_table);

	static nb::object ProduceScanner(nb::object &arrow_scanner, nb::handle &arrow_obj_handle,
	                                 ArrowStreamParameters &parameters, const ClientProperties &client_properties);
};
} // namespace duckdb
