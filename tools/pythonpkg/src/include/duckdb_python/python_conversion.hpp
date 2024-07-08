//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyresult.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb_python/nanobind/nb_wrapper.hpp"
#include "duckdb_python/python_objects.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/hugeint.hpp"

#include "datetime.h" // from Python

namespace duckdb {

enum class PythonObjectType {
	Other,
	None,
	Integer,
	Float,
	Bool,
	Decimal,
	Uuid,
	Datetime,
	Date,
	Time,
	Timedelta,
	String,
	ByteArray,
	MemoryView,
	Bytes,
	List,
	Tuple,
	Dict,
	NdArray,
	NdDatetime,
	Value
};

PythonObjectType GetPythonObjectType(nb::handle &ele);

bool TryTransformPythonNumeric(Value &res, nb::handle ele, const LogicalType &target_type = LogicalType::UNKNOWN);
bool DictionaryHasMapFormat(const PyDictionary &dict);
Value TransformPythonValue(nb::handle ele, const LogicalType &target_type = LogicalType::UNKNOWN,
                           bool nan_as_null = true);

} // namespace duckdb
