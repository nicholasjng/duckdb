//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pandas/pandas_analyzer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb_python/nanobind/nb_wrapper.hpp"
#include "duckdb_python/nanobind/gil_wrapper.hpp"
#include "duckdb_python/numpy/numpy_type.hpp"
#include "duckdb_python/python_conversion.hpp"

namespace duckdb {

class PandasAnalyzer {
public:
	explicit PandasAnalyzer(const ClientContext &context) {
		analyzed_type = LogicalType::SQLNULL;

		Value result;
		auto lookup_result = context.TryGetCurrentSetting("pandas_analyze_sample", result);
		D_ASSERT((bool)lookup_result);
		sample_size = result.GetValue<uint64_t>();
	}

public:
	LogicalType GetListType(nb::object &ele, bool &can_convert);
	LogicalType DictToMap(const PyDictionary &dict, bool &can_convert);
	LogicalType DictToStruct(const PyDictionary &dict, bool &can_convert);
	LogicalType GetItemType(nb::object ele, bool &can_convert);
	bool Analyze(nb::object column);
	LogicalType AnalyzedType() {
		return analyzed_type;
	}

private:
	LogicalType InnerAnalyze(nb::object column, bool &can_convert, idx_t increment);
	uint64_t GetSampleIncrement(idx_t rows);

private:
	uint64_t sample_size;
	//! Holds the gil to allow python object creation/destruction
	PythonGILWrapper gil;
	//! The resulting analyzed type
	LogicalType analyzed_type;
};

} // namespace duckdb
