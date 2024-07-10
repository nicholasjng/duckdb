#pragma once

#include "duckdb_python/nanobind/nb_wrapper.hpp"
#include "duckdb_python/nanobind/python_object_container.hpp"
#include "duckdb_python/numpy/numpy_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb_python/pandas/pandas_column.hpp"

namespace duckdb {

class ClientContext;

struct RegisteredArray {
	explicit RegisteredArray(nb::array numpy_array) : numpy_array(std::move(numpy_array)) {
	}
	nb::array numpy_array;
};

struct PandasColumnBindData {
	NumpyType numpy_type;
	unique_ptr<PandasColumn> pandas_col;
	unique_ptr<RegisteredArray> mask;
	//! Only for categorical types
	string internal_categorical_type;
	//! Hold ownership of objects created during scanning
	PythonObjectContainer object_str_val;
};

struct Pandas {
	static void Bind(const ClientContext &config, nb::handle df, vector<PandasColumnBindData> &out,
	                 vector<LogicalType> &return_types, vector<string> &names);
};

} // namespace duckdb
