#pragma once

#include "duckdb_python/nanobind/nb_wrapper.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {

struct PandasColumnBindData;
class ClientContext;

struct NumpyBind {
	static void Bind(const ClientContext &config, nb::handle df, vector<PandasColumnBindData> &out,
	                 vector<LogicalType> &return_types, vector<string> &names);
};

} // namespace duckdb
