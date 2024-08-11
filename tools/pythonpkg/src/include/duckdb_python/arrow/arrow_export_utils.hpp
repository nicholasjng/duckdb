#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace duckdb {

namespace pyarrow {

nb::object ToArrowTable(const vector<LogicalType> &types, const vector<string> &names, const nb::list &batches,
                        const ClientProperties &options);

} // namespace pyarrow

} // namespace duckdb
