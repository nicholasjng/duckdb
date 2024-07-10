#pragma once

#include "duckdb_python/nanobind/nb_wrapper.hpp"

namespace duckdb {

namespace pyarrow {

nb::object ToArrowTable(const vector<LogicalType> &types, const vector<string> &names, const nb::list &batches,
                        const ClientProperties &options);

} // namespace pyarrow

} // namespace duckdb
