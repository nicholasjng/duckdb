#pragma once

#include "duckdb_python/nanobind/nb_wrapper.hpp"

namespace duckdb {

struct PythonGILWrapper {
	nb::gil_scoped_acquire acquire;
};

} // namespace duckdb
