//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/python_import_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/nanobind/nb_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb_python/import_cache/python_import_cache_modules.hpp"
#include "duckdb/common/stack.hpp"

namespace duckdb {

struct PythonImporter {
public:
	static nb::handle Import(stack<reference<PythonImportCacheItem>> &hierarchy, bool load = true);
};

} // namespace duckdb
