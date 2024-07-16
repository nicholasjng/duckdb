
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

namespace duckdb {

struct PythonImportCache {
public:
	explicit PythonImportCache() {
	}
	~PythonImportCache();

public:
	PyarrowCacheItem pyarrow;
	PandasCacheItem pandas;
	DatetimeCacheItem datetime;
	DecimalCacheItem decimal;
	IpythonCacheItem IPython;
	IpywidgetsCacheItem ipywidgets;
	NumpyCacheItem numpy;
	PathlibCacheItem pathlib;
	PolarsCacheItem polars;
	DuckdbCacheItem duckdb;
	PytzCacheItem pytz;
	TypesCacheItem types;
	TypingCacheItem typing;
	UuidCacheItem uuid;
	CollectionsCacheItem collections;

public:
	nb::handle AddCache(nb::object item);

private:
	vector<nb::object> owned_objects;
};

} // namespace duckdb
