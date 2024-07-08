#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/main/external_dependencies.hpp"
#include "duckdb_python/nanobind/nb_wrapper.hpp"
#include "duckdb_python/nanobind/registered_py_object.hpp"

namespace duckdb {

class PythonDependencyItem : public DependencyItem {
public:
	explicit PythonDependencyItem(unique_ptr<RegisteredObject> &&object);
	~PythonDependencyItem() override;

public:
	static shared_ptr<DependencyItem> Create(nb::object object);
	static shared_ptr<DependencyItem> Create(unique_ptr<RegisteredObject> &&object);

public:
	unique_ptr<RegisteredObject> object;
};

} // namespace duckdb
