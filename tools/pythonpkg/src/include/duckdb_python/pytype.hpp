#pragma once

#include "duckdb_python/nanobind/nb_wrapper.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

class PyGenericAlias : public nb::object {
public:
	using nb::object::object;

public:
	static bool check_(const nb::handle &object);
};

class PyUnionType : public nb::object {
public:
	using nb::object::object;

public:
	static bool check_(const nb::handle &object);
};

class DuckDBPyType : public enable_shared_from_this<DuckDBPyType> {
public:
	explicit DuckDBPyType(LogicalType type);

public:
	static void Initialize(nb::handle &m);

public:
	bool Equals(const shared_ptr<DuckDBPyType> &other) const;
	bool EqualsString(const string &type_str) const;
	shared_ptr<DuckDBPyType> GetAttribute(const string &name) const;
	nb::list Children() const;
	string ToString() const;
	const LogicalType &Type() const;
	string GetId() const;

private:
private:
	LogicalType type;
};

} // namespace duckdb
