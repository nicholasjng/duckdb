//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/nanobind/registered_py_object.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb_python/nanobind/nb_wrapper.hpp"

namespace duckdb {

class RegisteredObject {
public:
	explicit RegisteredObject(nb::object obj_p) : obj(std::move(obj_p)) {
	}
	virtual ~RegisteredObject() {
		nb::gil_scoped_acquire acquire;
		obj = nb::none();
	}

	nb::object obj;
};

} // namespace duckdb
