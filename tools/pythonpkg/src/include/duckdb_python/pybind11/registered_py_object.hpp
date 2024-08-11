//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pybind11/registered_py_object.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

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
