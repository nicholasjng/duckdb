//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/nanobind/nb_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/shared_ptr.h>
#include <nanobind/stl/unique_ptr.h>
#include <nanobind/stl/list.h>
#include <nanobind/stl/tuple.h>
#include "duckdb/common/vector.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/helper.hpp"
#include <memory>

namespace nanobind {

namespace detail {

template <typename Type, bool SAFE>
struct type_caster<duckdb::vector<Type, SAFE>> : list_caster<duckdb::vector<Type, SAFE>, Type> {};
} // namespace detail

bool gil_check();
void gil_assert();
bool is_list_like(handle obj);
bool is_dict_like(handle obj);

} // namespace nanobind

namespace duckdb {
#ifdef __GNUG__
#define NANOBIND_NAMESPACE nanobind __attribute__((visibility("hidden")))
#else
#define NANOBIND_NAMESPACE nanobind
#endif
namespace nb {

// We include everything from nanobind
using namespace nanobind;

using ssize_t = Py_ssize_t;

// nanobind does not have this built in.
NB_INLINE bool isinstance_python(handle obj, handle type) {
	if (type.ptr() == nullptr) {
		// The type was not imported, just return false
		return false;
	}
	const auto result = PyObject_IsInstance(obj.ptr(), type.ptr());
	if (result == -1) {
		throw python_error();
	}
	return result != 0;
}

} // namespace nb

template <class T, typename... ARGS>
void DefineMethod(std::vector<const char *> aliases, T &mod, ARGS &&... args) {
	for (auto &alias : aliases) {
		mod.def(alias, args...);
	}
}

} // namespace duckdb
