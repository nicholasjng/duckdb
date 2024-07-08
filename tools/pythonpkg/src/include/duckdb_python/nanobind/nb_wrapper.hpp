//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/nanobind/nb_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <nanobind/nanobind.h>
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

// TODO: Replace with nb::isinstance after confirming functional equivalence
// But we have the option to override certain functions
template <typename T, detail::enable_if_t<std::is_base_of<object, T>::value, int> = 0>
bool isinstance(handle obj) {
	return T::check_(obj);
}

template <typename T, detail::enable_if_t<!std::is_base_of<object, T>::value, int> = 0>
bool isinstance(handle obj) {
	return detail::isinstance_generic(obj, typeid(T));
}

template <>
inline bool isinstance<handle>(handle) = delete;
template <>
inline bool isinstance<object>(handle obj) {
	return obj.ptr() != nullptr;
}

inline bool isinstance(handle obj, handle type) {
	if (type.ptr() == nullptr) {
		// The type was not imported, just return false
		return false;
	}
	const auto result = PyObject_IsInstance(obj.ptr(), type.ptr());
	if (result == -1) {
		throw error_already_set();
	}
	return result != 0;
}

template <class T>
bool try_cast(const handle &object, T &result) {
	try {
		result = cast<T>(object);
	} catch (cast_error &) {
		return false;
	}
	return true;
}

} // namespace nb

template <class T, typename... ARGS>
void DefineMethod(std::vector<const char *> aliases, T &mod, ARGS &&... args) {
	for (auto &alias : aliases) {
		mod.def(alias, args...);
	}
}

} // namespace duckdb
