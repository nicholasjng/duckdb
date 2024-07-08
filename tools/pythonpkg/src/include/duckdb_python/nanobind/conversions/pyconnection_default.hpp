#pragma once

#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb/common/helper.hpp"

using duckdb::DuckDBPyConnection;
using duckdb::shared_ptr;

namespace nb = nanobind;

namespace NANOBIND_NAMESPACE {
namespace detail {

template <>
class type_caster<shared_ptr<DuckDBPyConnection>> noexcept {
	using type = DuckDBPyConnection;
	// This is used to generate documentation on duckdb-web
	NB_TYPE_CASTER(shared_ptr<type>, const_name("duckdb.DuckDBPyConnection"));

	bool from_python(handle src, uint8_t flags, cleanup_list *cleanup) {
		if (nb::none().is(src)) {
			value = DuckDBPyConnection::DefaultConnection();
			return true;
		}
		value = std::move()
		return true;
	}

	static handle from_cpp(shared_ptr<type> base, rv_policy rvp, cleanup_list *cleanup) noexcept {
		return holder_caster::from_cpp(base, rvp, h);
	}
};

template <>
struct is_holder_type<DuckDBPyConnection, shared_ptr<DuckDBPyConnection>> : std::true_type {};

} // namespace detail
} // namespace NANOBIND_NAMESPACE
