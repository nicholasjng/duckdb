#pragma once

#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb/common/helper.hpp"

using duckdb::DuckDBPyConnection;
using duckdb::shared_ptr;

namespace nb = nanobind;

namespace NB_NAMESPACE {
namespace detail {

template <>
struct type_caster<shared_ptr<DuckDBPyConnection>> : public type_caster_base<shared_ptr<DuckDBPyConnection>> {
    using base = type_caster_base<shared_ptr<DuckDBPyConnection>>;
	using type = DuckDBPyConnection;
	NB_TYPE_CASTER(shared_ptr<type>, const_name("duckdb.DuckDBPyConnection"));

	bool from_python(handle src, uint8_t flags, cleanup_list *cleanup) noexcept {
		if (nb::none().is(src)) {
			value = DuckDBPyConnection::DefaultConnection();
			return true;
		}
		if (!base::from_python(src, flags, cleanup)) {
		    return false;
		}
		return true;
	}

	static handle from_cpp(shared_ptr<type> base, rv_policy rvp, cleanup_list *cleanup) {
		return base::from_cpp(base, rvp, cleanup);
	}
};

} // namespace detail
} // namespace NB_NAMESPACE
