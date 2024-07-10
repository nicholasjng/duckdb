#pragma once

#include "duckdb_python/pyconnection.hpp"
#include "duckdb/common/helper.hpp"

using duckdb::Optional;

namespace nb = nanobind;

namespace NANOBIND_NAMESPACE {
namespace detail {

// TODO: Port this type caster
template <class T>
struct type_caster<Optional<T>> : public type_caster_base<Optional<T>> {
	using base = type_caster_base<Optional<T>>;
	using child = type_caster_base<T>;
	Optional<T> tmp;

public:
	bool load(handle src, bool convert) {
		if (base::load(src, convert)) {
			return true;
		} else if (child::load(src, convert)) {
			return true;
		}
		return false;
	}

	static handle cast(Optional<T> src, return_value_policy policy, handle parent) {
		return base::cast(src, policy, parent);
	}
};

} // namespace detail
} // namespace nanobind_NAMESPACE
