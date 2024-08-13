#pragma once

#include "duckdb_python/pyconnection.hpp"
#include "duckdb/common/helper.hpp"

using duckdb::Optional;

namespace nb = nanobind;

namespace NB_NAMESPACE {
namespace detail {

template <class T>
struct type_caster<Optional<T>> : public type_caster_base<Optional<T>> {
	using base = type_caster_base<Optional<T>>;
	using child = type_caster_base<T>;
	Optional<T> tmp;

public:
	bool from_python(handle src, uint8_t flags, cleanup_list *cleanup) noexcept {
		if (base::from_python(src, flags, cleanup)) {
			return true;
		} else if (child::from_python(src, flags, cleanup)) {
			return true;
		}
		return false;
	}

	static handle from_cpp(Optional<T> src, rv_policy policy, cleanup_list *cleanup) noexcept {
		return base::from_cpp(src, policy, cleanup);
	}
};

} // namespace detail
} // namespace NB_NAMESPACE
