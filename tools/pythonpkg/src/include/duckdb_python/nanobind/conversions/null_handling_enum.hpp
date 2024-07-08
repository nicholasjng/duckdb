#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

using duckdb::FunctionNullHandling;
using duckdb::InvalidInputException;
using duckdb::string;
using duckdb::StringUtil;

namespace nb = nanobind;

static FunctionNullHandling FunctionNullHandlingFromString(const string &type) {
	auto ltype = StringUtil::Lower(type);
	if (ltype.empty() || ltype == "default") {
		return FunctionNullHandling::DEFAULT_NULL_HANDLING;
	} else if (ltype == "special") {
		return FunctionNullHandling::SPECIAL_HANDLING;
	} else {
		throw InvalidInputException("'%s' is not a recognized type for 'null_handling'", type);
	}
}

static FunctionNullHandling FunctionNullHandlingFromInteger(int64_t value) {
	if (value == 0) {
		return FunctionNullHandling::DEFAULT_NULL_HANDLING;
	} else if (value == 1) {
		return FunctionNullHandling::SPECIAL_HANDLING;
	} else {
		throw InvalidInputException("'%d' is not a recognized type for 'null_handling'", value);
	}
}

namespace NANOBIND_NAMESPACE {
namespace detail {

template <>
struct type_caster<FunctionNullHandling> : public type_caster_base<FunctionNullHandling> {
	using base = type_caster_base<FunctionNullHandling>;
	FunctionNullHandling tmp;

public:
	bool from_python(handle src, uint8_t flags, cleanup_list *cleanup) noexcept {
		if (base::from_python(src, flags, cleanup)) {
			return true;
		} else if (nb::isinstance<nb::str>(src)) {
			tmp = FunctionNullHandlingFromString(nb::str(src));
			value = &tmp;
			return true;
		} else if (nb::isinstance<nb::int_>(src)) {
			tmp = FunctionNullHandlingFromInteger(src.cast<int64_t>());
			value = &tmp;
			return true;
		}
		return false;
	}

	static handle from_cpp(FunctionNullHandling src, rv_policy policy, cleanup_list *cleanup) noexcept {
		return base::from_cpp(src, policy, cleanup);
	}
};

} // namespace detail
} // namespace NANOBIND_NAMESPACE
