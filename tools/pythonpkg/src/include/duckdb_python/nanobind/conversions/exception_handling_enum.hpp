#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

using duckdb::InvalidInputException;
using duckdb::string;
using duckdb::StringUtil;

namespace duckdb {

enum class PythonExceptionHandling : uint8_t { FORWARD_ERROR, RETURN_NULL };

} // namespace duckdb

using duckdb::PythonExceptionHandling;

namespace nb = nanobind;

static PythonExceptionHandling PythonExceptionHandlingFromString(const string &type) {
	auto ltype = StringUtil::Lower(type);
	if (ltype.empty() || ltype == "default") {
		return PythonExceptionHandling::FORWARD_ERROR;
	} else if (ltype == "return_null") {
		return PythonExceptionHandling::RETURN_NULL;
	} else {
		throw InvalidInputException("'%s' is not a recognized type for 'exception_handling'", type);
	}
}

static PythonExceptionHandling PythonExceptionHandlingFromInteger(int64_t value) {
	if (value == 0) {
		return PythonExceptionHandling::FORWARD_ERROR;
	} else if (value == 1) {
		return PythonExceptionHandling::RETURN_NULL;
	} else {
		throw InvalidInputException("'%d' is not a recognized type for 'exception_handling'", value);
	}
}

namespace NANOBIND_NAMESPACE {
namespace detail {

template <>
struct type_caster<PythonExceptionHandling> : public type_caster_base<PythonExceptionHandling> {
	using base = type_caster_base<PythonExceptionHandling>;
	PythonExceptionHandling tmp;

public:
	bool from_python(handle src, uint8_t flags, cleanup_list *cleanup) noexcept {
		if (base::from_python(src, flags, cleanup)) {
			return true;
		} else if (nb::isinstance<nb::str>(src)) {
			tmp = PythonExceptionHandlingFromString(nb::str(src));
			value = &tmp;
			return true;
		} else if (nb::isinstance<nb::int_>(src)) {
			tmp = PythonExceptionHandlingFromInteger(src.cast<int64_t>());
			value = &tmp;
			return true;
		}
		return false;
	}

	static handle from_cpp(PythonExceptionHandling src, rv_policy policy, cleanup_list *cleanup) noexcept {
		return base::from_cpp(src, policy, cleanup);
	}
};

} // namespace detail
} // namespace NANOBIND_NAMESPACE
