#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

using duckdb::InvalidInputException;
using duckdb::string;
using duckdb::StringUtil;

namespace duckdb {

enum class PythonUDFType : uint8_t { NATIVE, ARROW };

} // namespace duckdb

using duckdb::PythonUDFType;

namespace nb = nanobind;

static PythonUDFType PythonUDFTypeFromString(const string &type) {
	auto ltype = StringUtil::Lower(type);
	if (ltype.empty() || ltype == "default" || ltype == "native") {
		return PythonUDFType::NATIVE;
	} else if (ltype == "arrow") {
		return PythonUDFType::ARROW;
	} else {
		throw InvalidInputException("'%s' is not a recognized type for 'udf_type'", type);
	}
}

static PythonUDFType PythonUDFTypeFromInteger(int64_t value) {
	if (value == 0) {
		return PythonUDFType::NATIVE;
	} else if (value == 1) {
		return PythonUDFType::ARROW;
	} else {
		throw InvalidInputException("'%d' is not a recognized type for 'udf_type'", value);
	}
}

namespace NANOBIND_NAMESPACE {
namespace detail {

template <>
struct type_caster<PythonUDFType> : public type_caster_base<PythonUDFType> {
	using base = type_caster_base<PythonUDFType>;
	PythonUDFType tmp;

public:
	bool from_python(handle src, uint8_t flags, cleanup_list *cleanup) noexcept {
		if (base::from_python(src, flags, cleanup)) {
			return true;
		} else if (nb::isinstance<nb::str>(src)) {
			tmp = PythonUDFTypeFromString(nb::str(src));
			value = &tmp;
			return true;
		} else if (nb::isinstance<nb::int_>(src)) {
			tmp = PythonUDFTypeFromInteger(src.cast<int64_t>());
			value = &tmp;
			return true;
		}
		return false;
	}

	static handle from_cpp(PythonUDFType src, rv_policy policy, cleanup_list* cleanup) noexcept {
		return base::from_cpp(src, policy, cleanup);
	}
};

} // namespace detail
} // namespace NANOBIND_NAMESPACE
