#pragma once

#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

using duckdb::ExplainType;
using duckdb::InvalidInputException;
using duckdb::string;
using duckdb::StringUtil;

namespace nb = nanobind;

static ExplainType ExplainTypeFromString(const string &type) {
	auto ltype = StringUtil::Lower(type);
	if (ltype.empty() || ltype == "standard") {
		return ExplainType::EXPLAIN_STANDARD;
	} else if (ltype == "analyze") {
		return ExplainType::EXPLAIN_ANALYZE;
	} else {
		throw InvalidInputException("Unrecognized type for 'explain'");
	}
}

static ExplainType ExplainTypeFromInteger(int64_t value) {
	if (value == 0) {
		return ExplainType::EXPLAIN_STANDARD;
	} else if (value == 1) {
		return ExplainType::EXPLAIN_ANALYZE;
	} else {
		throw InvalidInputException("Unrecognized type for 'explain'");
	}
}

namespace NB_NAMESPACE {
namespace detail {

template <>
struct type_caster<ExplainType> : public type_caster_base<ExplainType> {
    NB_TYPE_CASTER(ExplainType, const_name("ExplainType"))

	using base = type_caster_base<ExplainType>;
	ExplainType tmp;

public:
	bool from_python(handle src, uint8_t flags, cleanup_list *cleanup) noexcept {
		if (base::from_python(src, flags, cleanup)) {
			return true;
		} else if (nb::isinstance<nb::str>(src)) {
			tmp = ExplainTypeFromString(nb::cast<std::string>(src));
			value = tmp;
			return true;
		} else if (nb::isinstance<nb::int_>(src)) {
			tmp = ExplainTypeFromInteger(nb::cast<int64_t>(src));
			value = tmp;
			return true;
		}
		return false;
	}

	static handle from_cpp(ExplainType src, rv_policy policy, cleanup_list *cleanup) noexcept {
		return base::from_cpp(src, policy, cleanup);
	}
};

} // namespace detail
} // namespace NB_NAMESPACE
