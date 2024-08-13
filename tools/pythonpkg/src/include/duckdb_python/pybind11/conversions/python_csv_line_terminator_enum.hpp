#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

using duckdb::InvalidInputException;
using duckdb::string;
using duckdb::StringUtil;

namespace duckdb {

struct PythonCSVLineTerminator {
public:
	PythonCSVLineTerminator() = delete;
	enum class Type { LINE_FEED, CARRIAGE_RETURN_LINE_FEED };

public:
	static Type FromString(const string &str) {
		if (str == "\\n") {
			return Type::LINE_FEED;
		}
		if (str == "\\r\\n") {
			return Type::CARRIAGE_RETURN_LINE_FEED;
		}
		if (str == "LINE_FEED") {
			return Type::LINE_FEED;
		}
		if (str == "CARRIAGE_RETURN_LINE_FEED") {
			return Type::CARRIAGE_RETURN_LINE_FEED;
		}
		throw InvalidInputException("'%s' is not a recognized type for 'lineterminator'", str);
	}
	static string ToString(Type type) {
		switch (type) {
		case Type::LINE_FEED:
			return "\\n";
		case Type::CARRIAGE_RETURN_LINE_FEED:
			return "\\r\\n";
		default:
			throw NotImplementedException("No conversion for PythonCSVLineTerminator enum to string");
		}
	}
};

} // namespace duckdb

using duckdb::PythonCSVLineTerminator;

namespace nb = nanobind;

namespace NB_NAMESPACE {
namespace detail {

template <>
struct type_caster<PythonCSVLineTerminator::Type> : public type_caster_base<PythonCSVLineTerminator::Type> {
    NB_TYPE_CASTER(PythonCSVLineTerminator::Type, "PythonCSVLineTerminatorType")

    using base = type_caster_base<PythonCSVLineTerminator::Type>;

public:
	bool from_python(handle src, uint8_t flags, cleanup_list *cleanup) noexcept {
		if (base::from_python(src, flags, cleanup)) {
			return true;
		} else if (nb::isinstance<nb::str>(src)) {
			value = duckdb::PythonCSVLineTerminator::FromString(nb::cast<string>(src));
			return true;
		}
		return false;
	}

	static handle from_cpp(PythonCSVLineTerminator::Type src, rv_policy policy, cleanup_list *cleanup) noexcept {
		return base::from_cpp(src, policy, cleanup);
	}
};

} // namespace detail
} // namespace NB_NAMESPACE
