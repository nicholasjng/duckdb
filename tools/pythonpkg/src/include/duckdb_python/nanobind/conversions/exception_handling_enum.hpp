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
