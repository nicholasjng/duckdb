#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

using duckdb::FunctionNullHandling;
using duckdb::InvalidInputException;
using duckdb::string;
using duckdb::StringUtil;

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
