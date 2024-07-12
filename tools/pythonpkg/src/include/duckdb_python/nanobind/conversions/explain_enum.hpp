#pragma once

#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

using duckdb::ExplainType;
using duckdb::InvalidInputException;
using duckdb::string;
using duckdb::StringUtil;

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
