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
