#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/enum_util.hpp"

using duckdb::InvalidInputException;
using duckdb::RenderMode;
using duckdb::string;
using duckdb::StringUtil;

static RenderMode RenderModeFromInteger(int64_t value) {
	if (value == 0) {
		return RenderMode::ROWS;
	} else if (value == 1) {
		return RenderMode::COLUMNS;
	} else {
		throw InvalidInputException("Unrecognized type for 'render_mode'");
	}
}
