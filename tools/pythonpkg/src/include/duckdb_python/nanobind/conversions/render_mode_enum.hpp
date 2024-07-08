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

namespace nb = nanobind;

static RenderMode RenderModeFromInteger(int64_t value) {
	if (value == 0) {
		return RenderMode::ROWS;
	} else if (value == 1) {
		return RenderMode::COLUMNS;
	} else {
		throw InvalidInputException("Unrecognized type for 'render_mode'");
	}
}

namespace NANOBIND_NAMESPACE {
namespace detail {

template <>
struct type_caster<RenderMode> : public type_caster_base<RenderMode> {
	using base = type_caster_base<RenderMode>;
	RenderMode tmp;

public:
	bool from_python(handle src, uint8_t flags, cleanup_list *cleanup) noexcept {
		if (base::from_python(src, flags, cleanup)) {
			return true;
		} else if (nb::isinstance<nb::str>(src)) {
			string render_mode_str = nb::str(src);
			auto render_mode =
			    duckdb::EnumUtil::FromString<RenderMode>(render_mode_str.empty() ? "ROWS" : render_mode_str);
			value = &render_mode;
			return true;
		} else if (nb::isinstance<nb::int_>(src)) {
			tmp = RenderModeFromInteger(src.cast<int64_t>());
			value = &tmp;
			return true;
		}
		return false;
	}

	static handle from_cpp(RenderMode src, rv_policy policy, cleanup_list *cleanup) noexcept {
		return base::from_cpp(src, policy, cleanup);
	}
};

} // namespace detail
} // namespace NANOBIND_NAMESPACE
