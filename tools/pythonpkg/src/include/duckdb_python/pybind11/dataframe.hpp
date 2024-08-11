//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pybind11/dataframe.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace duckdb {

class PandasDataFrame : public nb::object {
public:
	PandasDataFrame(const nb::object &o) : nb::object(o, borrowed_t {}) {
	}
	using nb::object::object;

public:
	static bool check_(const nb::handle &object); // NOLINT
	static bool IsPyArrowBacked(const nb::handle &df);
	static nb::object ToArrowTable(const nb::object &df);
};

class PolarsDataFrame : public nb::object {
public:
	PolarsDataFrame(const nb::object &o) : nb::object(o, borrowed_t {}) {
	}
	using nb::object::object;

public:
	static bool IsDataFrame(const nb::handle &object);
	static bool IsLazyFrame(const nb::handle &object);
	static bool check_(const nb::handle &object); // NOLINT
};
} // namespace duckdb

namespace nanobind {
namespace detail {
template <>
struct handle_type_name<duckdb::PandasDataFrame> {
	static constexpr auto name = _("pandas.DataFrame");
};
} // namespace detail
} // namespace nanobind
