#include "duckdb_python/pybind11/dataframe.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"

namespace duckdb {
bool PolarsDataFrame::IsDataFrame(const nb::handle &object) {
	if (!ModuleIsLoaded<PolarsCacheItem>()) {
		return false;
	}
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return nb::isinstance(object, import_cache.polars.DataFrame());
}

bool PolarsDataFrame::IsLazyFrame(const nb::handle &object) {
	if (!ModuleIsLoaded<PolarsCacheItem>()) {
		return false;
	}
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return nb::isinstance(object, import_cache.polars.LazyFrame());
}

bool PandasDataFrame::check_(const nb::handle &object) { // NOLINT
	if (!ModuleIsLoaded<PandasCacheItem>()) {
		return false;
	}
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return nb::isinstance(object, import_cache.pandas.DataFrame());
}

bool PandasDataFrame::IsPyArrowBacked(const nb::handle &df) {
	if (!PandasDataFrame::check_(df)) {
		return false;
	}

	auto &import_cache = *DuckDBPyConnection::ImportCache();
	nb::list dtypes = df.attr("dtypes");
	if (nb::len(dtypes) == 0) {
		return false;
	}

	auto arrow_dtype = import_cache.pandas.ArrowDtype();
	for (nb::handle dtype : dtypes) {
		if (nb::isinstance(dtype, arrow_dtype)) {
			return true;
		}
	}
	return false;
}

nb::object PandasDataFrame::ToArrowTable(const nb::object &df) {
	D_ASSERT(nb::gil_check());
	try {
		return nb::module_::import_("pyarrow").attr("lib").attr("Table").attr("from_pandas")(df);
	} catch (nb::python_error &) {
		// We don't fetch the original Python exception because it can cause a segfault
		// The cause of this is not known yet, for now we just side-step the issue.
		throw InvalidInputException(
		    "The dataframe could not be converted to a pyarrow.lib.Table, because a Python exception occurred.");
	}
}

bool PolarsDataFrame::check_(const nb::handle &object) { // NOLINT
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	return nb::isinstance(object, import_cache.polars.DataFrame());
}

} // namespace duckdb
