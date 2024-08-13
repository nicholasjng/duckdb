#pragma once

#include "duckdb_python/pandas/pandas_column.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace duckdb {

class PandasNumpyColumn : public PandasColumn {
public:
	PandasNumpyColumn(nb::ndarray<nb::numpy> array_p) : PandasColumn(PandasColumnBackend::NUMPY), array(std::move(array_p)) {
		D_ASSERT(nb::hasattr(array, "stride"));
		stride = static_cast<idx_t>(array.stride(0));
	}

public:
	nb::ndarray<nb::numpy> array;
	idx_t stride;
};

} // namespace duckdb
