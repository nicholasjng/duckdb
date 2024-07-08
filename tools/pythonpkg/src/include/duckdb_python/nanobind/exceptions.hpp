#include "duckdb_python/nanobind/nb_wrapper.hpp"

namespace nb = nanobind;

namespace duckdb {

void RegisterExceptions(const nb::module_ &m);

} // namespace duckdb
