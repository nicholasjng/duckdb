include_directories(src)
include_directories(../../src/include)
include_directories(src/include)

# Try to import all Python components potentially needed by nanobind
find_package(Python 3.8
  REQUIRED COMPONENTS Interpreter Development.Module
  OPTIONAL_COMPONENTS Development.SABIModule)

# Import nanobind through CMake's find_package mechanism
find_package(nanobind CONFIG REQUIRED)

nanobind_add_module(
  # Name of the extension
  duckdb

  # Target the stable ABI for Python 3.12+, which reduces
  # the number of binary wheels that must be built. This
  # does nothing on older Python versions
  STABLE_ABI

  # Build libnanobind statically and merge it into the
  # extension (which itself remains a shared library)
  #
  # If your project builds multiple extensions, you can
  # replace this flag by NB_SHARED to conserve space by
  # reusing a shared libnanobind across libraries
  NB_STATIC

  # Source code goes here
  src/arrow/arrow_array_stream.cpp
  src/arrow/arrow_export_utils.cpp
  src/common/exceptions.cpp
  src/dataframe.cpp
  src/functional/functional.cpp
  src/importer.cpp
  src/jupyter/jupyter_progress_bar_display.cpp
  src/map.cpp
  src/nanobind/nb_wrapper.cpp
  src/native/python_conversion.cpp
  src/native/python_objects.cpp
  src/numpy/array_wrapper.cpp
  src/numpy/numpy_bind.cpp
  src/numpy/numpy_result_conversion.cpp
  src/numpy/numpy_scan.cpp
  src/numpy/raw_array_wrapper.cpp
  src/numpy/type.cpp
  src/pandas/analyzer.cpp
  src/pandas/bind.cpp
  src/pandas/scan.cpp
  src/path_like.cpp
  src/pyconnection/type_creation.cpp
  src/pyconnection.cpp
  src/pyexpression/initialize.cpp
  src/pyexpression.cpp
  src/pyfilesystem.cpp
  src/pyrelation/initialize.cpp
  src/pyrelation.cpp
  src/pyresult.cpp
  src/pystatement.cpp
  src/python_dependency.cpp
  src/python_import_cache.cpp
  src/python_replacement_scan.cpp
  src/python_udf.cpp
  src/typing/pytype.cpp
  src/typing/typing.cpp
)

add_subdirectory(src)
# this is used for clang-tidy checks

set(ALL_OBJECT_FILES duckdb_python.cpp ${ALL_OBJECT_FILES})

add_library(python_duckdb ${ALL_OBJECT_FILES})
target_include_directories(python_duckdb PRIVATE ${PYTHON_INCLUDE_DIRS})
target_link_libraries(python_duckdb duckdb_static ${PYTHON_LIBRARIES})
