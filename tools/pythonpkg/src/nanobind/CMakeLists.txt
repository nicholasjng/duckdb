# this is used for clang-tidy checks
include_directories(${nanobind_INCLUDE_DIR})
include_directories(${PYTHON_INCLUDE_DIRS})
find_package(nanobind REQUIRED)

// TODO: This is bad search-replace, use actual nanobind CMake APIs
add_library(python_nanobind OBJECT pybind_wrapper.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:python_nanobind>
    PARENT_SCOPE)
