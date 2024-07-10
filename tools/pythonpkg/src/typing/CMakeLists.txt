# this is used for clang-tidy checks
include_directories(${nanobind_INCLUDE_DIR})
include_directories(${PYTHON_INCLUDE_DIRS})
find_package(nanobind REQUIRED)

add_library(python_type OBJECT pytype.cpp typing.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:python_type>
    PARENT_SCOPE)
