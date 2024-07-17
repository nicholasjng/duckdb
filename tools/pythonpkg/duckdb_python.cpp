#include "duckdb_python/nanobind/nb_wrapper.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parser.hpp"

#include "duckdb_python/python_objects.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pystatement.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/expression/pyexpression.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb_python/nanobind/exceptions.hpp"
#include "duckdb_python/typing.hpp"
#include "duckdb_python/functional.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb_python/nanobind/conversions/exception_handling_enum.hpp"
#include "duckdb_python/nanobind/conversions/python_udf_type_enum.hpp"
#include "duckdb/common/enums/statement_type.hpp"

#include "duckdb.hpp"

#ifndef DUCKDB_PYTHON_LIB_NAME
#define DUCKDB_PYTHON_LIB_NAME duckdb
#endif

namespace nb = nanobind;

namespace duckdb {

enum PySQLTokenType : uint8_t {
	PY_SQL_TOKEN_IDENTIFIER = 0,
	PY_SQL_TOKEN_NUMERIC_CONSTANT,
	PY_SQL_TOKEN_STRING_CONSTANT,
	PY_SQL_TOKEN_OPERATOR,
	PY_SQL_TOKEN_KEYWORD,
	PY_SQL_TOKEN_COMMENT
};

static nb::list PyTokenize(const string &query) {
	auto tokens = Parser::Tokenize(query);
	nb::list result;
	for (auto &token : tokens) {
		auto tuple = nb::make_tuple(2);
		tuple[0] = token.start;
		switch (token.type) {
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER:
			tuple[1] = PY_SQL_TOKEN_IDENTIFIER;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_NUMERIC_CONSTANT:
			tuple[1] = PY_SQL_TOKEN_NUMERIC_CONSTANT;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT:
			tuple[1] = PY_SQL_TOKEN_STRING_CONSTANT;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR:
			tuple[1] = PY_SQL_TOKEN_OPERATOR;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_KEYWORD:
			tuple[1] = PY_SQL_TOKEN_KEYWORD;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_COMMENT:
			tuple[1] = PY_SQL_TOKEN_COMMENT;
			break;
		}
		result.append(tuple);
	}
	return result;
}

static void InitializeConnectionMethods(nb::module_ &m) {

	// START_OF_CONNECTION_METHODS
	m.def(
	    "cursor",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Cursor();
	    },
	    "Create a duplicate of the current connection", nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "register_filesystem",
	    [](AbstractFileSystem filesystem, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    conn->RegisterFilesystem(filesystem);
	    },
	    "Register a fsspec compliant filesystem", nb::arg("filesystem"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "unregister_filesystem",
	    [](const nb::str &name, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    conn->UnregisterFilesystem(name);
	    },
	    "Unregister a filesystem", nb::arg("name"), nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "list_filesystems",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ListFilesystems();
	    },
	    "List registered filesystems, including builtin ones", nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "filesystem_is_registered",
	    [](const string &name, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FileSystemIsRegistered(name);
	    },
	    "Check if a filesystem with the provided name is currently registered", nb::arg("name"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "create_function",
	    [](const string &name, const nb::callable &udf, const nb::object &arguments = nb::none(),
	       const shared_ptr<DuckDBPyType> &return_type = nullptr, PythonUDFType type = PythonUDFType::NATIVE,
	       FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING,
	       PythonExceptionHandling exception_handling = PythonExceptionHandling::FORWARD_ERROR,
	       bool side_effects = false, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->RegisterScalarUDF(name, udf, arguments, return_type, type, null_handling, exception_handling,
		                                   side_effects);
	    },
	    "Create a DuckDB function out of the passing in Python function so it can be used in queries", nb::arg("name"),
	    nb::arg("function"), nb::arg("parameters") = nb::none(), nb::arg("return_type") = nb::none(), nb::kw_only(),
	    nb::arg("type") = PythonUDFType::NATIVE, nb::arg("null_handling") = FunctionNullHandling::DEFAULT_NULL_HANDLING,
	    nb::arg("exception_handling") = PythonExceptionHandling::FORWARD_ERROR, nb::arg("side_effects") = false,
	    nb::arg("connection") = nb::none());
	m.def(
	    "remove_function",
	    [](const string &name, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->UnregisterUDF(name);
	    },
	    "Remove a previously created function", nb::arg("name"), nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "sqltype",
	    [](const string &type_str, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Type(type_str);
	    },
	    "Create a type object by parsing the 'type_str' string", nb::arg("type_str"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "dtype",
	    [](const string &type_str, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Type(type_str);
	    },
	    "Create a type object by parsing the 'type_str' string", nb::arg("type_str"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "type",
	    [](const string &type_str, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Type(type_str);
	    },
	    "Create a type object by parsing the 'type_str' string", nb::arg("type_str"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "array_type",
	    [](const shared_ptr<DuckDBPyType> &type, idx_t size, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ArrayType(type, size);
	    },
	    "Create an array type object of 'type'", nb::arg("type").none(false), nb::arg("size"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "list_type",
	    [](const shared_ptr<DuckDBPyType> &type, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ListType(type);
	    },
	    "Create a list type object of 'type'", nb::arg("type").none(false), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "union_type",
	    [](const nb::object &members, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->UnionType(members);
	    },
	    "Create a union type object from 'members'", nb::arg("members").none(false), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "string_type",
	    [](const string &collation = string(), shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->StringType(collation);
	    },
	    "Create a string type with an optional collation", nb::arg("collation") = "", nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "enum_type",
	    [](const string &name, const shared_ptr<DuckDBPyType> &type, const nb::list &values_p,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->EnumType(name, type, values_p);
	    },
	    "Create an enum type of underlying 'type', consisting of the list of 'values'", nb::arg("name"),
	    nb::arg("type"), nb::arg("values"), nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "decimal_type",
	    [](int width, int scale, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->DecimalType(width, scale);
	    },
	    "Create a decimal type with 'width' and 'scale'", nb::arg("width"), nb::arg("scale"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "struct_type",
	    [](const nb::object &fields, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->StructType(fields);
	    },
	    "Create a struct type object from 'fields'", nb::arg("fields"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "row_type",
	    [](const nb::object &fields, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->StructType(fields);
	    },
	    "Create a struct type object from 'fields'", nb::arg("fields"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "map_type",
	    [](const shared_ptr<DuckDBPyType> &key_type, const shared_ptr<DuckDBPyType> &value_type,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->MapType(key_type, value_type);
	    },
	    "Create a map type object from 'key_type' and 'value_type'", nb::arg("key").none(false),
	    nb::arg("value").none(false), nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "duplicate",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Cursor();
	    },
	    "Create a duplicate of the current connection", nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "execute",
	    [](const nb::object &query, nb::object params = nb::list(), shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Execute(query, params);
	    },
	    "Execute the given SQL query, optionally using prepared statements with parameters set", nb::arg("query"),
	    nb::arg("parameters") = nb::none(), nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "executemany",
	    [](const nb::object &query, nb::object params = nb::list(), shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ExecuteMany(query, params);
	    },
	    "Execute the given prepared statement multiple times using the list of parameter sets in parameters",
	    nb::arg("query"), nb::arg("parameters") = nb::none(), nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "close",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    conn->Close();
	    },
	    "Close the connection", nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "interrupt",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    conn->Interrupt();
	    },
	    "Interrupt pending operations", nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "fetchone",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchOne();
	    },
	    "Fetch a single row from a result following execute", nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "fetchmany",
	    [](idx_t size, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchMany(size);
	    },
	    "Fetch the next set of rows from a result following execute", nb::arg("size") = 1, nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "fetchall",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchAll();
	    },
	    "Fetch all rows from a result following execute", nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "fetchnumpy",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchNumpy();
	    },
	    "Fetch a result as list of NumPy arrays following execute", nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "fetchdf",
	    [](bool date_as_object, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchDF(date_as_object);
	    },
	    "Fetch a result as DataFrame following execute()", nb::kw_only(), nb::arg("date_as_object") = false,
	    nb::arg("connection") = nb::none());
	m.def(
	    "fetch_df",
	    [](bool date_as_object, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchDF(date_as_object);
	    },
	    "Fetch a result as DataFrame following execute()", nb::kw_only(), nb::arg("date_as_object") = false,
	    nb::arg("connection") = nb::none());
	m.def(
	    "df",
	    [](bool date_as_object, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchDF(date_as_object);
	    },
	    "Fetch a result as DataFrame following execute()", nb::kw_only(), nb::arg("date_as_object") = false,
	    nb::arg("connection") = nb::none());
	m.def(
	    "fetch_df_chunk",
	    [](const idx_t vectors_per_chunk = 1, bool date_as_object = false,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchDFChunk(vectors_per_chunk, date_as_object);
	    },
	    "Fetch a chunk of the result as DataFrame following execute()", nb::arg("vectors_per_chunk") = 1, nb::kw_only(),
	    nb::arg("date_as_object") = false, nb::arg("connection") = nb::none());
	m.def(
	    "pl",
	    [](idx_t rows_per_batch, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchPolars(rows_per_batch);
	    },
	    "Fetch a result as Polars DataFrame following execute()", nb::arg("rows_per_batch") = 1000000, nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "fetch_arrow_table",
	    [](idx_t rows_per_batch, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchArrow(rows_per_batch);
	    },
	    "Fetch a result as Arrow table following execute()", nb::arg("rows_per_batch") = 1000000, nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "arrow",
	    [](idx_t rows_per_batch, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchArrow(rows_per_batch);
	    },
	    "Fetch a result as Arrow table following execute()", nb::arg("rows_per_batch") = 1000000, nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "fetch_record_batch",
	    [](const idx_t rows_per_batch, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchRecordBatchReader(rows_per_batch);
	    },
	    "Fetch an Arrow RecordBatchReader following execute()", nb::arg("rows_per_batch") = 1000000, nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "torch",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchPyTorch();
	    },
	    "Fetch a result as dict of PyTorch Tensors following execute()", nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "tf",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchTF();
	    },
	    "Fetch a result as dict of TensorFlow Tensors following execute()", nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "begin",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Begin();
	    },
	    "Start a new transaction", nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "commit",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Commit();
	    },
	    "Commit changes performed within a transaction", nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "rollback",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Rollback();
	    },
	    "Roll back changes performed within a transaction", nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "checkpoint",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Checkpoint();
	    },
	    "Synchronizes data in the write-ahead log (WAL) to the database data file (no-op for in-memory connections)",
	    nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "append",
	    [](const string &name, const PandasDataFrame &value, bool by_name,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Append(name, value, by_name);
	    },
	    "Append the passed DataFrame to the named table", nb::arg("table_name"), nb::arg("df"), nb::kw_only(),
	    nb::arg("by_name") = false, nb::arg("connection") = nb::none());
	m.def(
	    "register",
	    [](const string &name, const nb::object &python_object, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->RegisterPythonObject(name, python_object);
	    },
	    "Register the passed Python Object value for querying with a view", nb::arg("view_name"),
	    nb::arg("python_object"), nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "unregister",
	    [](const string &name, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->UnregisterPythonObject(name);
	    },
	    "Unregister the view name", nb::arg("view_name"), nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "table",
	    [](const string &tname, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Table(tname);
	    },
	    "Create a relation object for the named table", nb::arg("table_name"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "view",
	    [](const string &vname, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->View(vname);
	    },
	    "Create a relation object for the named view", nb::arg("view_name"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "values",
	    [](nb::object params = nb::none(), shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->Values(params);
	    },
	    "Create a relation object from the passed values", nb::arg("values"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "table_function",
	    [](const string &fname, nb::object params = nb::list(), shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->TableFunction(fname, params);
	    },
	    "Create a relation object from the named table function with given parameters", nb::arg("name"),
	    nb::arg("parameters") = nb::none(), nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "read_json",
	    [](const string &name, const Optional<nb::object> &columns = nb::none(),
	       const Optional<nb::object> &sample_size = nb::none(), const Optional<nb::object> &maximum_depth = nb::none(),
	       const Optional<nb::str> &records = nb::none(), const Optional<nb::str> &format = nb::none(),
	       const Optional<nb::object> &date_format = nb::none(),
	       const Optional<nb::object> &timestamp_format = nb::none(),
	       const Optional<nb::object> &compression = nb::none(),
	       const Optional<nb::object> &maximum_object_size = nb::none(),
	       const Optional<nb::object> &ignore_errors = nb::none(),
	       const Optional<nb::object> &convert_strings_to_integers = nb::none(),
	       const Optional<nb::object> &field_appearance_threshold = nb::none(),
	       const Optional<nb::object> &map_inference_threshold = nb::none(),
	       const Optional<nb::object> &maximum_sample_files = nb::none(),
	       const Optional<nb::object> &filename = nb::none(),
	       const Optional<nb::object> &hive_partitioning = nb::none(),
	       const Optional<nb::object> &union_by_name = nb::none(), const Optional<nb::object> &hive_types = nb::none(),
	       const Optional<nb::object> &hive_types_autocast = nb::none(),
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ReadJSON(name, columns, sample_size, maximum_depth, records, format, date_format,
		                          timestamp_format, compression, maximum_object_size, ignore_errors,
		                          convert_strings_to_integers, field_appearance_threshold, map_inference_threshold,
		                          maximum_sample_files, filename, hive_partitioning, union_by_name, hive_types,
		                          hive_types_autocast);
	    },
	    "Create a relation object from the JSON file in 'name'", nb::arg("name"), nb::kw_only(),
	    nb::arg("columns") = nb::none(), nb::arg("sample_size") = nb::none(), nb::arg("maximum_depth") = nb::none(),
	    nb::arg("records") = nb::none(), nb::arg("format") = nb::none(), nb::arg("date_format") = nb::none(),
	    nb::arg("timestamp_format") = nb::none(), nb::arg("compression") = nb::none(),
	    nb::arg("maximum_object_size") = nb::none(), nb::arg("ignore_errors") = nb::none(),
	    nb::arg("convert_strings_to_integers") = nb::none(), nb::arg("field_appearance_threshold") = nb::none(),
	    nb::arg("map_inference_threshold") = nb::none(), nb::arg("maximum_sample_files") = nb::none(),
	    nb::arg("filename") = nb::none(), nb::arg("hive_partitioning") = nb::none(),
	    nb::arg("union_by_name") = nb::none(), nb::arg("hive_types") = nb::none(),
	    nb::arg("hive_types_autocast") = nb::none(), nb::arg("connection") = nb::none());
	m.def(
	    "extract_statements",
	    [](const string &query, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ExtractStatements(query);
	    },
	    "Parse the query string and extract the Statement object(s) produced", nb::arg("query"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "sql",
	    [](const nb::object &query, string alias = "", nb::object params = nb::list(),
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->RunQuery(query, alias, params);
	    },
	    "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, otherwise "
	    "run the query as-is.",
	    nb::arg("query"), nb::kw_only(), nb::arg("alias") = "", nb::arg("params") = nb::none(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "query",
	    [](const nb::object &query, string alias = "", nb::object params = nb::list(),
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->RunQuery(query, alias, params);
	    },
	    "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, otherwise "
	    "run the query as-is.",
	    nb::arg("query"), nb::kw_only(), nb::arg("alias") = "", nb::arg("params") = nb::none(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "from_query",
	    [](const nb::object &query, string alias = "", nb::object params = nb::list(),
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->RunQuery(query, alias, params);
	    },
	    "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, otherwise "
	    "run the query as-is.",
	    nb::arg("query"), nb::kw_only(), nb::arg("alias") = "", nb::arg("params") = nb::none(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "read_csv",
	    [](const nb::object &name, const nb::object &header = nb::none(), const nb::object &compression = nb::none(),
	       const nb::object &sep = nb::none(), const nb::object &delimiter = nb::none(),
	       const nb::object &dtype = nb::none(), const nb::object &na_values = nb::none(),
	       const nb::object &skiprows = nb::none(), const nb::object &quotechar = nb::none(),
	       const nb::object &escapechar = nb::none(), const nb::object &encoding = nb::none(),
	       const nb::object &parallel = nb::none(), const nb::object &date_format = nb::none(),
	       const nb::object &timestamp_format = nb::none(), const nb::object &sample_size = nb::none(),
	       const nb::object &all_varchar = nb::none(), const nb::object &normalize_names = nb::none(),
	       const nb::object &filename = nb::none(), const nb::object &null_padding = nb::none(),
	       const nb::object &names = nb::none(), shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ReadCSV(name, header, compression, sep, delimiter, dtype, na_values, skiprows, quotechar,
		                         escapechar, encoding, parallel, date_format, timestamp_format, sample_size,
		                         all_varchar, normalize_names, filename, null_padding, names);
	    },
	    "Create a relation object from the CSV file in 'name'", nb::arg("path_or_buffer"), nb::kw_only(),
	    nb::arg("header") = nb::none(), nb::arg("compression") = nb::none(), nb::arg("sep") = nb::none(),
	    nb::arg("delimiter") = nb::none(), nb::arg("dtype") = nb::none(), nb::arg("na_values") = nb::none(),
	    nb::arg("skiprows") = nb::none(), nb::arg("quotechar") = nb::none(), nb::arg("escapechar") = nb::none(),
	    nb::arg("encoding") = nb::none(), nb::arg("parallel") = nb::none(), nb::arg("date_format") = nb::none(),
	    nb::arg("timestamp_format") = nb::none(), nb::arg("sample_size") = nb::none(),
	    nb::arg("all_varchar") = nb::none(), nb::arg("normalize_names") = nb::none(), nb::arg("filename") = nb::none(),
	    nb::arg("null_padding") = nb::none(), nb::arg("names") = nb::none(), nb::arg("connection") = nb::none());
	m.def(
	    "from_csv_auto",
	    [](const nb::object &name, const nb::object &header = nb::none(), const nb::object &compression = nb::none(),
	       const nb::object &sep = nb::none(), const nb::object &delimiter = nb::none(),
	       const nb::object &dtype = nb::none(), const nb::object &na_values = nb::none(),
	       const nb::object &skiprows = nb::none(), const nb::object &quotechar = nb::none(),
	       const nb::object &escapechar = nb::none(), const nb::object &encoding = nb::none(),
	       const nb::object &parallel = nb::none(), const nb::object &date_format = nb::none(),
	       const nb::object &timestamp_format = nb::none(), const nb::object &sample_size = nb::none(),
	       const nb::object &all_varchar = nb::none(), const nb::object &normalize_names = nb::none(),
	       const nb::object &filename = nb::none(), const nb::object &null_padding = nb::none(),
	       const nb::object &names = nb::none(), shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->ReadCSV(name, header, compression, sep, delimiter, dtype, na_values, skiprows, quotechar,
		                         escapechar, encoding, parallel, date_format, timestamp_format, sample_size,
		                         all_varchar, normalize_names, filename, null_padding, names);
	    },
	    "Create a relation object from the CSV file in 'name'", nb::arg("path_or_buffer"), nb::kw_only(),
	    nb::arg("header") = nb::none(), nb::arg("compression") = nb::none(), nb::arg("sep") = nb::none(),
	    nb::arg("delimiter") = nb::none(), nb::arg("dtype") = nb::none(), nb::arg("na_values") = nb::none(),
	    nb::arg("skiprows") = nb::none(), nb::arg("quotechar") = nb::none(), nb::arg("escapechar") = nb::none(),
	    nb::arg("encoding") = nb::none(), nb::arg("parallel") = nb::none(), nb::arg("date_format") = nb::none(),
	    nb::arg("timestamp_format") = nb::none(), nb::arg("sample_size") = nb::none(),
	    nb::arg("all_varchar") = nb::none(), nb::arg("normalize_names") = nb::none(), nb::arg("filename") = nb::none(),
	    nb::arg("null_padding") = nb::none(), nb::arg("names") = nb::none(), nb::arg("connection") = nb::none());
	m.def(
	    "from_df",
	    [](const PandasDataFrame &value, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromDF(value);
	    },
	    "Create a relation object from the DataFrame in df", nb::arg("df"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "from_arrow",
	    [](nb::object &arrow_object, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromArrow(arrow_object);
	    },
	    "Create a relation object from an Arrow object", nb::arg("arrow_object"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "from_parquet",
	    [](const string &file_glob, bool binary_as_string, bool file_row_number, bool filename, bool hive_partitioning,
	       bool union_by_name, const nb::object &compression = nb::none(),
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromParquet(file_glob, binary_as_string, file_row_number, filename, hive_partitioning,
		                             union_by_name, compression);
	    },
	    "Create a relation object from the Parquet files in file_glob", nb::arg("file_glob"),
	    nb::arg("binary_as_string") = false, nb::kw_only(), nb::arg("file_row_number") = false,
	    nb::arg("filename") = false, nb::arg("hive_partitioning") = false, nb::arg("union_by_name") = false,
	    nb::arg("compression") = nb::none(), nb::arg("connection") = nb::none());
	m.def(
	    "read_parquet",
	    [](const string &file_glob, bool binary_as_string, bool file_row_number, bool filename, bool hive_partitioning,
	       bool union_by_name, const nb::object &compression = nb::none(),
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromParquet(file_glob, binary_as_string, file_row_number, filename, hive_partitioning,
		                             union_by_name, compression);
	    },
	    "Create a relation object from the Parquet files in file_glob", nb::arg("file_glob"),
	    nb::arg("binary_as_string") = false, nb::kw_only(), nb::arg("file_row_number") = false,
	    nb::arg("filename") = false, nb::arg("hive_partitioning") = false, nb::arg("union_by_name") = false,
	    nb::arg("compression") = nb::none(), nb::arg("connection") = nb::none());
	m.def(
	    "from_parquet",
	    [](const vector<string> &file_globs, bool binary_as_string, bool file_row_number, bool filename,
	       bool hive_partitioning, bool union_by_name, const nb::object &compression = nb::none(),
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromParquets(file_globs, binary_as_string, file_row_number, filename, hive_partitioning,
		                              union_by_name, compression);
	    },
	    "Create a relation object from the Parquet files in file_globs", nb::arg("file_globs"),
	    nb::arg("binary_as_string") = false, nb::kw_only(), nb::arg("file_row_number") = false,
	    nb::arg("filename") = false, nb::arg("hive_partitioning") = false, nb::arg("union_by_name") = false,
	    nb::arg("compression") = nb::none(), nb::arg("connection") = nb::none());
	m.def(
	    "read_parquet",
	    [](const vector<string> &file_globs, bool binary_as_string, bool file_row_number, bool filename,
	       bool hive_partitioning, bool union_by_name, const nb::object &compression = nb::none(),
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromParquets(file_globs, binary_as_string, file_row_number, filename, hive_partitioning,
		                              union_by_name, compression);
	    },
	    "Create a relation object from the Parquet files in file_globs", nb::arg("file_globs"),
	    nb::arg("binary_as_string") = false, nb::kw_only(), nb::arg("file_row_number") = false,
	    nb::arg("filename") = false, nb::arg("hive_partitioning") = false, nb::arg("union_by_name") = false,
	    nb::arg("compression") = nb::none(), nb::arg("connection") = nb::none());
	m.def(
	    "from_substrait",
	    [](nb::bytes &proto, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromSubstrait(proto);
	    },
	    "Create a query object from protobuf plan", nb::arg("proto"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "get_substrait",
	    [](const string &query, bool enable_optimizer = true, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->GetSubstrait(query, enable_optimizer);
	    },
	    "Serialize a query to protobuf", nb::arg("query"), nb::kw_only(), nb::arg("enable_optimizer") = true,
	    nb::arg("connection") = nb::none());
	m.def(
	    "get_substrait_json",
	    [](const string &query, bool enable_optimizer = true, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->GetSubstraitJSON(query, enable_optimizer);
	    },
	    "Serialize a query to protobuf on the JSON format", nb::arg("query"), nb::kw_only(),
	    nb::arg("enable_optimizer") = true, nb::arg("connection") = nb::none());
	m.def(
	    "from_substrait_json",
	    [](const string &json, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromSubstraitJSON(json);
	    },
	    "Create a query object from a JSON protobuf plan", nb::arg("json"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "get_table_names",
	    [](const string &query, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->GetTableNames(query);
	    },
	    "Extract the required table names from a query", nb::arg("query"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "install_extension",
	    [](const string &extension, bool force_install = false, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    conn->InstallExtension(extension, force_install);
	    },
	    "Install an extension by name", nb::arg("extension"), nb::kw_only(), nb::arg("force_install") = false,
	    nb::arg("connection") = nb::none());
	m.def(
	    "load_extension",
	    [](const string &extension, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    conn->LoadExtension(extension);
	    },
	    "Load an installed extension", nb::arg("extension"), nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "project",
	    [](const PandasDataFrame &df, const nb::args &args, const string &groups = "",
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromDF(df)->Project(args, groups);
	    },
	    "Project the relation object by the projection in project_expr", nb::arg("df"), nb::arg("args"),
	    nb::kw_only(), nb::arg("groups") = "", nb::arg("connection") = nb::none());
	m.def(
	    "distinct",
	    [](const PandasDataFrame &df, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromDF(df)->Distinct();
	    },
	    "Retrieve distinct rows from this relation object", nb::arg("df"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "write_csv",
	    [](const PandasDataFrame &df, const string &filename, const nb::object &sep = nb::none(),
	       const nb::object &na_rep = nb::none(), const nb::object &header = nb::none(),
	       const nb::object &quotechar = nb::none(), const nb::object &escapechar = nb::none(),
	       const nb::object &date_format = nb::none(), const nb::object &timestamp_format = nb::none(),
	       const nb::object &quoting = nb::none(), const nb::object &encoding = nb::none(),
	       const nb::object &compression = nb::none(), const nb::object &overwrite = nb::none(),
	       const nb::object &per_thread_output = nb::none(), const nb::object &use_tmp_file = nb::none(),
	       const nb::object &partition_by = nb::none(), shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    conn->FromDF(df)->ToCSV(filename, sep, na_rep, header, quotechar, escapechar, date_format, timestamp_format,
		                            quoting, encoding, compression, overwrite, per_thread_output, use_tmp_file,
		                            partition_by);
	    },
	    "Write the relation object to a CSV file in 'file_name'", nb::arg("df"), nb::arg("filename"), nb::kw_only(),
	    nb::arg("sep") = nb::none(), nb::arg("na_rep") = nb::none(), nb::arg("header") = nb::none(),
	    nb::arg("quotechar") = nb::none(), nb::arg("escapechar") = nb::none(), nb::arg("date_format") = nb::none(),
	    nb::arg("timestamp_format") = nb::none(), nb::arg("quoting") = nb::none(), nb::arg("encoding") = nb::none(),
	    nb::arg("compression") = nb::none(), nb::arg("overwrite") = nb::none(),
	    nb::arg("per_thread_output") = nb::none(), nb::arg("use_tmp_file") = nb::none(),
	    nb::arg("partition_by") = nb::none(), nb::arg("connection") = nb::none());
	m.def(
	    "aggregate",
	    [](const PandasDataFrame &df, const nb::object &expr, const string &groups = "",
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromDF(df)->Aggregate(expr, groups);
	    },
	    "Compute the aggregate aggr_expr by the optional groups group_expr on the relation", nb::arg("df"),
	    nb::arg("aggr_expr"), nb::arg("group_expr") = "", nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "alias",
	    [](const PandasDataFrame &df, const string &expr, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromDF(df)->SetAlias(expr);
	    },
	    "Rename the relation object to new alias", nb::arg("df"), nb::arg("alias"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "filter",
	    [](const PandasDataFrame &df, const nb::object &expr, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromDF(df)->Filter(expr);
	    },
	    "Filter the relation object by the filter in filter_expr", nb::arg("df"), nb::arg("filter_expr"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "limit",
	    [](const PandasDataFrame &df, int64_t n, int64_t offset = 0, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromDF(df)->Limit(n, offset);
	    },
	    "Only retrieve the first n rows from this relation object, starting at offset", nb::arg("df"), nb::arg("n"),
	    nb::arg("offset") = 0, nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "order",
	    [](const PandasDataFrame &df, const string &expr, shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromDF(df)->Order(expr);
	    },
	    "Reorder the relation object by order_expr", nb::arg("df"), nb::arg("order_expr"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "query_df",
	    [](const PandasDataFrame &df, const string &view_name, const string &sql_query,
	       shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromDF(df)->Query(view_name, sql_query);
	    },
	    "Run the given SQL query in sql_query on the view named virtual_table_name that refers to the relation object",
	    nb::arg("df"), nb::arg("virtual_table_name"), nb::arg("sql_query"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "description",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->GetDescription();
	    },
	    "Get result set attributes, mainly column names", nb::kw_only(), nb::arg("connection") = nb::none());
	m.def(
	    "rowcount",
	    [](shared_ptr<DuckDBPyConnection> conn = nullptr) {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->GetRowcount();
	    },
	    "Get result set row count", nb::kw_only(), nb::arg("connection") = nb::none());
	// END_OF_CONNECTION_METHODS

	// We define these "wrapper" methods manually because they are overloaded
	m.def(
	    "arrow",
	    [](idx_t rows_per_batch, shared_ptr<DuckDBPyConnection> conn) -> duckdb::pyarrow::Table {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchArrow(rows_per_batch);
	    },
	    "Fetch a result as Arrow table following execute()", nb::arg("rows_per_batch") = 1000000, nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "arrow",
	    [](nb::object &arrow_object, shared_ptr<DuckDBPyConnection> conn) -> unique_ptr<DuckDBPyRelation> {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromArrow(arrow_object);
	    },
	    "Create a relation object from an Arrow object", nb::arg("arrow_object"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
	m.def(
	    "df",
	    [](bool date_as_object, shared_ptr<DuckDBPyConnection> conn) -> PandasDataFrame {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FetchDF(date_as_object);
	    },
	    "Fetch a result as DataFrame following execute()", nb::kw_only(), nb::arg("date_as_object") = false,
	    nb::arg("connection") = nb::none());
	m.def(
	    "df",
	    [](const PandasDataFrame &value, shared_ptr<DuckDBPyConnection> conn) -> unique_ptr<DuckDBPyRelation> {
		    if (!conn) {
			    conn = DuckDBPyConnection::DefaultConnection();
		    }
		    return conn->FromDF(value);
	    },
	    "Create a relation object from the DataFrame df", nb::arg("df"), nb::kw_only(),
	    nb::arg("connection") = nb::none());
}

static void RegisterStatementType(nb::handle &m) {
	auto statement_type = nb::enum_<duckdb::StatementType>(m, "StatementType");
	static const duckdb::StatementType TYPES[] = {
	    duckdb::StatementType::INVALID_STATEMENT,      duckdb::StatementType::SELECT_STATEMENT,
	    duckdb::StatementType::INSERT_STATEMENT,       duckdb::StatementType::UPDATE_STATEMENT,
	    duckdb::StatementType::CREATE_STATEMENT,       duckdb::StatementType::DELETE_STATEMENT,
	    duckdb::StatementType::PREPARE_STATEMENT,      duckdb::StatementType::EXECUTE_STATEMENT,
	    duckdb::StatementType::ALTER_STATEMENT,        duckdb::StatementType::TRANSACTION_STATEMENT,
	    duckdb::StatementType::COPY_STATEMENT,         duckdb::StatementType::ANALYZE_STATEMENT,
	    duckdb::StatementType::VARIABLE_SET_STATEMENT, duckdb::StatementType::CREATE_FUNC_STATEMENT,
	    duckdb::StatementType::EXPLAIN_STATEMENT,      duckdb::StatementType::DROP_STATEMENT,
	    duckdb::StatementType::EXPORT_STATEMENT,       duckdb::StatementType::PRAGMA_STATEMENT,
	    duckdb::StatementType::VACUUM_STATEMENT,       duckdb::StatementType::CALL_STATEMENT,
	    duckdb::StatementType::SET_STATEMENT,          duckdb::StatementType::LOAD_STATEMENT,
	    duckdb::StatementType::RELATION_STATEMENT,     duckdb::StatementType::EXTENSION_STATEMENT,
	    duckdb::StatementType::LOGICAL_PLAN_STATEMENT, duckdb::StatementType::ATTACH_STATEMENT,
	    duckdb::StatementType::DETACH_STATEMENT,       duckdb::StatementType::MULTI_STATEMENT,
	    duckdb::StatementType::COPY_DATABASE_STATEMENT};
	static const idx_t AMOUNT = sizeof(TYPES) / sizeof(duckdb::StatementType);
	for (idx_t i = 0; i < AMOUNT; i++) {
		auto &type = TYPES[i];
		statement_type.value(StatementTypeToString(type).c_str(), type);
	}
	statement_type.export_values();
}

static void RegisterExpectedResultType(nb::handle &m) {
	auto expected_return_type = nb::enum_<duckdb::StatementReturnType>(m, "ExpectedResultType");
	static const duckdb::StatementReturnType TYPES[] = {duckdb::StatementReturnType::QUERY_RESULT,
	                                                    duckdb::StatementReturnType::CHANGED_ROWS,
	                                                    duckdb::StatementReturnType::NOTHING};
	static const idx_t AMOUNT = sizeof(TYPES) / sizeof(duckdb::StatementReturnType);
	for (idx_t i = 0; i < AMOUNT; i++) {
		auto &type = TYPES[i];
		expected_return_type.value(StatementReturnTypeToString(type).c_str(), type);
	}
	expected_return_type.export_values();
}

NB_MODULE(DUCKDB_PYTHON_LIB_NAME, m) { // NOLINT
	nb::enum_<duckdb::ExplainType>(m, "ExplainType")
	    .value("STANDARD", duckdb::ExplainType::EXPLAIN_STANDARD)
	    .value("ANALYZE", duckdb::ExplainType::EXPLAIN_ANALYZE)
	    .export_values();

	RegisterStatementType(m);

	RegisterExpectedResultType(m);

	nb::enum_<duckdb::PythonExceptionHandling>(m, "PythonExceptionHandling")
	    .value("DEFAULT", duckdb::PythonExceptionHandling::FORWARD_ERROR)
	    .value("RETURN_NULL", duckdb::PythonExceptionHandling::RETURN_NULL)
	    .export_values();

	nb::enum_<duckdb::RenderMode>(m, "RenderMode")
	    .value("ROWS", duckdb::RenderMode::ROWS)
	    .value("COLUMNS", duckdb::RenderMode::COLUMNS)
	    .export_values();

	DuckDBPyTyping::Initialize(m);
	DuckDBPyFunctional::Initialize(m);
	DuckDBPyExpression::Initialize(m);
	DuckDBPyStatement::Initialize(m);
	DuckDBPyRelation::Initialize(m);
	DuckDBPyConnection::Initialize(m);
	PythonObject::Initialize();

	m.doc() = "DuckDB is an embeddable SQL OLAP Database Management System";
	m.attr("__package__") = "duckdb";
	m.attr("__version__") = std::string(DuckDB::LibraryVersion()).substr(1);
	m.attr("__standard_vector_size__") = DuckDB::StandardVectorSize();
	m.attr("__git_revision__") = DuckDB::SourceID();
	m.attr("__interactive__") = DuckDBPyConnection::DetectAndGetEnvironment();
	m.attr("__jupyter__") = DuckDBPyConnection::IsJupyter();
	m.attr("default_connection") = DuckDBPyConnection::DefaultConnection();
	m.attr("apilevel") = "2.0";
	m.attr("threadsafety") = 1;
	m.attr("paramstyle") = "qmark";

	InitializeConnectionMethods(m);

	RegisterExceptions(m);

	m.def("connect", &DuckDBPyConnection::Connect,
	      "Create a DuckDB database instance. Can take a database file name to read/write persistent data and a "
	      "read_only flag if no changes are desired",
	      nb::arg("database") = ":memory:", nb::arg("read_only") = false, nb::arg("config") = nb::dict());
	m.def("tokenize", PyTokenize,
	      "Tokenizes a SQL string, returning a list of (position, type) tuples that can be "
	      "used for e.g. syntax highlighting",
	      nb::arg("query"));
	nb::enum_<PySQLTokenType>(m, "token_type")
	    .value("identifier", PySQLTokenType::PY_SQL_TOKEN_IDENTIFIER)
	    .value("numeric_const", PySQLTokenType::PY_SQL_TOKEN_NUMERIC_CONSTANT)
	    .value("string_const", PySQLTokenType::PY_SQL_TOKEN_STRING_CONSTANT)
	    .value("operator", PySQLTokenType::PY_SQL_TOKEN_OPERATOR)
	    .value("keyword", PySQLTokenType::PY_SQL_TOKEN_KEYWORD)
	    .value("comment", PySQLTokenType::PY_SQL_TOKEN_COMMENT)
	    .export_values();

	// we need this because otherwise we try to remove registered_dfs on shutdown when python is already dead
	m.def("_clean_default_connection", []() -> void * { DuckDBPyConnection::Cleanup(); });
}

} // namespace duckdb
