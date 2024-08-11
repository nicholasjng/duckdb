//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyconnection/pyconnection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb_python/arrow/arrow_array_stream.hpp"
#include "duckdb.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb_python/import_cache/python_import_cache.hpp"
#include "duckdb_python/numpy/numpy_type.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pytype.hpp"
#include "duckdb_python/path_like.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "duckdb_python/pyfilesystem.hpp"
#include "duckdb_python/pybind11/registered_py_object.hpp"
#include "duckdb_python/python_dependency.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb_python/pybind11/conversions/exception_handling_enum.hpp"
#include "duckdb_python/pybind11/conversions/python_udf_type_enum.hpp"
#include "duckdb_python/pybind11/conversions/python_csv_line_terminator_enum.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {
struct BoundParameterData;

enum class PythonEnvironmentType { NORMAL, INTERACTIVE, JUPYTER };

struct DuckDBPyRelation;

class RegisteredArrow : public RegisteredObject {

public:
	RegisteredArrow(unique_ptr<PythonTableArrowArrayStreamFactory> arrow_factory_p, nb::object obj_p)
	    : RegisteredObject(std::move(obj_p)), arrow_factory(std::move(arrow_factory_p)) {};
	unique_ptr<PythonTableArrowArrayStreamFactory> arrow_factory;
};

struct ConnectionGuard {
public:
	ConnectionGuard() {
	}
	~ConnectionGuard() {
	}

public:
	DuckDB &GetDatabase() {
		if (!database) {
			ThrowConnectionException();
		}
		return *database;
	}
	const DuckDB &GetDatabase() const {
		if (!database) {
			ThrowConnectionException();
		}
		return *database;
	}
	Connection &GetConnection() {
		if (!connection) {
			ThrowConnectionException();
		}
		return *connection;
	}
	const Connection &GetConnection() const {
		if (!connection) {
			ThrowConnectionException();
		}
		return *connection;
	}
	DuckDBPyRelation &GetResult() {
		if (!result) {
			ThrowConnectionException();
		}
		return *result;
	}
	const DuckDBPyRelation &GetResult() const {
		if (!result) {
			ThrowConnectionException();
		}
		return *result;
	}

public:
	bool HasResult() const {
		return result != nullptr;
	}

public:
	void SetDatabase(shared_ptr<DuckDB> db) {
		database = std::move(db);
	}
	void SetDatabase(ConnectionGuard &con) {
		if (!con.database) {
			ThrowConnectionException();
		}
		database = con.database;
	}
	void SetConnection(unique_ptr<Connection> con) {
		connection = std::move(con);
	}
	void SetResult(unique_ptr<DuckDBPyRelation> res) {
		result = std::move(res);
	}

private:
	void ThrowConnectionException() const {
		throw ConnectionException("Connection already closed!");
	}

private:
	shared_ptr<DuckDB> database;
	unique_ptr<Connection> connection;
	unique_ptr<DuckDBPyRelation> result;
};

struct DuckDBPyConnection : public enable_shared_from_this<DuckDBPyConnection> {
private:
	class Cursors {
	public:
		Cursors() {
		}

	public:
		void AddCursor(shared_ptr<DuckDBPyConnection> conn);
		void ClearCursors();

	private:
		mutex lock;
		vector<weak_ptr<DuckDBPyConnection>> cursors;
	};

public:
	ConnectionGuard con;
	Cursors cursors;
	std::mutex py_connection_lock;
	//! MemoryFileSystem used to temporarily store file-like objects for reading
	shared_ptr<ModifiedMemoryFileSystem> internal_object_filesystem;
	case_insensitive_map_t<unique_ptr<ExternalDependency>> registered_functions;
	case_insensitive_set_t registered_objects;

public:
	explicit DuckDBPyConnection() {
	}
	~DuckDBPyConnection();

public:
	static void Initialize(nb::handle &m);
	static void Cleanup();

	shared_ptr<DuckDBPyConnection> Enter();

	static void Exit(DuckDBPyConnection &self, const nb::object &exc_type, const nb::object &exc,
	                 const nb::object &traceback);

	static bool DetectAndGetEnvironment();
	static bool IsJupyter();
	static shared_ptr<DuckDBPyConnection> DefaultConnection();
	static PythonImportCache *ImportCache();
	static bool IsInteractive();

	unique_ptr<DuckDBPyRelation> ReadCSV(const nb::object &name, nb::kwargs &kwargs);

	nb::list ExtractStatements(const string &query);

	unique_ptr<DuckDBPyRelation> ReadJSON(
	    const nb::object &name, const Optional<nb::object> &columns = nb::none(),
	    const Optional<nb::object> &sample_size = nb::none(), const Optional<nb::object> &maximum_depth = nb::none(),
	    const Optional<nb::str> &records = nb::none(), const Optional<nb::str> &format = nb::none(),
	    const Optional<nb::object> &date_format = nb::none(), const Optional<nb::object> &timestamp_format = nb::none(),
	    const Optional<nb::object> &compression = nb::none(),
	    const Optional<nb::object> &maximum_object_size = nb::none(),
	    const Optional<nb::object> &ignore_errors = nb::none(),
	    const Optional<nb::object> &convert_strings_to_integers = nb::none(),
	    const Optional<nb::object> &field_appearance_threshold = nb::none(),
	    const Optional<nb::object> &map_inference_threshold = nb::none(),
	    const Optional<nb::object> &maximum_sample_files = nb::none(),
	    const Optional<nb::object> &filename = nb::none(), const Optional<nb::object> &hive_partitioning = nb::none(),
	    const Optional<nb::object> &union_by_name = nb::none(), const Optional<nb::object> &hive_types = nb::none(),
	    const Optional<nb::object> &hive_types_autocast = nb::none());

	shared_ptr<DuckDBPyType> MapType(const shared_ptr<DuckDBPyType> &key_type,
	                                 const shared_ptr<DuckDBPyType> &value_type);
	shared_ptr<DuckDBPyType> StructType(const nb::object &fields);
	shared_ptr<DuckDBPyType> ListType(const shared_ptr<DuckDBPyType> &type);
	shared_ptr<DuckDBPyType> ArrayType(const shared_ptr<DuckDBPyType> &type, idx_t size);
	shared_ptr<DuckDBPyType> UnionType(const nb::object &members);
	shared_ptr<DuckDBPyType> EnumType(const string &name, const shared_ptr<DuckDBPyType> &type,
	                                  const nb::list &values_p);
	shared_ptr<DuckDBPyType> DecimalType(int width, int scale);
	shared_ptr<DuckDBPyType> StringType(const string &collation = string());
	shared_ptr<DuckDBPyType> Type(const string &type_str);

	shared_ptr<DuckDBPyConnection>
	RegisterScalarUDF(const string &name, const nb::callable &udf, const nb::object &arguments = nb::none(),
	                  const shared_ptr<DuckDBPyType> &return_type = nullptr, PythonUDFType type = PythonUDFType::NATIVE,
	                  FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                  PythonExceptionHandling exception_handling = PythonExceptionHandling::FORWARD_ERROR,
	                  bool side_effects = false);

	shared_ptr<DuckDBPyConnection> UnregisterUDF(const string &name);

	shared_ptr<DuckDBPyConnection> ExecuteMany(const nb::object &query, nb::object params = nb::list());

	void ExecuteImmediately(vector<unique_ptr<SQLStatement>> statements);
	unique_ptr<PreparedStatement> PrepareQuery(unique_ptr<SQLStatement> statement);
	unique_ptr<QueryResult> ExecuteInternal(PreparedStatement &prep, nb::object params = nb::list());

	shared_ptr<DuckDBPyConnection> Execute(const nb::object &query, nb::object params = nb::list());
	shared_ptr<DuckDBPyConnection> ExecuteFromString(const string &query);

	shared_ptr<DuckDBPyConnection> Append(const string &name, const PandasDataFrame &value, bool by_name);

	shared_ptr<DuckDBPyConnection> RegisterPythonObject(const string &name, const nb::object &python_object);

	void InstallExtension(const string &extension, bool force_install = false);

	void LoadExtension(const string &extension);

	unique_ptr<DuckDBPyRelation> RunQuery(const nb::object &query, string alias = "", nb::object params = nb::list());

	unique_ptr<DuckDBPyRelation> Table(const string &tname);

	unique_ptr<DuckDBPyRelation> Values(nb::object params = nb::none());

	unique_ptr<DuckDBPyRelation> View(const string &vname);

	unique_ptr<DuckDBPyRelation> TableFunction(const string &fname, nb::object params = nb::list());

	unique_ptr<DuckDBPyRelation> FromDF(const PandasDataFrame &value);

	unique_ptr<DuckDBPyRelation> FromParquet(const string &file_glob, bool binary_as_string, bool file_row_number,
	                                         bool filename, bool hive_partitioning, bool union_by_name,
	                                         const nb::object &compression = nb::none());

	unique_ptr<DuckDBPyRelation> FromParquets(const vector<string> &file_globs, bool binary_as_string,
	                                          bool file_row_number, bool filename, bool hive_partitioning,
	                                          bool union_by_name, const nb::object &compression = nb::none());

	unique_ptr<DuckDBPyRelation> FromArrow(nb::object &arrow_object);

	unique_ptr<DuckDBPyRelation> FromSubstrait(nb::bytes &proto);

	unique_ptr<DuckDBPyRelation> GetSubstrait(const string &query, bool enable_optimizer = true);

	unique_ptr<DuckDBPyRelation> GetSubstraitJSON(const string &query, bool enable_optimizer = true);

	unique_ptr<DuckDBPyRelation> FromSubstraitJSON(const string &json);

	unordered_set<string> GetTableNames(const string &query);

	shared_ptr<DuckDBPyConnection> UnregisterPythonObject(const string &name);

	shared_ptr<DuckDBPyConnection> Begin();

	shared_ptr<DuckDBPyConnection> Commit();

	shared_ptr<DuckDBPyConnection> Rollback();

	shared_ptr<DuckDBPyConnection> Checkpoint();

	void Close();

	void Interrupt();

	ModifiedMemoryFileSystem &GetObjectFileSystem();

	// cursor() is stupid
	shared_ptr<DuckDBPyConnection> Cursor();

	Optional<nb::list> GetDescription();

	int GetRowcount();

	// these should be functions on the result but well
	Optional<nb::tuple> FetchOne();

	nb::list FetchMany(idx_t size);

	nb::list FetchAll();

	nb::dict FetchNumpy();
	PandasDataFrame FetchDF(bool date_as_object);
	PandasDataFrame FetchDFChunk(const idx_t vectors_per_chunk = 1, bool date_as_object = false);

	duckdb::pyarrow::Table FetchArrow(idx_t rows_per_batch);
	PolarsDataFrame FetchPolars(idx_t rows_per_batch);

	nb::dict FetchPyTorch();

	nb::dict FetchTF();

	duckdb::pyarrow::RecordBatchReader FetchRecordBatchReader(const idx_t rows_per_batch);

	static shared_ptr<DuckDBPyConnection> Connect(const nb::object &database, bool read_only, const nb::dict &config);

	static vector<Value> TransformPythonParamList(const nb::handle &params);
	static case_insensitive_map_t<BoundParameterData> TransformPythonParamDict(const nb::dict &params);

	void RegisterFilesystem(AbstractFileSystem filesystem);
	void UnregisterFilesystem(const nb::str &name);
	nb::list ListFilesystems();
	bool FileSystemIsRegistered(const string &name);

	//! Default connection to an in-memory database
	static shared_ptr<DuckDBPyConnection> default_connection;
	//! Caches and provides an interface to get frequently used modules+subtypes
	static shared_ptr<PythonImportCache> import_cache;

	static bool IsPandasDataframe(const nb::object &object);
	static bool IsPolarsDataframe(const nb::object &object);
	static bool IsAcceptedArrowObject(const nb::object &object);
	static NumpyObjectType IsAcceptedNumpyObject(const nb::object &object);

	static unique_ptr<QueryResult> CompletePendingQuery(PendingQueryResult &pending_query);

private:
	PathLike GetPathLike(const nb::object &object);
	unique_lock<std::mutex> AcquireConnectionLock();
	ScalarFunction CreateScalarUDF(const string &name, const nb::callable &udf, const nb::object &parameters,
	                               const shared_ptr<DuckDBPyType> &return_type, bool vectorized,
	                               FunctionNullHandling null_handling, PythonExceptionHandling exception_handling,
	                               bool side_effects);
	void RegisterArrowObject(const nb::object &arrow_object, const string &name);
	vector<unique_ptr<SQLStatement>> GetStatements(const nb::object &query);

	static PythonEnvironmentType environment;
	static void DetectEnvironment();
};

template <typename T>
static bool ModuleIsLoaded() {
	auto dict = nb::module_::import_("sys").attr("modules");
	return dict.contains(nb::str(T::Name));
}

} // namespace duckdb
