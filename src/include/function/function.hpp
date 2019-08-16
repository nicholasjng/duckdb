//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "common/unordered_set.hpp"
#include "parser/column_definition.hpp"

namespace duckdb {
class CatalogEntry;
class Catalog;
class ClientContext;
class ExpressionExecutor;
class Transaction;

class AggregateFunction;
class FunctionSet;
class ScalarFunction;
class TableFunction;

struct FunctionData {
	virtual ~FunctionData() {
	}

	virtual unique_ptr<FunctionData> Copy() = 0;
};

//! Function is the base class used for any type of function (scalar, aggregate or simple function)
class Function {
public:
	Function(string name) : name(name) { }
	virtual ~Function() {}

	//! The name of the function
	string name;
public:
	//! Returns the formatted string name(arg1, arg2, ...)
	static string CallToString(string name, vector<SQLType> arguments);
	//! Returns the formatted string name(arg1, arg2..) -> return_type
	static string CallToString(string name, vector<SQLType> arguments, SQLType return_type);
};

class SimpleFunction : public Function {
public:
	SimpleFunction(string name, vector<SQLType> arguments, SQLType return_type, bool has_side_effects) :
		Function(name), arguments(move(arguments)), return_type(return_type), has_side_effects(has_side_effects) { }
	virtual ~SimpleFunction() {}

	//! The set of arguments of the function
	vector<SQLType> arguments;
	//! Return type of the function
	SQLType return_type;
	//! Whether or not the function has side effects (e.g. sequence increments, random() functions, NOW()). Functions
	//! with side-effects cannot be constant-folded.
	bool has_side_effects;
};

class BuiltinFunctions {
public:
	BuiltinFunctions(Transaction &transaction, Catalog &catalog);

	//! Initialize a catalog with all built-in functions
	void Initialize();
public:
	void AddFunction(AggregateFunction function);
	void AddFunction(FunctionSet set);
	void AddFunction(ScalarFunction function);
	void AddFunction(TableFunction function);
private:
	Transaction &transaction;
	Catalog &catalog;
private:
	template<class T>
	void Register() {
		T::RegisterFunction(*this);
	}

	// table-producing functions
	void RegisterSQLiteFunctions();

	// aggregates
	void RegisterAlgebraicAggregates();
	void RegisterDistributiveAggregates();

	// scalar functions
	void RegisterDateFunctions();
	void RegisterMathFunctions();
	void RegisterOperators();
	void RegisterStringFunctions();
	void RegisterSequenceFunctions();
	void RegisterTrigonometricsFunctions();
};

} // namespace duckdb
