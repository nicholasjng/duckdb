#include "duckdb_python/pytype.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// NOLINTNEXTLINE(readability-identifier-naming)
bool PyGenericAlias::check_(const nb::handle &object) {
	if (!ModuleIsLoaded<TypesCacheItem>()) {
		return false;
	}
	auto &import_cache = *DuckDBPyConnection::import_Cache();
	return nb::isinstance(object, import_cache.types.GenericAlias());
}

// NOLINTNEXTLINE(readability-identifier-naming)
bool PyUnionType::check_(const nb::handle &object) {
	auto types_loaded = ModuleIsLoaded<TypesCacheItem>();
	auto typing_loaded = ModuleIsLoaded<TypingCacheItem>();

	if (!types_loaded && !typing_loaded) {
		return false;
	}

	auto &import_cache = *DuckDBPyConnection::import_Cache();
	if (types_loaded && nb::isinstance(object, import_cache.types.UnionType())) {
		return true;
	}
	if (typing_loaded && nb::isinstance(object, import_cache.typing._UnionGenericAlias())) {
		return true;
	}
	return false;
}

DuckDBPyType::DuckDBPyType(LogicalType type) : type(std::move(type)) {
}

bool DuckDBPyType::Equals(const shared_ptr<DuckDBPyType> &other) const {
	if (!other) {
		return false;
	}
	return type == other->type;
}

bool DuckDBPyType::EqualsString(const string &type_str) const {
	return StringUtil::CIEquals(type.ToString(), type_str);
}

shared_ptr<DuckDBPyType> DuckDBPyType::GetAttribute(const string &name) const {
	if (type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::UNION) {
		auto &children = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < children.size(); i++) {
			auto &child = children[i];
			if (StringUtil::CIEquals(child.first, name)) {
				return make_shared_ptr<DuckDBPyType>(StructType::GetChildType(type, i));
			}
		}
	}
	if (type.id() == LogicalTypeId::LIST && StringUtil::CIEquals(name, "child")) {
		return make_shared_ptr<DuckDBPyType>(ListType::GetChildType(type));
	}
	if (type.id() == LogicalTypeId::MAP) {
		auto is_key = StringUtil::CIEquals(name, "key");
		auto is_value = StringUtil::CIEquals(name, "value");
		if (is_key) {
			return make_shared_ptr<DuckDBPyType>(MapType::KeyType(type));
		} else if (is_value) {
			return make_shared_ptr<DuckDBPyType>(MapType::ValueType(type));
		} else {
			throw nb::attribute_error(StringUtil::Format("Tried to get a child from a map by the name of '%s', but "
			                                             "this type only has 'key' and 'value' children",
			                                             name));
		}
	}
	throw nb::attribute_error(
	    StringUtil::Format("Tried to get child type by the name of '%s', but this type either isn't nested, "
	                       "or it doesn't have a child by that name",
	                       name));
}

static LogicalType FromObject(const nb::object &object);

namespace {
enum class PythonTypeObject : uint8_t {
	INVALID,   // not convertible to our type
	BASE,      // 'builtin' type objects
	UNION,     // typing.UnionType
	COMPOSITE, // list|dict types
	STRUCT,    // dictionary
	STRING,    // string value
};
}

static PythonTypeObject GetTypeObjectType(const nb::handle &type_object) {
	if (nb::isinstance<nb::type>(type_object)) {
		return PythonTypeObject::BASE;
	}
	if (nb::isinstance<nb::str>(type_object)) {
		return PythonTypeObject::STRING;
	}
	if (nb::isinstance<PyGenericAlias>(type_object)) {
		return PythonTypeObject::COMPOSITE;
	}
	if (nb::isinstance<nb::dict>(type_object)) {
		return PythonTypeObject::STRUCT;
	}
	if (nb::isinstance<PyUnionType>(type_object)) {
		return PythonTypeObject::UNION;
	}
	return PythonTypeObject::INVALID;
}

static LogicalType FromString(const string &type_str, shared_ptr<DuckDBPyConnection> pycon) {
	if (!pycon) {
		pycon = DuckDBPyConnection::DefaultConnection();
	}
	auto &connection = pycon->con.GetConnection();
	return TransformStringToLogicalType(type_str, *connection.context);
}

static bool FromNumpyType(const nb::object &type, LogicalType &result) {
	// Since this is a type, we have to create an instance from it first.
	auto obj = type();
	// We convert these to string because the underlying physical
	// types of a numpy type aren't consistent on every platform
	string type_str = nb::str(obj.attr("dtype"));
	if (type_str == "bool") {
		result = LogicalType::BOOLEAN;
	} else if (type_str == "int8") {
		result = LogicalType::TINYINT;
	} else if (type_str == "uint8") {
		result = LogicalType::UTINYINT;
	} else if (type_str == "int16") {
		result = LogicalType::SMALLINT;
	} else if (type_str == "uint16") {
		result = LogicalType::USMALLINT;
	} else if (type_str == "int32") {
		result = LogicalType::INTEGER;
	} else if (type_str == "uint32") {
		result = LogicalType::UINTEGER;
	} else if (type_str == "int64") {
		result = LogicalType::BIGINT;
	} else if (type_str == "uint64") {
		result = LogicalType::UBIGINT;
	} else if (type_str == "float16") {
		// FIXME: should we even support this?
		result = LogicalType::FLOAT;
	} else if (type_str == "float32") {
		result = LogicalType::FLOAT;
	} else if (type_str == "float64") {
		result = LogicalType::DOUBLE;
	} else {
		return false;
	}
	return true;
}

static LogicalType FromType(const nb::type &obj) {
	nb::module_ builtins = nb::module_::import_("builtins");
	if (obj.is(builtins.attr("str"))) {
		return LogicalType::VARCHAR;
	}
	if (obj.is(builtins.attr("int"))) {
		return LogicalType::BIGINT;
	}
	if (obj.is(builtins.attr("bytearray"))) {
		return LogicalType::BLOB;
	}
	if (obj.is(builtins.attr("bytes"))) {
		return LogicalType::BLOB;
	}
	if (obj.is(builtins.attr("float"))) {
		return LogicalType::DOUBLE;
	}
	if (obj.is(builtins.attr("bool"))) {
		return LogicalType::BOOLEAN;
	}

	LogicalType result;
	if (FromNumpyType(obj, result)) {
		return result;
	}

	throw nb::type_error("Could not convert from unknown 'type' to DuckDBPyType");
}

static bool IsMapType(const nb::tuple &args) {
	if (args.size() != 2) {
		return false;
	}
	for (auto &arg : args) {
		if (GetTypeObjectType(arg) == PythonTypeObject::INVALID) {
			return false;
		}
	}
	return true;
}

static nb::tuple FilterNones(const nb::tuple &args) {
	nb::list result;

	for (const auto &arg : args) {
		nb::object object = nb::borrow<nb::object>(arg);
		if (object.is(nb::none().get_type())) {
			continue;
		}
		result.append(object);
	}
	return nb::tuple(result);
}

static LogicalType FromUnionTypeInternal(const nb::tuple &args) {
	idx_t index = 1;
	child_list_t<LogicalType> members;

	for (const auto &arg : args) {
		auto name = StringUtil::Format("u%d", index++);
		nb::object object = nb::borrow<nb::object>(arg);
		members.push_back(make_pair(name, FromObject(object)));
	}

	return LogicalType::UNION(std::move(members));
}

static LogicalType FromUnionType(const nb::object &obj) {
	nb::tuple args = obj.attr("__args__");

	// Optional inserts NoneType into the Union
	// all types are nullable in DuckDB so we just filter the Nones
	auto filtered_args = FilterNones(args);
	if (filtered_args.size() == 1) {
		// If only a single type is left, dont construct a UNION
		return FromObject(filtered_args[0]);
	}
	return FromUnionTypeInternal(filtered_args);
};

static LogicalType FromGenericAlias(const nb::object &obj) {
	nb::module_ builtins = nb::module_::import_("builtins");
	nb::module_ types = nb::module_::import_("types");
	auto generic_alias = types.attr("GenericAlias");
	D_ASSERT(nb::isinstance(obj, generic_alias));
	auto origin = obj.attr("__origin__");
	nb::tuple args = obj.attr("__args__");

	if (origin.is(builtins.attr("list"))) {
		if (args.size() != 1) {
			throw NotImplementedException("Can only create a LIST from a single type");
		}
		return LogicalType::LIST(FromObject(args[0]));
	}
	if (origin.is(builtins.attr("dict"))) {
		if (IsMapType(args)) {
			return LogicalType::MAP(FromObject(args[0]), FromObject(args[1]));
		} else {
			throw NotImplementedException("Can only create a MAP from a dict if args is formed correctly");
		}
	}
	string origin_type = nb::str(origin);
	throw InvalidInputException("Could not convert from '%s' to DuckDBPyType", origin_type);
}

static LogicalType FromDictionary(const nb::object &obj) {
	auto dict = nb::steal<nb::dict>(obj);
	child_list_t<LogicalType> children;
	children.reserve(dict.size());
	for (auto &item : dict) {
		auto &name_p = item.first;
		auto type_p = nb::borrow<nb::object>(item.second);
		string name = nb::str(name_p);
		auto type = FromObject(type_p);
		children.push_back(std::make_pair(name, std::move(type)));
	}
	return LogicalType::STRUCT(std::move(children));
}

static LogicalType FromObject(const nb::object &object) {
	auto object_type = GetTypeObjectType(object);
	switch (object_type) {
	case PythonTypeObject::BASE: {
		return FromType(object);
	}
	case PythonTypeObject::COMPOSITE: {
		return FromGenericAlias(object);
	}
	case PythonTypeObject::STRUCT: {
		return FromDictionary(object);
	}
	case PythonTypeObject::UNION: {
		return FromUnionType(object);
	}
	case PythonTypeObject::STRING: {
		auto string_value = std::string(nb::str(object));
		return FromString(string_value, nullptr);
	}
	default: {
		string actual_type = nb::str(object.get_type());
		throw NotImplementedException("Could not convert from object of type '%s' to DuckDBPyType", actual_type);
	}
	}
}

void DuckDBPyType::Initialize(nb::handle &m) {
	auto type_module = nb::class_<DuckDBPyType, shared_ptr<DuckDBPyType>>(m, "DuckDBPyType", nb::module_local());

	type_module.def("__repr__", &DuckDBPyType::ToString, "Stringified representation of the type object");
	type_module.def("__eq__", &DuckDBPyType::Equals, "Compare two types for equality", nb::arg("other"));
	type_module.def("__eq__", &DuckDBPyType::EqualsString, "Compare two types for equality", nb::arg("other"));
	type_module.def_property_readonly("id", &DuckDBPyType::GetId);
	type_module.def_property_readonly("children", &DuckDBPyType::Children);
	type_module.def(nb::init<>([](const string &type_str, shared_ptr<DuckDBPyConnection> connection = nullptr) {
		auto ltype = FromString(type_str, std::move(connection));
		return make_shared_ptr<DuckDBPyType>(ltype);
	}));
	type_module.def(nb::init<>([](const PyGenericAlias &obj) {
		auto ltype = FromGenericAlias(obj);
		return make_shared_ptr<DuckDBPyType>(ltype);
	}));
	type_module.def(nb::init<>([](const PyUnionType &obj) {
		auto ltype = FromUnionType(obj);
		return make_shared_ptr<DuckDBPyType>(ltype);
	}));
	type_module.def(nb::init<>([](const nb::object &obj) {
		auto ltype = FromObject(obj);
		return make_shared_ptr<DuckDBPyType>(ltype);
	}));
	type_module.def("__getattr__", &DuckDBPyType::GetAttribute, "Get the child type by 'name'", nb::arg("name"));
	type_module.def("__getitem__", &DuckDBPyType::GetAttribute, "Get the child type by 'name'", nb::arg("name"));

	nb::implicitly_convertible<nb::object, DuckDBPyType>();
	nb::implicitly_convertible<nb::str, DuckDBPyType>();
	nb::implicitly_convertible<PyGenericAlias, DuckDBPyType>();
	nb::implicitly_convertible<PyUnionType, DuckDBPyType>();
}

string DuckDBPyType::ToString() const {
	return type.ToString();
}

nb::list DuckDBPyType::Children() const {

	switch (type.id()) {
	case LogicalTypeId::LIST:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::UNION:
	case LogicalTypeId::MAP:
	case LogicalTypeId::ARRAY:
	case LogicalTypeId::ENUM:
	case LogicalTypeId::DECIMAL:
		break;
	default:
		throw InvalidInputException("This type is not nested so it doesn't have children");
	}

	nb::list children;
	auto id = type.id();
	if (id == LogicalTypeId::LIST) {
		children.append(nb::make_tuple("child", make_shared_ptr<DuckDBPyType>(ListType::GetChildType(type))));
		return children;
	}
	if (id == LogicalTypeId::ARRAY) {
		children.append(nb::make_tuple("child", make_shared_ptr<DuckDBPyType>(ArrayType::GetChildType(type))));
		children.append(nb::make_tuple("size", ArrayType::GetSize(type)));
		return children;
	}
	if (id == LogicalTypeId::ENUM) {
		auto &values_insert_order = EnumType::GetValuesInsertOrder(type);
		auto strings = FlatVector::GetData<string_t>(values_insert_order);
		nb::list strings_list;
		for (size_t i = 0; i < EnumType::GetSize(type); i++) {
			strings_list.append(nb::str(strings[i].GetString()));
		}
		children.append(nb::make_tuple("values", strings_list));
		return children;
	}
	if (id == LogicalTypeId::STRUCT || id == LogicalTypeId::UNION) {
		auto &struct_children = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < struct_children.size(); i++) {
			auto &child = struct_children[i];
			children.append(
			    nb::make_tuple(child.first, make_shared_ptr<DuckDBPyType>(StructType::GetChildType(type, i))));
		}
		return children;
	}
	if (id == LogicalTypeId::MAP) {
		children.append(nb::make_tuple("key", make_shared_ptr<DuckDBPyType>(MapType::KeyType(type))));
		children.append(nb::make_tuple("value", make_shared_ptr<DuckDBPyType>(MapType::ValueType(type))));
		return children;
	}
	if (id == LogicalTypeId::DECIMAL) {
		children.append(nb::make_tuple("precision", DecimalType::GetWidth(type)));
		children.append(nb::make_tuple("scale", DecimalType::GetScale(type)));
		return children;
	}
	throw InternalException("Children is not implemented for this type");
}

string DuckDBPyType::GetId() const {
	return StringUtil::Lower(LogicalTypeIdToString(type.id()));
}

const LogicalType &DuckDBPyType::Type() const {
	return type;
}

} // namespace duckdb
