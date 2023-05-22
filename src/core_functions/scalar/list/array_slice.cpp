#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

template <typename INPUT_TYPE, typename INDEX_TYPE>
INDEX_TYPE ValueLength(const INPUT_TYPE &value) {
	return 0;
}

template <>
int64_t ValueLength(const list_entry_t &value) {
	return value.length;
}

template <>
int32_t ValueLength(const string_t &value) {
	return LengthFun::Length<string_t, int32_t>(value);
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
bool ClampIndex(INDEX_TYPE &index, const INPUT_TYPE &value) {
	const auto length = ValueLength<INPUT_TYPE, INDEX_TYPE>(value);
	if (index < 0) {
		if (-index > length) {
			return false;
		}
		index = length + index;
	} else if (index > length) {
		index = length;
	}
	return true;
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
static bool ClampSlice(const INPUT_TYPE &value, INDEX_TYPE &begin, INDEX_TYPE &end, bool begin_valid, bool end_valid) {
	// Clamp offsets

	//	begin = begin_valid ? begin : 0;
	begin = (begin > 0) ? begin - 1 : begin;
	//	end = end_valid ? end : ValueLength<INPUT_TYPE, INDEX_TYPE>(value);
	if (!ClampIndex(begin, value) || !ClampIndex(end, value)) {
		return false;
	}
	end = MaxValue<INDEX_TYPE>(begin, end);

	return true;
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
INPUT_TYPE SliceValue(Vector &result, INPUT_TYPE input, INDEX_TYPE begin, INDEX_TYPE end) {
	return input;
}

template <>
list_entry_t SliceValue(Vector &result, list_entry_t input, int64_t begin, int64_t end) {

	input.offset += begin;
	input.length = end - begin;
	return input;
}

template <>
string_t SliceValue(Vector &result, string_t input, int32_t begin, int32_t end) {
	// one-based - zero has strange semantics

	return SubstringFun::SubstringUnicode(result, input, begin + 1, end - begin);
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
INPUT_TYPE SliceValueWithSteps(Vector &result, SelectionVector &sel, INPUT_TYPE input, INDEX_TYPE begin, INDEX_TYPE end,
                               INDEX_TYPE step, idx_t &sel_idx) {
	return input;
}

template <>
list_entry_t SliceValueWithSteps(Vector &result, SelectionVector &sel, list_entry_t input, int64_t begin, int64_t end,
                                 int64_t step, idx_t &sel_idx) {
	input.length = step < (end - begin) ? (end - begin + step) / step : 1;
	//	auto account_for_idx = 1;
	//	if (result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
	//		account_for_idx = 0;
	//	}
	idx_t child_idx = input.offset + begin;
	input.offset = sel_idx;
	for (idx_t i = 0; i < input.length; i++) {
		sel.set_index(sel_idx, child_idx);
		child_idx += step;
		sel_idx++;
	}
	return input;
}

template <>
string_t SliceValueWithSteps(Vector &result, SelectionVector &sel, string_t input, int32_t begin, int32_t end,
                             int32_t step, idx_t &sel_idx) {
	NotImplementedException("Slice with steps has not been implemented for string types, you can consider rewriting "
	                        "your query as follows: SELECT array_to_string((str_split('%s', '')[%i:%i:%i], '')",
	                        input.GetString(), begin, end, step);
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
INDEX_TYPE SliceLength(const INPUT_TYPE &value) {
	return 0;
}

template <>
int64_t SliceLength(const list_entry_t &value) {
	return value.length;
}

template <>
int32_t SliceLength(const string_t &value) {
	return value.GetSize();
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
static void ExecuteSlice(Vector &result, Vector &v, Vector &b, Vector &e, const idx_t count, optional_ptr<Vector> s) {
	optional_ptr<Vector> result_child_vector;
	if (s) {
		result_child_vector = &ListVector::GetEntry(result);
	}

	SelectionVector sel;
	idx_t sel_idx = 0;

	if (result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto rdata = ConstantVector::GetData<INPUT_TYPE>(result);
		auto vdata = ConstantVector::GetData<INPUT_TYPE>(v);
		auto bdata = ConstantVector::GetData<INDEX_TYPE>(b);
		auto edata = ConstantVector::GetData<INDEX_TYPE>(e);
		auto sdata = s ? ConstantVector::GetData<INDEX_TYPE>(*s) : nullptr;

		auto sliced = vdata[0];
		auto begin = bdata[0];
		auto end = edata[0];
		auto step = sdata ? sdata[0] : 1;

		if (begin == (INDEX_TYPE)NumericLimits<int64_t>::Maximum()) {
			begin = 0;
		}

		if (end == (INDEX_TYPE)NumericLimits<int64_t>::Maximum()) {
			end = SliceLength<INPUT_TYPE, INDEX_TYPE>(sliced);
		}

		auto vvalid = !ConstantVector::IsNull(v);
		auto bvalid = !ConstantVector::IsNull(b);
		auto evalid = !ConstantVector::IsNull(e);
		auto svalid = s && !ConstantVector::IsNull(*s);

		if (step == 0) {
			throw ValueOutOfRangeException("Slice step cannot be zero");
		}

		if (s && svalid && step > 0) {
			sel.Initialize((end - begin + step) / step);
		}

		// Try to slice
		if (!vvalid || !bvalid || !evalid || (s && !svalid) || step < 0 ||
		    !ClampSlice(sliced, begin, end, bvalid, evalid)) {
			ConstantVector::SetNull(result, true);
		} else if (step == 1) {
			rdata[0] = SliceValue<INPUT_TYPE, INDEX_TYPE>(result, sliced, begin, end);
		} else {
			rdata[0] = SliceValueWithSteps<INPUT_TYPE, INDEX_TYPE>(result, sel, sliced, begin, end, step, sel_idx);
		}

		if (s) {
			result_child_vector->Slice(sel, (end - begin + 1) / step);
		}
	} else {
		UnifiedVectorFormat vdata, bdata, edata, sdata;

		v.ToUnifiedFormat(count, vdata);
		b.ToUnifiedFormat(count, bdata);
		e.ToUnifiedFormat(count, edata);
		if (s) {
			s->ToUnifiedFormat(count, sdata);
		}

		idx_t new_size = 0;
		for (idx_t i = 0; s && i < count; ++i) {
			auto vidx = vdata.sel->get_index(i);
			auto bidx = bdata.sel->get_index(i);
			auto eidx = edata.sel->get_index(i);
			auto sidx = s ? sdata.sel->get_index(i) : 0;

			auto sliced = ((INPUT_TYPE *)vdata.data)[vidx];
			auto begin = ((INDEX_TYPE *)bdata.data)[bidx];
			auto end = ((INDEX_TYPE *)edata.data)[eidx];
			auto step = s ? ((INDEX_TYPE *)sdata.data)[sidx] : 1;

			if (begin == (INDEX_TYPE)NumericLimits<int64_t>::Maximum()) {
				begin = 0;
			}

			if (end == (INDEX_TYPE)NumericLimits<int64_t>::Maximum()) {
				end = SliceLength<INPUT_TYPE, INDEX_TYPE>(sliced);
			}

			if (step == 0) {
				throw ValueOutOfRangeException("Slice step cannot be zero");
			}

			if (s && !ClampSlice(sliced, begin, end, bidx, eidx)) {
				new_size += (end - begin + step) / step;
			} else {
				new_size += end - begin;
			}
		}

		if (s) {
			sel.Initialize(new_size);
		}

		auto rdata = FlatVector::GetData<INPUT_TYPE>(result);
		auto &rmask = FlatVector::Validity(result);

		for (idx_t i = 0; i < count; ++i) {
			auto vidx = vdata.sel->get_index(i);
			auto bidx = bdata.sel->get_index(i);
			auto eidx = edata.sel->get_index(i);
			auto sidx = s ? sdata.sel->get_index(i) : 0;

			auto sliced = ((INPUT_TYPE *)vdata.data)[vidx];
			auto begin = ((INDEX_TYPE *)bdata.data)[bidx];
			auto end = ((INDEX_TYPE *)edata.data)[eidx];
			auto step = s ? ((INDEX_TYPE *)sdata.data)[sidx] : 1;

			if (begin == (INDEX_TYPE)NumericLimits<int64_t>::Maximum()) {
				begin = 0;
			}

			if (end == (INDEX_TYPE)NumericLimits<int64_t>::Maximum()) {
				end = SliceLength<INPUT_TYPE, INDEX_TYPE>(sliced);
			}

			auto vvalid = vdata.validity.RowIsValid(vidx);
			auto bvalid = bdata.validity.RowIsValid(bidx);
			auto evalid = edata.validity.RowIsValid(eidx);
			auto svalid = s && sdata.validity.RowIsValid(sidx);

			if (!vvalid || !bvalid || !evalid || !svalid || step < 0 ||
			    !ClampSlice(sliced, begin, end, bvalid, evalid)) {
				rmask.SetInvalid(i);
			} else if (step == 1) {
				rdata[i] = SliceValue<INPUT_TYPE, INDEX_TYPE>(result, sliced, begin, end);
			} else {
				rdata[i] = SliceValueWithSteps<INPUT_TYPE, INDEX_TYPE>(result, sel, sliced, begin, end, step, sel_idx);
			}
		}
		if (s) {
			result_child_vector->Slice(sel, new_size);
		}
	}

	result.Verify(count);
}

static void ArraySliceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3 || args.ColumnCount() == 4);
	D_ASSERT(args.data.size() == 3 || args.data.size() == 4);
	auto count = args.size();

	Vector &v = args.data[0];
	Vector &b = args.data[1];
	Vector &e = args.data[2];

	optional_ptr<Vector> s;
	if (args.ColumnCount() == 4) {
		s = &args.data[3];
	}

	result.SetVectorType(args.AllConstant() ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	switch (result.GetType().id()) {
	case LogicalTypeId::LIST: {
		// Share the value dictionary as we are just going to slice it
		if (v.GetVectorType() != VectorType::FLAT_VECTOR && v.GetVectorType() != VectorType::CONSTANT_VECTOR) {
			v.Flatten(count);
		}
		ListVector::ReferenceEntry(result, v);
		ExecuteSlice<list_entry_t, int64_t>(result, v, b, e, count, s);
		break;
	}
	case LogicalTypeId::VARCHAR: {
		ExecuteSlice<string_t, int32_t>(result, v, b, e, count, s);
		break;
	}
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

static unique_ptr<FunctionData> ArraySliceBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 3 || arguments.size() == 4);
	D_ASSERT(bound_function.arguments.size() == 3 || bound_function.arguments.size() == 4);

	//	if (arguments[1]->expression_class == ExpressionClass::BOUND_CONSTANT && arguments[2]->expression_class ==
	// ExpressionClass::BOUND_CONSTANT) { 		auto &first_bound_const_expr =
	// arguments[1]->Cast<BoundConstantExpression>(); 		auto &second_bound_const_expr =
	//arguments[2]->Cast<BoundConstantExpression>(); 		if (first_bound_const_expr.value.GetValue<uint64_t>() ==
	//NumericLimits<int64_t>::Maximum() && second_bound_const_expr.value.GetValue<uint64_t>() ==
	// NumericLimits<int64_t>::Maximum()) {
	//            // If both begin and end are invalid, we can return the original list
	//            bound_function.return_type = LogicalType::LIST(arguments[0]->return_type);
	//            return ;
	//        }
	//	}

	switch (arguments[0]->return_type.id()) {
	case LogicalTypeId::LIST:
		// The result is the same type
		bound_function.return_type = arguments[0]->return_type;
		break;
	case LogicalTypeId::VARCHAR:
		// string slice returns a string, but can only accept 32 bit integers
		if (bound_function.arguments.size() == 4) {
			throw NotImplementedException(
			    "Slice with steps has not been implemented for string types, you can consider rewriting your query as "
			    "follows:\n SELECT array_to_string((str_split(string, '')[begin:end:step], '');");
		}
		bound_function.return_type = arguments[0]->return_type;
		for (idx_t i = 1; i < 3; i++) {
			if (arguments[i]->expression_class == ExpressionClass::BOUND_CONSTANT) {
				auto &bound_const_expr = arguments[i]->Cast<BoundConstantExpression>();
				if (bound_const_expr.value.GetValue<uint64_t>() == NumericLimits<int64_t>::Maximum()) {
					// If begin or end is empty cast to bigint
					bound_function.arguments[i] = LogicalType::BIGINT;
				} else {
					bound_function.arguments[i] = LogicalType::INTEGER;
				}
			}
		}
		break;
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::UNKNOWN:
		bound_function.arguments[0] = LogicalTypeId::UNKNOWN;
		bound_function.return_type = LogicalType::SQLNULL;
		break;
	default:
		throw BinderException("ARRAY_SLICE can only operate on LISTs and VARCHARs");
	}

	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunctionSet ListSliceFun::GetFunctions() {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun({LogicalType::ANY, LogicalType::BIGINT, LogicalType::BIGINT}, LogicalType::ANY,
	                   ArraySliceFunction, ArraySliceBind);
	//	fun.varargs = LogicalType::ANY; // Do we need this?
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;

	ScalarFunctionSet set;
	set.AddFunction(fun);
	fun.arguments.push_back(LogicalType::BIGINT);
	set.AddFunction(fun);
	return set;
}

} // namespace duckdb

// ./build/release/test/unittest
