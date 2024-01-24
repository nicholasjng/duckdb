#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/list_segment.hpp"
#include "duckdb/common/types/sel_cache.hpp"
#include "duckdb/core_functions/scalar/struct_functions.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"

namespace duckdb {

struct SortedAggregateBindData : public FunctionData {
	SortedAggregateBindData(ClientContext &context, BoundAggregateExpression &expr)
	    : buffer_manager(BufferManager::GetBufferManager(context)), function(expr.function),
	      bind_info(std::move(expr.bind_info)), threshold(ClientConfig::GetConfig(context).ordered_aggregate_threshold),
	      external(ClientConfig::GetConfig(context).force_external) {
		auto &children = expr.children;
		arg_types.reserve(children.size());
		arg_funcs.reserve(children.size());
		for (const auto &child : children) {
			arg_types.emplace_back(child->return_type);
			ListSegmentFunctions funcs;
			GetSegmentDataFunctions(funcs, arg_types.back());
			arg_funcs.emplace_back(funcs);
		}
		auto &order_bys = *expr.order_bys;
		sort_types.reserve(order_bys.orders.size());
		sort_funcs.reserve(order_bys.orders.size());
		for (auto &order : order_bys.orders) {
			orders.emplace_back(order.Copy());
			sort_types.emplace_back(order.expression->return_type);
			ListSegmentFunctions funcs;
			GetSegmentDataFunctions(funcs, sort_types.back());
			sort_funcs.emplace_back(funcs);
		}
		sorted_on_args = (children.size() == order_bys.orders.size());
		for (size_t i = 0; sorted_on_args && i < children.size(); ++i) {
			sorted_on_args = children[i]->Equals(*order_bys.orders[i].expression);
		}
	}

	SortedAggregateBindData(const SortedAggregateBindData &other)
	    : buffer_manager(other.buffer_manager), function(other.function), arg_types(other.arg_types),
	      arg_funcs(other.arg_funcs), sort_types(other.sort_types), sort_funcs(other.sort_funcs),
	      sorted_on_args(other.sorted_on_args), threshold(other.threshold), external(other.external) {
		if (other.bind_info) {
			bind_info = other.bind_info->Copy();
		}
		for (auto &order : other.orders) {
			orders.emplace_back(order.Copy());
		}
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<SortedAggregateBindData>(*this);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<SortedAggregateBindData>();
		if (bind_info && other.bind_info) {
			if (!bind_info->Equals(*other.bind_info)) {
				return false;
			}
		} else if (bind_info || other.bind_info) {
			return false;
		}
		if (function != other.function) {
			return false;
		}
		if (orders.size() != other.orders.size()) {
			return false;
		}
		for (size_t i = 0; i < orders.size(); ++i) {
			if (!orders[i].Equals(other.orders[i])) {
				return false;
			}
		}
		return true;
	}

	BufferManager &buffer_manager;
	AggregateFunction function;
	vector<LogicalType> arg_types;
	unique_ptr<FunctionData> bind_info;
	vector<ListSegmentFunctions> arg_funcs;

	vector<BoundOrderByNode> orders;
	vector<LogicalType> sort_types;
	vector<ListSegmentFunctions> sort_funcs;
	bool sorted_on_args;

	//! The sort flush threshold
	const idx_t threshold;
	const bool external;
};

struct SortedAggregateState {
	// Linked list equivalent of DataChunk
	using LinkedLists = vector<LinkedList>;
	using LinkedChunkFunctions = vector<ListSegmentFunctions>;

	//! Capacities of the various levels of buffering
	static const idx_t LIST_CAPACITY = 16;
	static const idx_t CHUNK_CAPACITY = STANDARD_VECTOR_SIZE;

	SortedAggregateState() : count(0), nsel(0), offset(0) {
	}

	static inline void InitializeLinkedList(LinkedLists &linked, const vector<LogicalType> &types) {
		if (linked.empty() && !types.empty()) {
			linked.resize(types.size(), LinkedList());
		}
	}

	inline void InitializeLinkedLists(const SortedAggregateBindData &order_bind) {
		InitializeLinkedList(sort_linked, order_bind.sort_types);
		if (!order_bind.sorted_on_args) {
			InitializeLinkedList(arg_linked, order_bind.arg_types);
		}
	}

	static inline void InitializeChunk(unique_ptr<DataChunk> &chunk, const vector<LogicalType> &types) {
		if (!chunk && !types.empty()) {
			chunk = make_uniq<DataChunk>();
			chunk->Initialize(Allocator::DefaultAllocator(), types);
		}
	}

	void InitializeChunks(const SortedAggregateBindData &order_bind) {
		// Lazy instantiation of the buffer chunks
		InitializeChunk(sort_chunk, order_bind.sort_types);
		if (!order_bind.sorted_on_args) {
			InitializeChunk(arg_chunk, order_bind.arg_types);
		}
	}

	static inline void FlushLinkedList(const LinkedChunkFunctions &funcs, LinkedLists &linked, DataChunk &chunk) {
		idx_t total_count = 0;
		for (column_t i = 0; i < linked.size(); ++i) {
			funcs[i].BuildListVector(linked[i], chunk.data[i], total_count);
			chunk.SetCardinality(linked[i].total_capacity);
		}
	}

	void FlushLinkedLists(const SortedAggregateBindData &order_bind) {
		InitializeChunks(order_bind);
		FlushLinkedList(order_bind.sort_funcs, sort_linked, *sort_chunk);
		if (arg_chunk) {
			FlushLinkedList(order_bind.arg_funcs, arg_linked, *arg_chunk);
		}
	}

	void InitializeCollections(const SortedAggregateBindData &order_bind) {
		ordering = make_uniq<ColumnDataCollection>(order_bind.buffer_manager, order_bind.sort_types);
		ordering_append = make_uniq<ColumnDataAppendState>();
		ordering->InitializeAppend(*ordering_append);

		if (!order_bind.sorted_on_args) {
			arguments = make_uniq<ColumnDataCollection>(order_bind.buffer_manager, order_bind.arg_types);
			arguments_append = make_uniq<ColumnDataAppendState>();
			arguments->InitializeAppend(*arguments_append);
		}
	}

	void FlushChunks(const SortedAggregateBindData &order_bind) {
		D_ASSERT(sort_chunk);
		ordering->Append(*ordering_append, *sort_chunk);
		sort_chunk->Reset();

		if (arguments) {
			D_ASSERT(arg_chunk);
			arguments->Append(*arguments_append, *arg_chunk);
			arg_chunk->Reset();
		}
	}

	void Resize(const SortedAggregateBindData &order_bind, idx_t n) {
		count = n;

		//	Establish the current buffering
		if (count <= LIST_CAPACITY) {
			InitializeLinkedLists(order_bind);
		}

		if (count > LIST_CAPACITY && !sort_chunk && !ordering) {
			FlushLinkedLists(order_bind);
		}

		if (count > CHUNK_CAPACITY && !ordering) {
			InitializeCollections(order_bind);
			FlushChunks(order_bind);
		}
	}

	static void LinkedAppend(const LinkedChunkFunctions &functions, ArenaAllocator &allocator, DataChunk &input,
	                         LinkedLists &linked, SelectionVector &sel, idx_t nsel) {
		const auto count = input.size();
		for (column_t c = 0; c < input.ColumnCount(); ++c) {
			auto &func = functions[c];
			auto &linked_list = linked[c];
			RecursiveUnifiedVectorFormat input_data;
			Vector::RecursiveToUnifiedFormat(input.data[c], count, input_data);
			for (idx_t i = 0; i < nsel; ++i) {
				idx_t sidx = sel.get_index(i);
				func.AppendRow(allocator, linked_list, input_data, sidx);
			}
		}
	}

	static void LinkedAbsorb(LinkedLists &source, LinkedLists &target) {
		D_ASSERT(source.size() == target.size());
		for (column_t i = 0; i < source.size(); ++i) {
			auto &src = source[i];
			if (!src.total_capacity) {
				break;
			}

			auto &tgt = target[i];
			if (!tgt.total_capacity) {
				tgt = src;
			} else {
				// append the linked list
				tgt.last_segment->next = src.first_segment;
				tgt.last_segment = src.last_segment;
				tgt.total_capacity += src.total_capacity;
			}
		}
	}

	void Update(const AggregateInputData &aggr_input_data, DataChunk &sort_input, DataChunk &arg_input) {
		const auto &order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		Resize(order_bind, count + sort_input.size());

		sel.Initialize(nullptr);
		nsel = sort_input.size();

		if (ordering) {
			//	Using collections
			ordering->Append(*ordering_append, sort_input);
			if (arguments) {
				arguments->Append(*arguments_append, arg_input);
			}
		} else if (sort_chunk) {
			//	Still using data chunks
			sort_chunk->Append(sort_input);
			if (arg_chunk) {
				arg_chunk->Append(arg_input);
			}
		} else {
			//	Still using linked lists
			LinkedAppend(order_bind.sort_funcs, aggr_input_data.allocator, sort_input, sort_linked, sel, nsel);
			if (!arg_linked.empty()) {
				LinkedAppend(order_bind.arg_funcs, aggr_input_data.allocator, arg_input, arg_linked, sel, nsel);
			}
		}

		nsel = 0;
		offset = 0;
	}

	void UpdateSlice(const AggregateInputData &aggr_input_data, DataChunk &sort_input, DataChunk &arg_input) {
		const auto &order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		Resize(order_bind, count + nsel);

		if (ordering) {
			//	Using collections
			D_ASSERT(sort_chunk);
			sort_chunk->Slice(sort_input, sel, nsel);
			if (arg_chunk) {
				arg_chunk->Slice(arg_input, sel, nsel);
			}
			FlushChunks(order_bind);
		} else if (sort_chunk) {
			//	Still using data chunks
			sort_chunk->Append(sort_input, true, &sel, nsel);
			if (arg_chunk) {
				arg_chunk->Append(arg_input, true, &sel, nsel);
			}
		} else {
			//	Still using linked lists
			LinkedAppend(order_bind.sort_funcs, aggr_input_data.allocator, sort_input, sort_linked, sel, nsel);
			if (!arg_linked.empty()) {
				LinkedAppend(order_bind.arg_funcs, aggr_input_data.allocator, arg_input, arg_linked, sel, nsel);
			}
		}

		nsel = 0;
		offset = 0;
	}

	void Swap(SortedAggregateState &other) {
		std::swap(count, other.count);

		std::swap(arguments, other.arguments);
		std::swap(arguments_append, other.arguments_append);
		std::swap(ordering, other.ordering);
		std::swap(ordering_append, other.ordering_append);

		std::swap(sort_chunk, other.sort_chunk);
		std::swap(arg_chunk, other.arg_chunk);

		std::swap(sort_linked, other.sort_linked);
		std::swap(arg_linked, other.arg_linked);
	}

	void Absorb(const SortedAggregateBindData &order_bind, SortedAggregateState &other) {
		if (!other.count) {
			return;
		} else if (!count) {
			Swap(other);
			return;
		}

		//	Change to a state large enough for all the data
		Resize(order_bind, count + other.count);

		//	3x3 matrix.
		//	We can simplify the logic a bit because the target is already set for the final capacity
		if (!sort_chunk) {
			//	If the combined count is still linked lists,
			//	then just move the pointers.
			//	Note that this assumes ArenaAllocator is shared and the memory will not vanish under us.
			LinkedAbsorb(other.sort_linked, sort_linked);
			if (!arg_linked.empty()) {
				LinkedAbsorb(other.arg_linked, arg_linked);
			}

			other.Reset();
			return;
		}

		if (!other.sort_chunk) {
			other.FlushLinkedLists(order_bind);
		}

		if (!ordering) {
			//	Still using chunks, which means the source is using chunks or lists
			D_ASSERT(sort_chunk);
			D_ASSERT(other.sort_chunk);
			sort_chunk->Append(*other.sort_chunk);
			if (arg_chunk) {
				D_ASSERT(other.arg_chunk);
				arg_chunk->Append(*other.arg_chunk);
			}
		} else {
			// Using collections, so source could be using anything.
			if (other.ordering) {
				ordering->Combine(*other.ordering);
				if (arguments) {
					D_ASSERT(other.arguments);
					arguments->Combine(*other.arguments);
				}
			} else {
				ordering->Append(*other.sort_chunk);
				if (arguments) {
					D_ASSERT(other.arg_chunk);
					arguments->Append(*other.arg_chunk);
				}
			}
		}

		//	Free all memory as we have absorbed it.
		other.Reset();
	}

	void PrefixSortBuffer(DataChunk &prefixed) {
		for (column_t col_idx = 0; col_idx < sort_chunk->ColumnCount(); ++col_idx) {
			prefixed.data[col_idx + 1].Reference(sort_chunk->data[col_idx]);
		}
		prefixed.SetCardinality(*sort_chunk);
	}

	void Finalize(const SortedAggregateBindData &order_bind, DataChunk &prefixed, LocalSortState &local_sort) {
		if (arguments) {
			ColumnDataScanState sort_state;
			ordering->InitializeScan(sort_state);
			ColumnDataScanState arg_state;
			arguments->InitializeScan(arg_state);
			for (sort_chunk->Reset(); ordering->Scan(sort_state, *sort_chunk); sort_chunk->Reset()) {
				PrefixSortBuffer(prefixed);
				arg_chunk->Reset();
				arguments->Scan(arg_state, *arg_chunk);
				local_sort.SinkChunk(prefixed, *arg_chunk);
			}
		} else if (ordering) {
			ColumnDataScanState sort_state;
			ordering->InitializeScan(sort_state);
			for (sort_chunk->Reset(); ordering->Scan(sort_state, *sort_chunk); sort_chunk->Reset()) {
				PrefixSortBuffer(prefixed);
				local_sort.SinkChunk(prefixed, *sort_chunk);
			}
		} else {
			//	Force chunks so we can sort
			if (!sort_chunk) {
				FlushLinkedLists(order_bind);
			}

			PrefixSortBuffer(prefixed);
			if (arg_chunk) {
				local_sort.SinkChunk(prefixed, *arg_chunk);
			} else {
				local_sort.SinkChunk(prefixed, *sort_chunk);
			}
		}

		Reset();
	}

	void Reset() {
		//	Release all memory
		ordering.reset();
		arguments.reset();

		sort_chunk.reset();
		arg_chunk.reset();

		sort_linked.clear();
		arg_linked.clear();

		count = 0;
	}

	idx_t count;

	unique_ptr<ColumnDataCollection> arguments;
	unique_ptr<ColumnDataAppendState> arguments_append;
	unique_ptr<ColumnDataCollection> ordering;
	unique_ptr<ColumnDataAppendState> ordering_append;

	unique_ptr<DataChunk> sort_chunk;
	unique_ptr<DataChunk> arg_chunk;

	LinkedLists sort_linked;
	LinkedLists arg_linked;

	// Selection for scattering
	SelectionVector sel;
	idx_t nsel;
	idx_t offset;
};

struct SortedAggregateFunction {
	template <typename STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <typename STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~STATE();
	}

	static void ProjectInputs(Vector inputs[], const SortedAggregateBindData &order_bind, idx_t input_count,
	                          idx_t count, DataChunk &arg_input, DataChunk &sort_input) {
		idx_t col = 0;

		if (!order_bind.sorted_on_args) {
			arg_input.InitializeEmpty(order_bind.arg_types);
			for (auto &dst : arg_input.data) {
				dst.Reference(inputs[col++]);
			}
			arg_input.SetCardinality(count);
		}

		sort_input.InitializeEmpty(order_bind.sort_types);
		for (auto &dst : sort_input.data) {
			dst.Reference(inputs[col++]);
		}
		sort_input.SetCardinality(count);
	}

	static void SimpleUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, data_ptr_t state,
	                         idx_t count) {
		const auto order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		DataChunk arg_input;
		DataChunk sort_input;
		ProjectInputs(inputs, order_bind, input_count, count, arg_input, sort_input);

		const auto order_state = reinterpret_cast<SortedAggregateState *>(state);
		order_state->Update(aggr_input_data, sort_input, arg_input);
	}

	static void ScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &states,
	                          idx_t count) {
		if (!count) {
			return;
		}

		// Append the arguments to the two sub-collections
		const auto &order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		DataChunk arg_inputs;
		DataChunk sort_inputs;
		ProjectInputs(inputs, order_bind, input_count, count, arg_inputs, sort_inputs);

		// We have to scatter the chunks one at a time
		// so build a selection vector for each one.
		UnifiedVectorFormat svdata;
		states.ToUnifiedFormat(count, svdata);

		// Size the selection vector for each state.
		auto sdata = UnifiedVectorFormat::GetDataNoConst<SortedAggregateState *>(svdata);
		for (idx_t i = 0; i < count; ++i) {
			auto sidx = svdata.sel->get_index(i);
			auto order_state = sdata[sidx];
			order_state->nsel++;
		}

		// Build the selection vector for each state.
		vector<sel_t> sel_data(count);
		idx_t start = 0;
		for (idx_t i = 0; i < count; ++i) {
			auto sidx = svdata.sel->get_index(i);
			auto order_state = sdata[sidx];
			if (!order_state->offset) {
				//	First one
				order_state->offset = start;
				order_state->sel.Initialize(sel_data.data() + order_state->offset);
				start += order_state->nsel;
			}
			sel_data[order_state->offset++] = sidx;
		}

		// Append nonempty slices to the arguments
		for (idx_t i = 0; i < count; ++i) {
			auto sidx = svdata.sel->get_index(i);
			auto order_state = sdata[sidx];
			if (!order_state->nsel) {
				continue;
			}

			order_state->UpdateSlice(aggr_input_data, sort_inputs, arg_inputs);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		auto &order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		auto &other = const_cast<STATE &>(source);
		target.Absorb(order_bind, other);
	}

	static void Window(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
	                   const_data_ptr_t g_state, data_ptr_t l_state, const SubFrames &subframes, Vector &result,
	                   idx_t rid) {
		throw InternalException("Sorted aggregates should not be generated for window clauses");
	}

	static void Finalize(Vector &states, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
	                     const idx_t offset) {
		auto &order_bind = aggr_input_data.bind_data->Cast<SortedAggregateBindData>();
		auto &buffer_manager = order_bind.buffer_manager;
		RowLayout payload_layout;
		payload_layout.Initialize(order_bind.arg_types);
		DataChunk chunk;
		chunk.Initialize(Allocator::DefaultAllocator(), order_bind.arg_types);
		DataChunk sliced;
		sliced.Initialize(Allocator::DefaultAllocator(), order_bind.arg_types);

		//	 Reusable inner state
		vector<data_t> agg_state(order_bind.function.state_size());
		Vector agg_state_vec(Value::POINTER(CastPointerToValue(agg_state.data())));

		// State variables
		auto bind_info = order_bind.bind_info.get();
		AggregateInputData aggr_bind_info(bind_info, aggr_input_data.allocator);

		// Inner aggregate APIs
		auto initialize = order_bind.function.initialize;
		auto destructor = order_bind.function.destructor;
		auto simple_update = order_bind.function.simple_update;
		auto update = order_bind.function.update;
		auto finalize = order_bind.function.finalize;

		auto sdata = FlatVector::GetData<SortedAggregateState *>(states);

		vector<idx_t> state_unprocessed(count, 0);
		for (idx_t i = 0; i < count; ++i) {
			state_unprocessed[i] = sdata[i]->count;
		}

		// Sort the input payloads on (state_idx ASC, orders)
		vector<BoundOrderByNode> orders;
		orders.emplace_back(BoundOrderByNode(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST,
		                                     make_uniq<BoundConstantExpression>(Value::USMALLINT(0))));
		for (const auto &order : order_bind.orders) {
			orders.emplace_back(order.Copy());
		}

		auto global_sort = make_uniq<GlobalSortState>(buffer_manager, orders, payload_layout);
		global_sort->external = order_bind.external;
		auto local_sort = make_uniq<LocalSortState>();
		local_sort->Initialize(*global_sort, global_sort->buffer_manager);

		DataChunk prefixed;
		prefixed.Initialize(Allocator::DefaultAllocator(), global_sort->sort_layout.logical_types);

		//	Go through the states accumulating values to sort until we hit the sort threshold
		idx_t unsorted_count = 0;
		idx_t sorted = 0;
		for (idx_t finalized = 0; finalized < count;) {
			if (unsorted_count < order_bind.threshold) {
				auto state = sdata[finalized];
				prefixed.Reset();
				prefixed.data[0].Reference(Value::USMALLINT(finalized));
				state->Finalize(order_bind, prefixed, *local_sort);
				unsorted_count += state_unprocessed[finalized];

				// Go to the next aggregate unless this is the last one
				if (++finalized < count) {
					continue;
				}
			}

			//	If they were all empty (filtering) flush them
			//	(This can only happen on the last range)
			if (!unsorted_count) {
				break;
			}

			//	Sort all the data
			global_sort->AddLocalState(*local_sort);
			global_sort->PrepareMergePhase();
			while (global_sort->sorted_blocks.size() > 1) {
				global_sort->InitializeMergeRound();
				MergeSorter merge_sorter(*global_sort, global_sort->buffer_manager);
				merge_sorter.PerformInMergeRound();
				global_sort->CompleteMergeRound(false);
			}

			auto scanner = make_uniq<PayloadScanner>(*global_sort);
			initialize(agg_state.data());
			while (scanner->Remaining()) {
				chunk.Reset();
				scanner->Scan(chunk);
				idx_t consumed = 0;

				// Distribute the scanned chunk to the aggregates
				while (consumed < chunk.size()) {
					//	Find the next aggregate that needs data
					for (; !state_unprocessed[sorted]; ++sorted) {
						// Finalize a single value at the next offset
						agg_state_vec.SetVectorType(states.GetVectorType());
						finalize(agg_state_vec, aggr_bind_info, result, 1, sorted + offset);
						if (destructor) {
							destructor(agg_state_vec, aggr_bind_info, 1);
						}

						initialize(agg_state.data());
					}
					const auto input_count = MinValue(state_unprocessed[sorted], chunk.size() - consumed);
					for (column_t col_idx = 0; col_idx < chunk.ColumnCount(); ++col_idx) {
						sliced.data[col_idx].Slice(chunk.data[col_idx], consumed, consumed + input_count);
					}
					sliced.SetCardinality(input_count);

					// These are all simple updates, so use it if available
					if (simple_update) {
						simple_update(sliced.data.data(), aggr_bind_info, sliced.data.size(), agg_state.data(),
						              sliced.size());
					} else {
						// We are only updating a constant state
						agg_state_vec.SetVectorType(VectorType::CONSTANT_VECTOR);
						update(sliced.data.data(), aggr_bind_info, sliced.data.size(), agg_state_vec, sliced.size());
					}

					consumed += input_count;
					state_unprocessed[sorted] -= input_count;
				}
			}

			//	Finalize the last state for this sort
			agg_state_vec.SetVectorType(states.GetVectorType());
			finalize(agg_state_vec, aggr_bind_info, result, 1, sorted + offset);
			if (destructor) {
				destructor(agg_state_vec, aggr_bind_info, 1);
			}
			++sorted;

			//	Stop if we are done
			if (finalized >= count) {
				break;
			}

			//	Create a new sort
			scanner.reset();
			global_sort = make_uniq<GlobalSortState>(buffer_manager, orders, payload_layout);
			global_sort->external = order_bind.external;
			local_sort = make_uniq<LocalSortState>();
			local_sort->Initialize(*global_sort, global_sort->buffer_manager);
			unsorted_count = 0;
		}

		for (; sorted < count; ++sorted) {
			initialize(agg_state.data());

			// Finalize a single value at the next offset
			agg_state_vec.SetVectorType(states.GetVectorType());
			finalize(agg_state_vec, aggr_bind_info, result, 1, sorted + offset);

			if (destructor) {
				destructor(agg_state_vec, aggr_bind_info, 1);
			}
		}

		result.Verify(count);
	}
};

struct SortKeyBindData : public FunctionData {
	explicit SortKeyBindData(const vector<BoundOrderByNode> &order_bys) {
		for (auto &order : order_bys) {
			orders.emplace_back(order.Copy());
		}
	}

	SortKeyBindData(const SortKeyBindData &other) {
		for (auto &order : other.orders) {
			orders.emplace_back(order.Copy());
		}
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<SortKeyBindData>(*this);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<SortKeyBindData>();
		if (orders.size() != other.orders.size()) {
			return false;
		}
		for (size_t i = 0; i < orders.size(); ++i) {
			if (!orders[i].Equals(other.orders[i])) {
				return false;
			}
		}
		return true;
	}

	vector<BoundOrderByNode> orders;
};

struct SortKeyLocalData : public FunctionLocalState {
	explicit SortKeyLocalData(const SortKeyBindData &bind_data) : sort_layout(bind_data.orders) {
	}

	SortLayout sort_layout;
};

struct SortKeyFun {

	static unique_ptr<FunctionLocalState> InitLocalState(ExpressionState &state, const BoundFunctionExpression &expr,
	                                                     FunctionData *bind_data_p) {
		return make_uniq<SortKeyLocalData>(bind_data_p->Cast<SortKeyBindData>());
	}

	static void Execute(DataChunk &sort, ExpressionState &state, Vector &result) {
		const auto count = sort.size();
		auto &function_state = state.Cast<ExecuteFunctionState>();
		auto &sort_key_state = function_state.local_state->Cast<SortKeyLocalData>();
		auto &sort_layout = sort_key_state.sort_layout;

		// Build and serialize sorting data to radix sortable rows
		data_ptr_t data_pointers[STANDARD_VECTOR_SIZE];
		Vector addresses(LogicalType::POINTER, reinterpret_cast<data_ptr_t>(data_pointers));

		//	Allocate the strings and point into them
		const auto key_size = sort_layout.entry_size;
		auto rdata = FlatVector::GetData<string_t>(result);
		for (idx_t i = 0; i < count; ++i) {
			auto blob = StringVector::EmptyString(result, key_size);
			data_pointers[i] = reinterpret_cast<data_ptr_t>(blob.GetDataWriteable());
			rdata[i] = blob;
		}

		//	Scatter the keys
		const SelectionVector &sel_ptr = *FlatVector::IncrementalSelectionVector();
		for (column_t sort_col = 0; sort_col < sort_layout.column_count; ++sort_col) {
			bool has_null = sort_layout.has_null[sort_col];
			bool nulls_first = sort_layout.order_by_null_types[sort_col] == OrderByNullType::NULLS_FIRST;
			bool desc = sort_layout.order_types[sort_col] == OrderType::DESCENDING;
			RowOperations::RadixScatter(sort.data[sort_col], count, sel_ptr, count, data_pointers, desc, has_null,
			                            nulls_first, sort_layout.prefix_lengths[sort_col],
			                            sort_layout.column_sizes[sort_col]);
		}

		//	Finalize the keys
		for (idx_t i = 0; i < count; ++i) {
			rdata[i].Finalize();
		}
	}

	static ScalarFunction GetFunction(const vector<BoundOrderByNode> &orders) {
		vector<LogicalType> arguments;
		for (const auto &order : orders) {
			arguments.emplace_back(order.expression->return_type);
		}

		return ScalarFunction("sort_key", arguments, LogicalTypeId::BLOB, Execute, nullptr, nullptr, nullptr,
		                      InitLocalState);
	}

	static unique_ptr<BoundFunctionExpression> Bind(ClientContext &context, vector<BoundOrderByNode> &orders) {
		auto sort_key_func = SortKeyFun::GetFunction(orders);
		auto sort_key_bind_data = make_uniq<SortKeyBindData>(orders);
		vector<unique_ptr<Expression>> children;
		for (auto &order : orders) {
			children.emplace_back(std::move(order.expression));
		}
		orders.clear();

		FunctionBinder function_binder(context);
		auto bound_sort_key = function_binder.BindScalarFunction(sort_key_func, std::move(children), false);
		bound_sort_key->bind_info = std::move(sort_key_bind_data);

		return bound_sort_key;
	}
};

struct CompareAggregateBindData : public FunctionData {
	explicit CompareAggregateBindData(BoundAggregateExpression &expr)
	    : function(expr.function), child_bind(std::move(expr.bind_info)) {
	}

	CompareAggregateBindData(const CompareAggregateBindData &other) : function(other.function) {
		if (other.child_bind) {
			child_bind = other.child_bind->Copy();
		}
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CompareAggregateBindData>(*this);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CompareAggregateBindData>();
		if (child_bind && other.child_bind) {
			if (!child_bind->Equals(*other.child_bind)) {
				return false;
			}
		} else if (child_bind || other.child_bind) {
			return false;
		}
		if (function != other.function) {
			return false;
		}
		return true;
	}

	AggregateFunction function;
	unique_ptr<FunctionData> child_bind;
};

struct CompareAggregateState {
	CompareAggregateState() : val(nullptr), key(uint32_t(0)), key_data(nullptr) {
	}

	inline void Initialize(CompareAggregateBindData &compare_bind, ArenaAllocator &allocator) {
		val = allocator.Allocate(compare_bind.function.state_size());
		compare_bind.function.initialize(val);
	}

	inline int Compare(CompareAggregateBindData &compare_bind, ArenaAllocator &allocator, const string_t &right_key) {
		if (!val) {
			Initialize(compare_bind, allocator);
			return -1; // Uninitialized is before all keys
		} else {
			return key < right_key;
		}
	}

	inline void UpdateKey(ArenaAllocator &allocator, const string_t &right_key) {
		if (right_key.IsInlined()) {
			key = right_key;
			return;
		}

		//	Need storage
		const auto key_size = key.GetSize();
		const auto right_size = right_key.GetSize();
		if (!key_data) {
			key_data = allocator.Allocate(right_size);
		} else if (key_size < right_size) {
			key_data = allocator.Reallocate(key_data, key_size, right_size);
		}

		::memcpy(key_data, right_key.GetData(), right_size);
		key = string_t(reinterpret_cast<char *>(key_data), right_size);
	}

	void Absorb(CompareAggregateBindData &compare_bind, ArenaAllocator &allocator, CompareAggregateState &other) {
		if (!other.val) {
			return;
		} else if (!val) {
			std::swap(*this, other);
		} else {
			const auto cmp = Compare(compare_bind, allocator, other.key);
			if (cmp < 0) {
				//	Our key is smaller, so update
				std::swap(*this, other);
			}
		}
	}

	data_ptr_t val;
	string_t key;
	data_ptr_t key_data;
};

struct CombineAggregateFunction {
	template <typename STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <typename STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~STATE();
	}

	template <bool SKIP_NULLS>
	static void SimpleUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                         data_ptr_t state_p, idx_t count) {
		auto &compare_bind = aggr_input_data.bind_data->Cast<CompareAggregateBindData>();
		auto &allocator = aggr_input_data.allocator;
		const auto child_cols = input_count - 1;

		auto state = reinterpret_cast<CompareAggregateState *>(state_p);

		UnifiedVectorFormat avdata, kvdata;
		if (SKIP_NULLS) {
			inputs[0].ToUnifiedFormat(count, avdata);
		}
		inputs[1].ToUnifiedFormat(count, kvdata);
		auto kdata = UnifiedVectorFormat::GetDataNoConst<string_t>(kvdata);

		sel_t best_idx = count;
		for (idx_t i = 0; i < count; ++i) {
			if (SKIP_NULLS && !avdata.validity.AllValid()) {
				const auto aidx = avdata.sel->get_index(i);
				if (!avdata.validity.RowIsValid(aidx)) {
					continue;
				}
			}

			const auto kidx = kvdata.sel->get_index(i);
			const auto right_key = kdata[kidx];
			const auto cmp = state->Compare(compare_bind, allocator, right_key);
			if (cmp < 0) {
				//	Old key is smaller, so update
				best_idx = i;
				state->UpdateKey(allocator, right_key);
			}
		}

		if (best_idx >= count) {
			return;
		}

		SelectionVector sel(&best_idx);
		SelCache merge_cache;
		for (idx_t i = 0; i < child_cols; ++i) {
			inputs[i].Slice(sel, 1, merge_cache);
		}
		AggregateInputData child_aggr_data(compare_bind.child_bind, aggr_input_data.allocator,
		                                   AggregateCombineType::ALLOW_DESTRUCTIVE);

		// These are all simple updates, so use it if available
		auto simple_update = compare_bind.function.simple_update;
		if (simple_update) {
			simple_update(inputs, child_aggr_data, child_cols, state->val, 1);
		} else {
			// We are only updating a constant state
			Vector agg_state_vec(Value::POINTER(CastPointerToValue(state->val)));
			agg_state_vec.SetVectorType(VectorType::CONSTANT_VECTOR);
			compare_bind.function.update(inputs, child_aggr_data, child_cols, agg_state_vec, 1);
		}
	}

	template <bool SKIP_NULLS>
	static void ScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &states,
	                          idx_t count) {
		auto &compare_bind = aggr_input_data.bind_data->Cast<CompareAggregateBindData>();
		auto &allocator = aggr_input_data.allocator;
		const auto child_cols = (input_count - 1);

		UnifiedVectorFormat avdata, kvdata, svdata;
		if (SKIP_NULLS) {
			inputs[0].ToUnifiedFormat(count, avdata);
		}
		inputs[1].ToUnifiedFormat(count, kvdata);
		auto kdata = UnifiedVectorFormat::GetDataNoConst<string_t>(kvdata);

		states.ToUnifiedFormat(count, svdata);
		auto sdata = UnifiedVectorFormat::GetDataNoConst<CompareAggregateState *>(svdata);

		data_ptr_t child_state_ptrs[STANDARD_VECTOR_SIZE];
		Vector child_states(LogicalType::POINTER, reinterpret_cast<data_ptr_t>(child_state_ptrs));
		sel_t seldata[STANDARD_VECTOR_SIZE];
		SelectionVector sel(seldata);
		idx_t nsel = 0;
		for (idx_t i = 0; i < count; ++i) {
			if (SKIP_NULLS && !avdata.validity.AllValid()) {
				const auto aidx = avdata.sel->get_index(i);
				if (!avdata.validity.RowIsValid(aidx)) {
					continue;
				}
			}

			const auto kidx = kvdata.sel->get_index(i);
			const auto right_key = kdata[kidx];

			const auto sidx = svdata.sel->get_index(i);
			auto state = sdata[sidx];
			const auto cmp = state->Compare(compare_bind, allocator, right_key);
			if (cmp < 0) {
				//	Old key is smaller, so update
				child_state_ptrs[nsel] = state->val;
				seldata[nsel++] = i;
				state->UpdateKey(allocator, right_key);
			}
		}

		//	Update the winners
		SelCache merge_cache;
		for (idx_t i = 0; i < child_cols; ++i) {
			inputs[i].Slice(sel, nsel, merge_cache);
		}
		AggregateInputData child_aggr_data(compare_bind.child_bind, aggr_input_data.allocator,
		                                   AggregateCombineType::ALLOW_DESTRUCTIVE);
		compare_bind.function.update(inputs, child_aggr_data, child_cols, child_states, nsel);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		auto &compare_bind = aggr_input_data.bind_data->Cast<CompareAggregateBindData>();
		auto &allocator = aggr_input_data.allocator;
		auto &other = const_cast<STATE &>(source);
		target.Absorb(compare_bind, allocator, other);
	}

	static void Window(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
	                   const_data_ptr_t g_state, data_ptr_t l_state, const SubFrames &subframes, Vector &result,
	                   idx_t rid) {
		throw InternalException("Sorted aggregates should not be generated for window clauses");
	}

	static void Finalize(Vector &states, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
	                     const idx_t offset) {
		//	Finalize the inner aggregate into the result
		auto &compare_bind = aggr_input_data.bind_data->Cast<CompareAggregateBindData>();
		auto &allocator = aggr_input_data.allocator;

		UnifiedVectorFormat svdata;
		states.ToUnifiedFormat(count, svdata);
		auto sdata = UnifiedVectorFormat::GetDataNoConst<CompareAggregateState *>(svdata);

		data_ptr_t child_state_ptrs[STANDARD_VECTOR_SIZE];
		Vector child_states(LogicalType::POINTER, reinterpret_cast<data_ptr_t>(child_state_ptrs));
		for (idx_t i = 0; i < count; ++i) {
			const auto sidx = svdata.sel->get_index(i);
			auto state = sdata[sidx];
			if (!state->val) {
				state->Initialize(compare_bind, allocator);
			}
			child_state_ptrs[i] = state->val;
		}

		AggregateInputData child_aggr_data(compare_bind.child_bind, aggr_input_data.allocator,
		                                   AggregateCombineType::ALLOW_DESTRUCTIVE);
		compare_bind.function.finalize(child_states, child_aggr_data, result, count, offset);

		auto destructor = compare_bind.function.destructor;
		if (destructor) {
			destructor(child_states, child_aggr_data, count);
		}
	}

	template <bool SKIP_NULLS>
	static AggregateFunction GetFunction(BoundAggregateExpression &expr) {
		auto &children = expr.children;
		auto &bound_function = expr.function;

		vector<LogicalType> arguments;
		arguments.reserve(children.size());
		for (const auto &child : children) {
			arguments.emplace_back(child->return_type);
		}

		return AggregateFunction(bound_function.name, arguments, bound_function.return_type,
		                         AggregateFunction::StateSize<CompareAggregateState>,
		                         AggregateFunction::StateInitialize<CompareAggregateState, CombineAggregateFunction>,
		                         CombineAggregateFunction::ScatterUpdate<SKIP_NULLS>,
		                         AggregateFunction::StateCombine<CompareAggregateState, CombineAggregateFunction>,
		                         CombineAggregateFunction::Finalize, bound_function.null_handling,
		                         CombineAggregateFunction::SimpleUpdate<SKIP_NULLS>, nullptr,
		                         AggregateFunction::StateDestroy<CompareAggregateState, CombineAggregateFunction>,
		                         nullptr, CombineAggregateFunction::Window);
	}

	static bool Bind(ClientContext &context, BoundAggregateExpression &expr) {
		//	Only comparable aggregates
		if (expr.function.order_dependent != AggregateOrderDependent::COMPARE_DEPENDENT) {
			return false;
		}

		//	Only fixed-width radix keys for now
		auto &order_bys = *expr.order_bys;
		SortLayout sort_layout(order_bys.orders);
		if (!sort_layout.all_constant) {
			return false;
		}

		const auto &fname = expr.function.name;
		const bool skip_nulls = (fname == "any_value");
		if (fname == "first" || fname == "arbitrary" || skip_nulls) {
			//	Rebind to LAST and invert the sort order
			expr.function = FirstFun::GetLastFunction(expr.return_type);
			if (expr.function.bind) {
				expr.bind_info = expr.function.bind(context, expr.function, expr.children);
			}
			for (auto &order : order_bys.orders) {
				order.type = (order.type == OrderType::ASCENDING) ? OrderType::DESCENDING : OrderType::ASCENDING;
				order.null_order = (order.null_order == OrderByNullType::NULLS_FIRST) ? OrderByNullType::NULLS_LAST
				                                                                      : OrderByNullType::NULLS_FIRST;
			}
		} else if (expr.function.name != "last") {
			return false;
		}

		//	Bind the sort key function as a second argument
		expr.children.emplace_back(SortKeyFun::Bind(context, order_bys.orders));

		// Replace the aggregate with the wrapper
		auto parent_bind = make_uniq<CompareAggregateBindData>(expr);
		expr.function = skip_nulls ? GetFunction<true>(expr) : GetFunction<false>(expr);
		expr.bind_info = std::move(parent_bind);
		expr.order_bys.reset();

		return true;
	}
};

void FunctionBinder::BindSortedAggregate(BoundAggregateExpression &expr, const vector<unique_ptr<Expression>> &groups) {
	if (!expr.order_bys || expr.order_bys->orders.empty() || expr.children.empty()) {
		// not a sorted aggregate: return
		return;
	}
	if (context.config.enable_optimizer) {
		// for each ORDER BY - check if it is actually necessary
		// expressions that are in the groups do not need to be ORDERED BY
		// `ORDER BY` on a group has no effect, because for each aggregate, the group is unique
		// similarly, we only need to ORDER BY each aggregate once
		expression_set_t seen_expressions;
		for (auto &target : groups) {
			seen_expressions.insert(*target);
		}
		vector<BoundOrderByNode> new_order_nodes;
		for (auto &order_node : expr.order_bys->orders) {
			if (seen_expressions.find(*order_node.expression) != seen_expressions.end()) {
				// we do not need to order by this node
				continue;
			}
			seen_expressions.insert(*order_node.expression);
			new_order_nodes.push_back(std::move(order_node));
		}
		if (new_order_nodes.empty()) {
			expr.order_bys.reset();
			return;
		}
		expr.order_bys->orders = std::move(new_order_nodes);
	}
	if (expr.order_bys->orders.empty()) {
		// not really a sorted aggregate: return
		return;
	}

	auto &bound_function = expr.function;

	//	Some sorted aggregates (FIRST, LAST...) only need to compare sort keys on each row
	//	instead of keeping all the values around (i.e., they are not holistic)
	if (CombineAggregateFunction::Bind(context, expr)) {
		return;
	}

	auto sorted_bind = make_uniq<SortedAggregateBindData>(context, expr);

	auto &children = expr.children;
	if (!sorted_bind->sorted_on_args) {
		// The arguments are the children plus the sort columns.
		for (auto &order : expr.order_bys->orders) {
			children.emplace_back(std::move(order.expression));
		}
	}

	vector<LogicalType> arguments;
	arguments.reserve(children.size());
	for (const auto &child : children) {
		arguments.emplace_back(child->return_type);
	}

	// Replace the aggregate with the wrapper
	AggregateFunction ordered_aggregate(
	    bound_function.name, arguments, bound_function.return_type, AggregateFunction::StateSize<SortedAggregateState>,
	    AggregateFunction::StateInitialize<SortedAggregateState, SortedAggregateFunction>,
	    SortedAggregateFunction::ScatterUpdate,
	    AggregateFunction::StateCombine<SortedAggregateState, SortedAggregateFunction>,
	    SortedAggregateFunction::Finalize, bound_function.null_handling, SortedAggregateFunction::SimpleUpdate, nullptr,
	    AggregateFunction::StateDestroy<SortedAggregateState, SortedAggregateFunction>, nullptr,
	    SortedAggregateFunction::Window);

	expr.function = std::move(ordered_aggregate);
	expr.bind_info = std::move(sorted_bind);
	expr.order_bys.reset();
}

} // namespace duckdb
