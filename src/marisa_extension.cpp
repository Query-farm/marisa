#include "marisa_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include <marisa.h>
#include "query_farm_telemetry.hpp"

namespace duckdb {

inline void MarisaTrieLookupScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &trie_vector = args.data[0];
	auto &value_vector = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, bool>(trie_vector, value_vector, result, args.size(),
	                                                  [&](string_t trie_value, string_t value) {
		                                                  marisa::Trie trie;
		                                                  trie.map(trie_value.GetData(), trie_value.GetSize());

		                                                  marisa::Agent agent;
		                                                  agent.set_query(value.GetData(), value.GetSize());
		                                                  return trie.lookup(agent);
	                                                  });
}

// Common helper function to reduce code duplication
template <typename SearchFunc>
inline list_entry_t ExecuteMarisaTrieSearch(string_t trie_value, string_t value, int32_t max_results, Vector &result,
                                            SearchFunc search_func) {

	if (max_results < 0) {
		throw InvalidInputException("max_results must be non-negative, got %d", max_results);
	}

	// Early return for zero max_results
	if (max_results == 0) {
		auto current_size = ListVector::GetListSize(result);
		return list_entry_t {current_size, 0};
	}

	marisa::Trie trie;
	trie.map(trie_value.GetData(), trie_value.GetSize());

	marisa::Agent agent;
	agent.set_query(value.GetData(), value.GetSize());

	auto current_size = ListVector::GetListSize(result);

	// Reserve space upfront to avoid multiple reallocations
	std::vector<std::string> results;
	results.reserve(max_results > 1000 ? 1000 : static_cast<size_t>(max_results)); // Cap initial reserve

	// Collect results using the provided search function
	size_t count = 0;
	while (count < static_cast<size_t>(max_results) && search_func(trie, agent)) {
		results.emplace_back(agent.key().str());
		++count;
	}

	// Early return if no results found
	if (results.empty()) {
		return list_entry_t {current_size, 0};
	}

	auto new_size = current_size + results.size();
	if (ListVector::GetListCapacity(result) < new_size) {
		ListVector::Reserve(result, new_size);
	}

	auto &child_entry = ListVector::GetEntry(result);
	auto child_vals = FlatVector::GetData<string_t>(child_entry);

	// Use range-based loop with index for better readability
	for (size_t i = 0; i < results.size(); ++i) {
		child_vals[current_size + i] = StringVector::AddStringOrBlob(child_entry, results[i]);
	}

	ListVector::SetListSize(result, new_size);

	return list_entry_t {current_size, results.size()};
}

inline void MarisaTrieCommonPrefixScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &trie_vector = args.data[0];
	auto &value_vector = args.data[1];
	auto &length_vector = args.data[2];

	TernaryExecutor::Execute<string_t, string_t, int32_t, list_entry_t>(
	    trie_vector, value_vector, length_vector, result, args.size(),
	    [&](string_t trie_value, string_t value, int32_t max_results) {
		    return ExecuteMarisaTrieSearch(
		        trie_value, value, max_results, result,
		        [](marisa::Trie &trie, marisa::Agent &agent) { return trie.common_prefix_search(agent); });
	    });
}

inline void MarisaTriePredictiveSearchScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &trie_vector = args.data[0];
	auto &value_vector = args.data[1];
	auto &length_vector = args.data[2];

	TernaryExecutor::Execute<string_t, string_t, int32_t, list_entry_t>(
	    trie_vector, value_vector, length_vector, result, args.size(),
	    [&](string_t trie_value, string_t value, int32_t max_results) {
		    return ExecuteMarisaTrieSearch(
		        trie_value, value, max_results, result,
		        [](marisa::Trie &trie, marisa::Agent &agent) { return trie.predictive_search(agent); });
	    });
}

struct MarisaTrieBindData : public FunctionData {
	MarisaTrieBindData() {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<MarisaTrieBindData>();
	}

	bool Equals(const FunctionData &other) const override {
		return dynamic_cast<const MarisaTrieBindData *>(&other) != nullptr;
	}
};

struct MarisaTrieState {

	unique_ptr<std::vector<string>> entries;

	MarisaTrieState() {
		entries = make_uniq<std::vector<string>>();
		entries->reserve(STANDARD_VECTOR_SIZE); // Reserve space to avoid reallocations
	}
};

template <class BIND_DATA_TYPE>
struct MarisaTrieCreateOperation {

	template <class STATE>
	static void Initialize(STATE &state) {
		state.entries.release();
		state.entries = nullptr;
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.entries) {
			state.entries = nullptr;
		}
	}

	template <class A_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata) {
		if (!state.entries) {
			state.entries = make_uniq<std::vector<string>>();
			state.entries->reserve(STANDARD_VECTOR_SIZE);
		}
		state.entries->push_back(a_data.GetString());
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		// Early return if source is empty
		if (!source.entries || source.entries->empty()) {
			return;
		}

		// Initialize target if needed
		if (!target.entries) {
			target.entries = make_uniq<std::vector<std::string>>();
		}

		// Reserve space and merge efficiently
		target.entries->reserve(target.entries->size() + source.entries->size());
		target.entries->insert(target.entries->end(), std::make_move_iterator(source.entries->begin()),
		                       std::make_move_iterator(source.entries->end()));
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.entries || state.entries->empty()) {
			finalize_data.ReturnNull();
		} else {

			marisa::Keyset keyset;
			for (const auto &entry : *state.entries) {
				keyset.push_back(std::string_view(entry));
			}
			marisa::Trie trie;
			trie.build(keyset);
			std::stringstream memstream(std::ios::in | std::ios::out | std::ios::binary);
			memstream << trie;
			target = StringVector::AddStringOrBlob(finalize_data.result, memstream.str());
		}
	}
};

unique_ptr<FunctionData> MarisaTrieBind(ClientContext &context, AggregateFunction &function,
                                        vector<unique_ptr<Expression>> &arguments) {

	if (arguments.size() != 1) {
		throw BinderException("Marisa Trie aggregate function requires one argument");
	}
	return make_uniq<MarisaTrieBindData>();
}

template <typename T>
auto static MarisaTrieCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction {
	return AggregateFunction::UnaryAggregateDestructor<
	    MarisaTrieState, T, string_t, MarisaTrieCreateOperation<MarisaTrieBindData>, AggregateDestructorType::LEGACY>(
	    type, result_type);
}

static void LoadInternal(ExtensionLoader &loader) {
	auto lookup_scalar_function = ScalarFunction("marisa_lookup", {LogicalType::BLOB, LogicalType::VARCHAR},
	                                             LogicalType::BOOLEAN, MarisaTrieLookupScalarFun);
	loader.RegisterFunction(lookup_scalar_function);

	auto common_prefix_scalar_function =
	    ScalarFunction("marisa_common_prefix", {LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::INTEGER},
	                   LogicalType::LIST(LogicalType::VARCHAR), MarisaTrieCommonPrefixScalarFun);
	loader.RegisterFunction(common_prefix_scalar_function);

	auto predictive_search_scalar_function =
	    ScalarFunction("marisa_predictive", {LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::INTEGER},
	                   LogicalType::LIST(LogicalType::VARCHAR), MarisaTriePredictiveSearchScalarFun);
	loader.RegisterFunction(predictive_search_scalar_function);

	auto &db = loader.GetDatabaseInstance();
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);

	AggregateFunctionSet marisa_trie_set("marisa_trie");

	auto fun = MarisaTrieCreateAggregate<string_t>(LogicalType::VARCHAR, LogicalType::BLOB);
	fun.bind = MarisaTrieBind;
	marisa_trie_set.AddFunction(fun);

	CreateAggregateFunctionInfo marisa_trie_create_info(marisa_trie_set);

	{
		FunctionDescription desc;
		desc.description = "Creates a new Marisa Trie from the value supplied.";
		desc.examples.push_back("SELECT marisa_trie(column) FROM table");
		marisa_trie_create_info.descriptions.push_back(desc);
	}

	system_catalog.CreateFunction(data, marisa_trie_create_info);

	QueryFarmSendTelemetry(loader, "marisa", "2025120401");
}

void MarisaExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string MarisaExtension::Name() {
	return "marisa";
}

std::string MarisaExtension::Version() const {
	return "2025120401";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(marisa, loader) {
	duckdb::LoadInternal(loader);
}
}
