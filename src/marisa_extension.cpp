#include "marisa_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include <marisa.h>
#include "query_farm_telemetry.hpp"

namespace duckdb {

//! Register a scalar function with a description and a runnable example in duckdb_functions()
static void RegisterDocumented(ExtensionLoader &loader, ScalarFunction function, const string &description,
                               const string &example) {
	CreateScalarFunctionInfo info(std::move(function));
	FunctionDescription desc;
	desc.description = description;
	desc.examples.push_back(example);
	desc.categories.push_back("marisa");
	desc.categories.push_back("string");
	info.descriptions.push_back(std::move(desc));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	loader.RegisterFunction(std::move(info));
}

//! A positional-only parameter with a real name (bare types would be reported as col0, col1, ...)
static FunctionParameter Positional(const char *name, LogicalType type) {
	return FunctionParameter(name, std::move(type), {}, FunctionParameterKind::POSITIONAL_ONLY);
}

inline void MarisaTrieLookupScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &trie_vector = args.data[0];
	auto &value_vector = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, bool>(trie_vector, value_vector, result,
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
		return list_entry_t(current_size, 0);
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
		return list_entry_t(current_size, 0);
	}

	auto new_size = current_size + results.size();
	if (ListVector::GetListCapacity(result) < new_size) {
		ListVector::Reserve(result, new_size);
	}

	auto &child_entry = ListVector::GetChildMutable(result);
	auto child_vals = FlatVector::GetDataMutable<string_t>(child_entry);

	// Use range-based loop with index for better readability
	for (size_t i = 0; i < results.size(); ++i) {
		child_vals[current_size + i] = StringVector::AddStringOrBlob(child_entry, results[i]);
	}

	ListVector::SetListSize(result, new_size);

	return list_entry_t(current_size, results.size());
}

inline void MarisaTrieCommonPrefixScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &trie_vector = args.data[0];
	auto &value_vector = args.data[1];
	auto &length_vector = args.data[2];

	TernaryExecutor::Execute<string_t, string_t, int32_t, list_entry_t>(
	    trie_vector, value_vector, length_vector, result,
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
	    trie_vector, value_vector, length_vector, result,
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

unique_ptr<FunctionData> MarisaTrieBind(BindAggregateFunctionInput &input) {
	auto &arguments = input.GetArguments();

	if (arguments.size() != 1) {
		throw BinderException("Marisa Trie aggregate function requires one argument");
	}
	return make_uniq<MarisaTrieBindData>();
}

template <typename T>
auto static MarisaTrieCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction {
	return AggregateFunction::UnaryAggregate<
	    MarisaTrieState, T, string_t, MarisaTrieCreateOperation<MarisaTrieBindData>, AggregateDestructorType::LEGACY>(
	    type, result_type);
}

static void LoadInternal(ExtensionLoader &loader) {
	auto lookup_scalar_function =
	    ScalarFunction("marisa_lookup",
	                   {Positional("trie", LogicalType::BLOB), Positional("search", LogicalType::VARCHAR)},
	                   LogicalType::BOOLEAN, MarisaTrieLookupScalarFun);
	lookup_scalar_function.SetFallible();
	RegisterDocumented(loader, std::move(lookup_scalar_function),
	                   "Returns true if search is a key stored in the trie, false otherwise.",
	                   "SELECT marisa_lookup(marisa_trie(w), 'apple') FROM (VALUES ('apple'), ('banana')) t(w); "
	                   "-- true");

	auto common_prefix_scalar_function =
	    ScalarFunction("marisa_common_prefix",
	                   {Positional("trie", LogicalType::BLOB), Positional("search", LogicalType::VARCHAR),
	                    Positional("max_results", LogicalType::INTEGER)},
	                   LogicalType::LIST(LogicalType::VARCHAR), MarisaTrieCommonPrefixScalarFun);
	common_prefix_scalar_function.SetFallible();
	RegisterDocumented(loader, std::move(common_prefix_scalar_function),
	                   "Returns up to max_results keys stored in the trie that are prefixes of search, shortest first.",
	                   "SELECT marisa_common_prefix(marisa_trie(w), 'applesauce', 10) "
	                   "FROM (VALUES ('a'), ('app'), ('apple'), ('banana')) t(w); -- [a, app, apple]");

	auto predictive_search_scalar_function =
	    ScalarFunction("marisa_predictive",
	                   {Positional("trie", LogicalType::BLOB), Positional("prefix", LogicalType::VARCHAR),
	                    Positional("max_results", LogicalType::INTEGER)},
	                   LogicalType::LIST(LogicalType::VARCHAR), MarisaTriePredictiveSearchScalarFun);
	predictive_search_scalar_function.SetFallible();
	RegisterDocumented(loader, std::move(predictive_search_scalar_function),
	                   "Returns up to max_results keys stored in the trie that start with prefix.",
	                   "SELECT marisa_predictive(marisa_trie(w), 'app', 10) "
	                   "FROM (VALUES ('apple'), ('apply'), ('banana')) t(w); -- [apple, apply]");

	AggregateFunctionSet marisa_trie_set("marisa_trie");

	auto fun = MarisaTrieCreateAggregate<string_t>(LogicalType::VARCHAR, LogicalType::BLOB);
	fun.SetBindCallback(MarisaTrieBind);
	// Name the (positional-only) argument, otherwise it is reported as col0
	fun.GetSignature().GetParameter(0).SetName("value");
	marisa_trie_set.AddFunction(fun);

	CreateAggregateFunctionInfo marisa_trie_create_info(marisa_trie_set);

	{
		FunctionDescription desc;
		desc.description = "Aggregates VARCHAR values into a compact MARISA trie, returned as a BLOB for use with "
		                   "marisa_lookup, marisa_common_prefix and marisa_predictive.";
		desc.examples.push_back("SELECT marisa_lookup(marisa_trie(w), 'apple') FROM (VALUES ('apple'), ('banana')) t(w); "
		                        "-- true");
		desc.categories.push_back("marisa");
		desc.categories.push_back("aggregate");
		marisa_trie_create_info.descriptions.push_back(desc);
	}

	loader.RegisterFunction(marisa_trie_create_info);

	QueryFarmSendTelemetry(loader, "marisa", "2026111701");
}

void MarisaExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string MarisaExtension::Name() {
	return "marisa";
}

std::string MarisaExtension::Version() const {
	return "2026111701";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(marisa, loader) {
	duckdb::LoadInternal(loader);
}
}
