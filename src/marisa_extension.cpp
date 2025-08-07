#define DUCKDB_EXTENSION_MAIN

#include "marisa_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include <marisa.h>

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

inline void MarisaTrieCommonPrefixScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &trie_vector = args.data[0];
	auto &value_vector = args.data[1];
	auto &length_vector = args.data[2];

	TernaryExecutor::Execute<string_t, string_t, int32_t, list_entry_t>(
	    trie_vector, value_vector, length_vector, result, args.size(),
	    [&](string_t trie_value, string_t value, int32_t max_results) {
		    marisa::Trie trie;
		    trie.map(trie_value.GetData(), trie_value.GetSize());

		    marisa::Agent agent;
		    agent.set_query(value.GetData(), value.GetSize());

		    auto current_size = ListVector::GetListSize(result);

		    std::vector<string> results;
		    size_t count = 0;
		    while (count < max_results && trie.common_prefix_search(agent)) {
			    results.push_back(string(agent.key().str()));
			    count++;
		    }

		    auto new_size = current_size + results.size();
		    if (ListVector::GetListCapacity(result) < new_size) {
			    ListVector::Reserve(result, new_size);
		    }

		    auto &child_entry = ListVector::GetEntry(result);
		    auto child_vals = FlatVector::GetData<string_t>(child_entry);

		    for (size_t i = 0; i < results.size(); i++) {
			    child_vals[current_size + i] = StringVector::AddStringOrBlob(child_entry, results[i]);
		    }

		    ListVector::SetListSize(result, new_size);

		    return list_entry_t {current_size, results.size()};
	    });
}

inline void MarisaTriePredictiveSearchScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &trie_vector = args.data[0];
	auto &value_vector = args.data[1];
	auto &length_vector = args.data[2];

	TernaryExecutor::Execute<string_t, string_t, int32_t, list_entry_t>(
	    trie_vector, value_vector, length_vector, result, args.size(),
	    [&](string_t trie_value, string_t value, int32_t max_results) {
		    marisa::Trie trie;
		    trie.map(trie_value.GetData(), trie_value.GetSize());

		    marisa::Agent agent;
		    agent.set_query(value.GetData(), value.GetSize());

		    auto current_size = ListVector::GetListSize(result);

		    std::vector<string> results;
		    size_t count = 0;
		    while (count < max_results && trie.predictive_search(agent)) {
			    results.push_back(string(agent.key().str()));
			    count++;
		    }

		    auto new_size = current_size + results.size();
		    if (ListVector::GetListCapacity(result) < new_size) {
			    ListVector::Reserve(result, new_size);
		    }

		    auto &child_entry = ListVector::GetEntry(result);
		    auto child_vals = FlatVector::GetData<string_t>(child_entry);

		    for (size_t i = 0; i < results.size(); i++) {
			    child_vals[current_size + i] = StringVector::AddStringOrBlob(child_entry, results[i]);
		    }

		    ListVector::SetListSize(result, new_size);

		    return list_entry_t {current_size, results.size()};
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
		if (!target.entries) {
			target.entries = make_uniq<std::vector<string>>();
		}

		// merge the two vectors

		if (!source.entries) {
			return; // nothing to merge
		}
		target.entries->reserve(target.entries->size() + source.entries->size());
		target.entries->insert(target.entries->end(), source.entries->begin(), source.entries->end());
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.entries || state.entries->empty()) {
			finalize_data.ReturnNull();
		} else {

			marisa::Keyset keyset;
			for (const auto &entry : *state.entries) {
				keyset.push_back(entry);
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

static void LoadInternal(DatabaseInstance &instance) {
	auto lookup_scalar_function = ScalarFunction("marisa_lookup", {LogicalType::BLOB, LogicalType::VARCHAR},
	                                             LogicalType::BOOLEAN, MarisaTrieLookupScalarFun);
	ExtensionUtil::RegisterFunction(instance, lookup_scalar_function);

	auto common_prefix_scalar_function =
	    ScalarFunction("marisa_common_prefix", {LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::INTEGER},
	                   LogicalType::LIST(LogicalType::VARCHAR), MarisaTrieCommonPrefixScalarFun);
	ExtensionUtil::RegisterFunction(instance, common_prefix_scalar_function);

	auto predictive_search_scalar_function =
	    ScalarFunction("marisa_predictive", {LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::INTEGER},
	                   LogicalType::LIST(LogicalType::VARCHAR), MarisaTriePredictiveSearchScalarFun);
	ExtensionUtil::RegisterFunction(instance, predictive_search_scalar_function);

	auto &system_catalog = Catalog::GetSystemCatalog(instance);
	auto data = CatalogTransaction::GetSystemTransaction(instance);

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
}

void MarisaExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string MarisaExtension::Name() {
	return "marisa";
}

std::string MarisaExtension::Version() const {
	return "0.0.1";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void marisa_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::MarisaExtension>();
}

DUCKDB_EXTENSION_API const char *marisa_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
