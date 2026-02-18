#include "delta_functions.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct CDFBindData : public TableFunctionData {
	string path;
	uint32_t bind_n = 42;
};

struct CDFGlobalState : public GlobalTableFunctionState {
	uint32_t global_n = 17;
};

struct CDFLocalState : public LocalTableFunctionState {
	uint32_t local_n = 0xdeadbeef;
};

static unique_ptr<FunctionData> BindCDF(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<CDFBindData>();
	bind_data->path = input.inputs[0].GetValue<string>();
	names.emplace_back("change_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> InitCDFGlobalState(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<CDFGlobalState>();
}

static unique_ptr<LocalTableFunctionState> InitCDFLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                             GlobalTableFunctionState *global_state) {
	return make_uniq<CDFLocalState>();
}

static void DataChangesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<CDFBindData>();
	(void)data;
	output.SetCardinality(0);
}

TableFunctionSet DeltaFunctions::GetDeltaDataChangesFunction(ExtensionLoader &loader) {
	// API: delta_change_data('/path/to/my/delta/table', <start_version>[, end_version])
	auto cdf_from_version = TableFunction("delta_change_data",
	                                      {
	                                          LogicalType::VARCHAR, // path
	                                          LogicalType::UBIGINT, // start_version
	                                      },
	                                      DataChangesFunction, BindCDF, InitCDFGlobalState, InitCDFLocalState);

	auto cdf_between_versions = TableFunction("delta_change_data",
	                                          {
	                                              LogicalType::VARCHAR, // path
	                                              LogicalType::UBIGINT, // start_version
	                                              LogicalType::UBIGINT, // end_version
	                                          },
	                                          DataChangesFunction, BindCDF, InitCDFGlobalState, InitCDFLocalState);

	auto funcs = TableFunctionSet(cdf_from_version);
	funcs.AddFunction(cdf_between_versions);
	return funcs;
}

} // namespace duckdb
