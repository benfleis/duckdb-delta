#include "delta_functions.hpp"

#include "delta_kernel_ffi.hpp"
#include "delta_utils.hpp"

// XXX: kill me
#include "functions/delta_scan/delta_multi_file_list.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/table_function.hpp"
#include <cstdint>

namespace duckdb {

struct CDFBindData : public TableFunctionData {
	// Directly from Bind, kept for later
	string path;
	uint64_t from_version;
	bool has_through_version = false;
	uint64_t through_version;

	// Our part
	KernelExternEngine extern_engine;
};

struct CDFGlobalState : public GlobalTableFunctionState {
	ffi::Handle<ffi::ExclusiveTableChanges> table_changes; // XXX : free me!
};

struct CDFLocalState : public LocalTableFunctionState {
	uint32_t local_n = 0xdeadbeef;
};

static unique_ptr<FunctionData> BindCDF(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<CDFBindData>();
	bind_data->path = input.inputs[0].GetValue<string>();
	bind_data->from_version = input.inputs[1].GetValue<uint64_t>();
	if (input.inputs.size() == 3) {
		bind_data->has_through_version = true;
		bind_data->through_version = input.inputs[2].GetValue<uint64_t>();
	}

	// TODO move this engine out, rely on attach and path derefing
	auto builder = CreateBuilder(context, bind_data->path);
	ffi::Handle<ffi::SharedExternEngine> engine_res;
	auto res = KernelUtils::TryUnpackResult(ffi::builder_build(builder), engine_res);
	if (res.HasError()) {
		res.Throw();
	}
	bind_data->extern_engine = engine_res;

	// TODO: actually visit schema; for now hard code standard metadata ...
	names.emplace_back("_commit_version");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("_commit_timestamp");
	return_types.emplace_back(LogicalType::TIMESTAMP);
	names.emplace_back("_change_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	// ... and data
	names.emplace_back("i");
	return_types.emplace_back(LogicalType::INTEGER);

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> InitCDFGlobalState(ClientContext &context, TableFunctionInitInput &input) {
	auto &bd = input.bind_data->CastNoConst<CDFBindData>();

	// XXX: debugging
	std::cerr << "CDF init global: path=" << bd.path << " from=" << bd.from_version << " through=";
	if (bd.has_through_version) {
		std::cerr << bd.through_version;
	} else {
		std::cerr << "-";
	}
	std::cerr << "\n";

	auto table_changes_ffi_res =
	    !bool(bd.has_through_version) == false
	        ? ffi::table_changes_from_version(KernelUtils::ToDeltaString(bd.path), bd.extern_engine.get(),
	                                          bd.from_version)
	        : ffi::table_changes_between_versions(KernelUtils::ToDeltaString(bd.path), bd.extern_engine.get(),
	                                              bd.from_version, bd.through_version);
	ffi::Handle<ffi::ExclusiveTableChanges> table_changes_res;
	auto res = KernelUtils::TryUnpackResult(table_changes_ffi_res, table_changes_res);
	if (res.HasError()) {
		res.Throw();
	}

	auto global_state = make_uniq<CDFGlobalState>();
	global_state->table_changes = table_changes_res;
	return std::move(global_state);
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
	// API: delta_change_data('/path/to/my/delta/table', <from_version>[, through_version])
	auto cdf_from_version = TableFunction("delta_change_data",
	                                      {
	                                          LogicalType::VARCHAR, // path
	                                          LogicalType::UBIGINT, // from_version
	                                      },
	                                      DataChangesFunction, BindCDF, InitCDFGlobalState, InitCDFLocalState);

	auto cdf_between_versions = TableFunction("delta_change_data",
	                                          {
	                                              LogicalType::VARCHAR, // path
	                                              LogicalType::UBIGINT, // from_version
	                                              LogicalType::UBIGINT, // through_version
	                                          },
	                                          DataChangesFunction, BindCDF, InitCDFGlobalState, InitCDFLocalState);

	auto funcs = TableFunctionSet(cdf_from_version);
	funcs.AddFunction(cdf_between_versions);
	return funcs;
}

} // namespace duckdb
