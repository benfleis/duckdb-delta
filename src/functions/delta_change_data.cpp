#include "delta_functions.hpp"

#include "delta_kernel_ffi.hpp"
#include "delta_utils.hpp"

// XXX: kill me
#include "functions/delta_scan/delta_multi_file_list.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"
#include "generated_delta_kernel_ffi.hpp"
#include <cstdint>

namespace duckdb {

struct CDFBindData : public TableFunctionData {
	// Directly from Bind, kept for later
	string path;
	uint64_t from_version;
	bool has_through_version = false;
	uint64_t through_version;

	// Our parts --
	// XXX: do we need to enforce free ordering here among related/derivative mem blobs?
	KernelExternEngine extern_engine;
	struct {
		KernelTableChanges kernel;
		struct {
			KernelTableChangesScan kernel;
		} scan;
	} table_changes;
	// KernelTableChanges table_changes;
	// KernelTableChangesScan changes_scan;
	// KernelSchema changes_logical_schema;
	// KernelSchema changes_physical_schema;
};

struct CDFGlobalState : public GlobalTableFunctionState {
	ffi::Handle<ffi::SharedScanTableChangesIterator> scan_iter = {};
	ArrowTableSchema arrow_table;
	bool schema_initialized = false;
	bool done = false;
};

struct CDFLocalState : public LocalTableFunctionState {
	uint32_t local_n = 0xdeadbeef;
};

static unique_ptr<FunctionData> BindCDF(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<CDFBindData>();
	bd->path = input.inputs[0].GetValue<string>();
	bd->from_version = input.inputs[1].GetValue<uint64_t>();
	if (input.inputs.size() == 3) {
		bd->has_through_version = true;
		bd->through_version = input.inputs[2].GetValue<uint64_t>();
	}

	// TODO move this engine out, rely on attach and path derefing
	auto builder = CreateBuilder(context, bd->path);
	auto err_data = KernelUtils::TryUnpackKernelPointer(ffi::builder_build(builder), bd->extern_engine);
	if (err_data.HasError()) {
		err_data.Throw();
	}

#if 1
	// XXX: debugging
	std::cerr << "CDF init global: path=" << bd->path << " from=" << bd->from_version << " through=";
	if (bd->has_through_version) {
		std::cerr << bd->through_version;
	} else {
		std::cerr << "-";
	}
	std::cerr << "\n";
#endif

	auto table_changes_ffi_res =
	    bd->has_through_version
	        ? ffi::table_changes_between_versions(KernelUtils::ToDeltaString(bd->path), bd->extern_engine.get(),
	                                              bd->from_version, bd->through_version)
	        : ffi::table_changes_from_version(KernelUtils::ToDeltaString(bd->path), bd->extern_engine.get(),
	                                          bd->from_version);
	err_data = KernelUtils::TryUnpackKernelPointer(table_changes_ffi_res, bd->table_changes.kernel);
	if (err_data.HasError()) {
		err_data.Throw();
	}

	auto changes_scan_ffi_res =
	    ffi::table_changes_scan(bd->table_changes.kernel.get(), bd->extern_engine.get(), nullptr);
	err_data = KernelUtils::TryUnpackKernelPointer(changes_scan_ffi_res, bd->table_changes.scan.kernel);
	if (err_data.HasError()) {
		err_data.Throw();
	}

	auto fields =
	    SchemaVisitor::VisitTableChangesScanSchema(bd->extern_engine.get(), bd->table_changes.scan.kernel.get());
	for (const auto &field : fields) {
		names.push_back(field.name);
		return_types.push_back(field.type);
	}

	return std::move(bd);
}

static unique_ptr<GlobalTableFunctionState> InitCDFGlobalState(ClientContext &context, TableFunctionInitInput &input) {
	auto &bd = input.bind_data->CastNoConst<CDFBindData>();

	ffi::Handle<ffi::SharedTableChangesScan> scan_handle = bd.table_changes.scan.kernel.get();
	ffi::Handle<ffi::SharedExternEngine> engine_handle = bd.extern_engine.get();

	ffi::Handle<ffi::SharedScanTableChangesIterator> iter = nullptr;
	auto err = KernelUtils::TryUnpackResult(ffi::table_changes_scan_execute(scan_handle, engine_handle), iter);
	if (err.HasError()) {
		err.Throw();
	}

	auto global_state = make_uniq<CDFGlobalState>();
	global_state->scan_iter = iter;
	return std::move(global_state);
}

static unique_ptr<LocalTableFunctionState> InitCDFLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                             GlobalTableFunctionState *global_state) {
	return make_uniq<CDFLocalState>();
}

static void DataChangesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gs = data_p.global_state->Cast<CDFGlobalState>();

	if (gs.done) {
		output.SetCardinality(0);
		return;
	}

	ffi::ArrowFFIData arrow_data;
	auto err = KernelUtils::TryUnpackResult(ffi::scan_table_changes_next(gs.scan_iter), arrow_data);
	if (err.HasError()) {
		err.Throw();
	}

	if (!arrow_data.array.release || arrow_data.array.length == 0) {
		gs.done = true;
		output.SetCardinality(0);
		return;
	}

	if (!gs.schema_initialized) {
		std::cerr << "schema n_children=" << arrow_data.schema.n_children << "\n";
		std::cerr << "array n_children=" << arrow_data.array.n_children
		          << " children_ptr=" << (void *)arrow_data.array.children << "\n";
		for (int64_t i = 0; i < arrow_data.array.n_children; i++) {
			std::cerr << "  children[" << i << "]=" << (void *)arrow_data.array.children[i] << "\n";
		}

		ArrowTableFunction::PopulateArrowTableSchema(context, gs.arrow_table,
		                                             *reinterpret_cast<const ArrowSchema *>(&arrow_data.schema));
		gs.schema_initialized = true;
	}

	auto chunk_wrapper = make_uniq<ArrowArrayWrapper>();
	chunk_wrapper->arrow_array = *reinterpret_cast<ArrowArray *>(&arrow_data.array);
	arrow_data.array.release = nullptr; // transfer ownership to chunk_wrapper

	ArrowScanLocalState scan_state(std::move(chunk_wrapper), context);
	for (idx_t i = 0; i < output.ColumnCount(); i++) {
		scan_state.column_ids.push_back(i);
	}
	output.SetCardinality(arrow_data.array.length);

	std::cerr << "schema n_children=" << arrow_data.schema.n_children << "\n";
	std::cerr << "array n_children=" << arrow_data.array.n_children
	          << " children_ptr=" << (void *)arrow_data.array.children << "\n";
	for (int64_t i = 0; i < arrow_data.array.n_children; i++) {
		std::cerr << "  children[" << i << "]=" << (void *)arrow_data.array.children[i] << "\n";
	}

	ArrowTableFunction::ArrowToDuckDB(scan_state, gs.arrow_table.GetColumns(), output);
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
