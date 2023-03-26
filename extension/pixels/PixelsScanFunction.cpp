//
// Created by liyu on 3/26/23.
//

#include "PixelsScanFunction.hpp"

namespace duckdb {

TableFunctionSet PixelsScanFunction::GetFunctionSet() {
	TableFunctionSet set("pixels_scan");
	TableFunction table_function({LogicalType::VARCHAR}, PixelsScanImplementation, PixelsScanBind,
	                             PixelsScanInitGlobal, PixelsScanInitLocal);
	set.AddFunction(table_function);
	return set;
}

void PixelsScanFunction::PixelsScanImplementation(ClientContext &context,
                                                   TableFunctionInput &data_p,
                                                   DataChunk &output) {
	return;
}

unique_ptr<FunctionData> PixelsScanFunction::PixelsScanBind(
    						ClientContext &context, TableFunctionBindInput &input,
                            vector<LogicalType> &return_types, vector<string> &names) {
	return unique_ptr<FunctionData>();
}

unique_ptr<GlobalTableFunctionState> PixelsScanFunction::PixelsScanInitGlobal(
    						ClientContext &context, TableFunctionInitInput &input) {
	auto a = make_unique<PixelsReadGlobalState>();
	return a;
}

unique_ptr<LocalTableFunctionState> PixelsScanFunction::PixelsScanInitLocal(
    						ExecutionContext &context, TableFunctionInitInput &input,
                            GlobalTableFunctionState *gstate_p) {
	auto a = make_unique<PixelsReadLocalState>();
	return a;
}


}