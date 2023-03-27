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
	if (input.inputs[0].IsNull()) {
		throw ParserException("Pixels reader cannot take NULL list as parameter");
	}
	auto file_name = StringValue::Get(input.inputs[0]);

	PixelsReaderOption option;
	option.setSkipCorruptRecords(true);
	option.setTolerantSchemaEvolution(true);
	option.setEnableEncodedColumnVector(true);

	// includeCols comes from the caller of PixelsPageSource
	std::vector<std::string> includeCols;
	includeCols.emplace_back("n_nationkey");
	//    includeCols.emplace_back("n_name");
	includeCols.emplace_back("n_regionkey");
	includeCols.emplace_back("n_comment");
	option.setIncludeCols(includeCols);
	option.setRGRange(0, 1);
	option.setQueryId(1);
	
	PixelsFooterCache footerCache;
	auto * builder = new PixelsReaderBuilder;
	::Storage * storage = StorageFactory::getInstance()->getStorage(::Storage::file);
	PixelsReader * pixelsReader = builder
	                                 ->setPath(file_name)
	                                 ->setStorage(storage)
	                                 ->setPixelsFooterCache(footerCache)
	                                 ->build();
	PixelsRecordReader * pixelsRecordReader = pixelsReader->read(option);
	std::shared_ptr<VectorizedRowBatch> v = pixelsRecordReader->readBatch(13, false);

	for(const auto& col: v->cols) {
		std::cout<<"------"<<std::endl;
		col->print();
	}
	std::shared_ptr<VectorizedRowBatch> v1 = pixelsRecordReader->readBatch(12, false);
	std::cout<<"------"<<std::endl;
	std::cout<<"------"<<std::endl;
	std::cout<<"------"<<std::endl;
	std::cout<<"------"<<std::endl;
	for(const auto& col: v1->cols) {
		std::cout<<"------"<<std::endl;
		col->print();
	}
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