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
	auto  builder = std::make_shared<PixelsReaderBuilder>();

	shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
	auto a = builder->setPath(file_name);
	auto b = a->setStorage(storage);
	auto c = b->setPixelsFooterCache(footerCache);
	auto pixelsReader = c->build();
//	shared_ptr<PixelsReader> pixelsReader = builder
//	                                 ->setPath(file_name)
//	                                 ->setStorage(storage)
//	                                 ->setPixelsFooterCache(footerCache)
//	                                 ->build();
	auto result = make_unique<PixelsReadBindData>();
	auto d = pixelsReader->read(option);

//	std::shared_ptr<TypeDescription> resultSchema = result->pixelsRecordReader->getResultSchema();

//	TransformDuckdbType(resultSchema, return_types);

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
void PixelsScanFunction::TransformDuckdbType(const std::shared_ptr<TypeDescription>& type,
                                             vector<LogicalType> &return_types) {
	auto columnSchemas = type->getChildren();
	for(auto columnType: columnSchemas) {
		switch (columnType->getCategory()) {
			//        case TypeDescription::BOOLEAN:
			//            break;
			//        case TypeDescription::BYTE:
			//            break;
		case TypeDescription::SHORT:
		case TypeDescription::INT:
		case TypeDescription::LONG:
			return_types.emplace_back(LogicalType::INTEGER);
			break;
			//        case TypeDescription::FLOAT:
			//            break;
			//        case TypeDescription::DOUBLE:
			//            break;
			//        case TypeDescription::DECIMAL:
			//            break;
			//        case TypeDescription::STRING:
			//            break;
			//        case TypeDescription::DATE:
			//            break;
			//        case TypeDescription::TIME:
			//            break;
			//        case TypeDescription::TIMESTAMP:
			//            break;
			//        case TypeDescription::VARBINARY:
			//            break;
			//        case TypeDescription::BINARY:
			//            break;
		case TypeDescription::VARCHAR:
			return_types.emplace_back(LogicalType::VARCHAR);
			break;
		case TypeDescription::CHAR:
			return_types.emplace_back(LogicalType::VARCHAR);
			break;
			//        case TypeDescription::STRUCT:
			//            break;
		default:
			throw InvalidArgumentException("bad column type " + std::to_string(type->getCategory()));
		}
	}
}

}