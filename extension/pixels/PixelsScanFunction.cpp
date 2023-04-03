//
// Created by liyu on 3/26/23.
//

#include "PixelsScanFunction.hpp"

namespace duckdb {

TableFunctionSet PixelsScanFunction::GetFunctionSet() {
	TableFunctionSet set("pixels_scan");
	TableFunction table_function({LogicalType::VARCHAR}, PixelsScanImplementation, PixelsScanBind,
	                             PixelsScanInitGlobal, PixelsScanInitLocal);
	table_function.projection_pushdown = true;
	table_function.filter_pushdown = true;
	table_function.filter_prune = true;
	// TODO: maybe we need other code here. Refer parquet-extension.cpp
	set.AddFunction(table_function);
	return set;
}

void PixelsScanFunction::PixelsScanImplementation(ClientContext &context,
                                                   TableFunctionInput &data_p,
                                                   DataChunk &output) {
	if (!data_p.local_state) {
		return;
	}
	auto &data = (PixelsReadLocalState &)*data_p.local_state;
	auto &gstate = (PixelsReadGlobalState &)*data_p.global_state;
	auto &bind_data = (PixelsReadBindData &)*data_p.bind_data;

	if(data.pixelsRecordReader.get() == nullptr) {
		PixelsReaderOption option;
		option.setSkipCorruptRecords(true);
		option.setTolerantSchemaEvolution(true);
		option.setEnableEncodedColumnVector(true);

		// includeCols comes from the caller of PixelsPageSource
		option.setIncludeCols(data.column_names);
		option.setRGRange(0, 1);
		option.setQueryId(1);
		data.pixelsRecordReader = bind_data.pixelsReader->read(option);
	}
	auto pixelsRecordReader = data.pixelsRecordReader;

	if(pixelsRecordReader->isEndOfFile()) {
		data.pixelsRecordReader.reset();
		return;
	}
	std::shared_ptr<TypeDescription> resultSchema = pixelsRecordReader->getResultSchema();
	auto vectorizedRowBatch = pixelsRecordReader->readBatch(10000, false);
	assert(vectorizedRowBatch->rowCount > 0);
	output.SetCardinality(vectorizedRowBatch->rowCount);
	TransformDuckdbChunk(vectorizedRowBatch, output, resultSchema);
	return;
}

unique_ptr<FunctionData> PixelsScanFunction::PixelsScanBind(
    						ClientContext &context, TableFunctionBindInput &input,
                            vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw ParserException("Pixels reader cannot take NULL list as parameter");
	}
	auto file_name = StringValue::Get(input.inputs[0]);

	auto footerCache = std::make_shared<PixelsFooterCache>();
	auto  builder = std::make_shared<PixelsReaderBuilder>();

	shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
	shared_ptr<PixelsReader> pixelsReader = builder
	                                 ->setPath(file_name)
	                                 ->setStorage(storage)
	                                 ->setPixelsFooterCache(footerCache)
	                                 ->build();
	std::shared_ptr<TypeDescription> fileSchema = pixelsReader->getFileSchema();
	TransformDuckdbType(fileSchema, return_types);
	names = fileSchema->getFieldNames();

	auto result = make_unique<PixelsReadBindData>();
	result->pixelsReader = pixelsReader;
	result->fileSchema = fileSchema;
	return result;
}

unique_ptr<GlobalTableFunctionState> PixelsScanFunction::PixelsScanInitGlobal(
    						ClientContext &context, TableFunctionInitInput &input) {
	return make_unique<PixelsReadGlobalState>();
}

unique_ptr<LocalTableFunctionState> PixelsScanFunction::PixelsScanInitLocal(
    						ExecutionContext &context, TableFunctionInitInput &input,
                            GlobalTableFunctionState *gstate_p) {
	auto &bind_data = (PixelsReadBindData &)*input.bind_data;
	auto pixelsReadLocalState = make_unique<PixelsReadLocalState>();
	pixelsReadLocalState->column_ids = input.column_ids;
	auto fieldNames = bind_data.fileSchema->getFieldNames();
	for(column_t column_id : input.column_ids) {
		pixelsReadLocalState->column_names.emplace_back(fieldNames.at(column_id));
	}
	return pixelsReadLocalState;
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
				return_types.emplace_back(LogicalType::BIGINT);
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
void PixelsScanFunction::TransformDuckdbChunk(const shared_ptr<VectorizedRowBatch> &vectorizedRowBatch,
                                              DataChunk &output,
                                              const std::shared_ptr<TypeDescription> & schema) {
	for(int col_id = 0; col_id < vectorizedRowBatch->numCols; col_id++) {
		auto col = vectorizedRowBatch->cols.at(col_id);
		auto colSchema = schema->getChildren().at(col_id);
		switch (colSchema->getCategory()) {
				//        case TypeDescription::BOOLEAN:
				//            break;
				//        case TypeDescription::BYTE:
				//            break;
			case TypeDescription::SHORT:
			case TypeDescription::INT:
			case TypeDescription::LONG: {
				auto longCol = std::static_pointer_cast<LongColumnVector>(col);
				Vector vector(LogicalType::BIGINT, (data_ptr_t)longCol->vector);
				output.data.at(col_id).Reference(vector);
				break;
			}
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
			case TypeDescription::CHAR:
		    {
			    auto binaryCol = std::static_pointer_cast<BinaryColumnVector>(col);
			    auto result_ptr = FlatVector::GetData<duckdb::string_t>(output.data.at(col_id));
			    for(int row_id = 0; row_id < binaryCol->size; row_id++) {
				    int length = binaryCol->lens[row_id];
				    const char * data = (const char *)binaryCol->vector[row_id] + binaryCol->start[row_id];
				    result_ptr[row_id] = duckdb::string_t(data, length);
			    }
			    break;
		    }
			//        case TypeDescription::STRUCT:
			//            break;
//			default:
//				throw InvalidArgumentException("bad column type " + std::to_string(colSchema->getCategory()));
		}
	}
}

}