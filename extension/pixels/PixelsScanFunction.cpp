//
// Created by liyu on 3/26/23.
//

#include "PixelsScanFunction.hpp"

namespace duckdb {

static idx_t PixelsScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                                     LocalTableFunctionState *local_state,
                                     GlobalTableFunctionState *global_state) {
	auto &data = (PixelsReadLocalState &)*local_state;
	return data.batch_index;
}

TableFunctionSet PixelsScanFunction::GetFunctionSet() {
	TableFunctionSet set("pixels_scan");
	TableFunction table_function({LogicalType::VARCHAR}, PixelsScanImplementation, PixelsScanBind,
	                             PixelsScanInitGlobal, PixelsScanInitLocal);
	table_function.projection_pushdown = true;
//	table_function.filter_pushdown = true;
//	table_function.filter_prune = true;
	table_function.get_batch_index = PixelsScanGetBatchIndex;
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



	if(data.pixelsRecordReader->isEndOfFile()) {
		data.vectorizedRowBatchs.clear();
		data.pixelsRecordReader.reset();
		if(!PixelsParallelStateNext(context, bind_data, data, gstate)) {
			return;
		} else {
			PixelsReaderOption option;
			option.setSkipCorruptRecords(true);
			option.setTolerantSchemaEvolution(true);
			option.setEnableEncodedColumnVector(true);

			// includeCols comes from the caller of PixelsPageSource
			option.setIncludeCols(data.column_names);
			option.setRGRange(0, data.reader->getRowGroupNum());
			option.setQueryId(1);
			data.pixelsRecordReader = data.reader->read(option);
		}
	}
	auto pixelsRecordReader = data.pixelsRecordReader;
	std::shared_ptr<TypeDescription> resultSchema = pixelsRecordReader->getResultSchema();
	auto vectorizedRowBatch = pixelsRecordReader->readBatch(STANDARD_VECTOR_SIZE, false);
	assert(vectorizedRowBatch->rowCount > 0);
	output.SetCardinality(vectorizedRowBatch->rowCount);
	data.vectorizedRowBatchs.emplace_back(vectorizedRowBatch);
	TransformDuckdbChunk(data.column_ids, vectorizedRowBatch, output, resultSchema);
	return;
}

unique_ptr<FunctionData> PixelsScanFunction::PixelsScanBind(
    						ClientContext &context, TableFunctionBindInput &input,
                            vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw ParserException("Pixels reader cannot take NULL list as parameter");
	}
	auto file_name = StringValue::Get(input.inputs[0]);
	FileSystem &fs = FileSystem::GetFileSystem(context);
	auto files = fs.GlobFiles(file_name, context);
	sort(files.begin(), files.end());
	auto footerCache = std::make_shared<PixelsFooterCache>();
	auto builder = std::make_shared<PixelsReaderBuilder>();

	shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
	shared_ptr<PixelsReader> pixelsReader = builder
	                                 ->setPath(files.at(0))
	                                 ->setStorage(storage)
	                                 ->setPixelsFooterCache(footerCache)
	                                 ->build();
	std::shared_ptr<TypeDescription> fileSchema = pixelsReader->getFileSchema();
	TransformDuckdbType(fileSchema, return_types);
	names = fileSchema->getFieldNames();

	auto result = make_unique<PixelsReadBindData>();
	result->initialPixelsReader = pixelsReader;
	result->fileSchema = fileSchema;
	result->files = files;

	return result;
}

unique_ptr<GlobalTableFunctionState> PixelsScanFunction::PixelsScanInitGlobal(
    						ClientContext &context, TableFunctionInitInput &input) {

	auto &bind_data = (PixelsReadBindData &)*input.bind_data;

	auto result = make_unique<PixelsReadGlobalState>();

	result->file_mutexes = std::unique_ptr<mutex[]>(new mutex[bind_data.files.size()]);

	result->readers = std::vector<shared_ptr<PixelsReader>>(bind_data.files.size(), nullptr);

	result->initialPixelsReader = bind_data.initialPixelsReader;
	result->readers[0] = bind_data.initialPixelsReader;

	result->file_index = 0;
	result->max_threads = bind_data.files.size();

	result->batch_index = 0;

	return std::move(result);
}

unique_ptr<LocalTableFunctionState> PixelsScanFunction::PixelsScanInitLocal(
    						ExecutionContext &context, TableFunctionInitInput &input,
                            GlobalTableFunctionState *gstate_p) {
	auto &bind_data = (PixelsReadBindData &)*input.bind_data;

	auto &gstate = (PixelsReadGlobalState &)*gstate_p;

	auto result = make_unique<PixelsReadLocalState>();

	result->column_ids = input.column_ids;


	auto fieldNames = bind_data.fileSchema->getFieldNames();


	for(column_t column_id : input.column_ids) {
		if (!IsRowIdColumnId(column_id)) {
			result->column_names.emplace_back(fieldNames.at(column_id));
		}
	}

	PixelsParallelStateNext(context.client, bind_data, *result, gstate);
	PixelsReaderOption option;
	option.setSkipCorruptRecords(true);
	option.setTolerantSchemaEvolution(true);
	option.setEnableEncodedColumnVector(true);

	// includeCols comes from the caller of PixelsPageSource
	option.setIncludeCols(result->column_names);
	option.setRGRange(0, result->reader->getRowGroupNum());
	option.setQueryId(1);
	result->pixelsRecordReader = result->reader->read(option);

	return std::move(result);
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
			case TypeDescription::DECIMAL:
			    return_types.emplace_back(LogicalType::DECIMAL(columnType->getPrecision(), columnType->getScale()));
			    break;
			//        case TypeDescription::STRING:
			//            break;
			case TypeDescription::DATE:
			    return_types.emplace_back(LogicalType::DATE);
			    break;
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
				throw InvalidArgumentException("bad column type in TransformDuckdbType: " + std::to_string(type->getCategory()));
		}
	}
}
void PixelsScanFunction::TransformDuckdbChunk(const vector<column_t> & column_ids,
                                              const shared_ptr<VectorizedRowBatch> & vectorizedRowBatch,
                                              DataChunk & output,
                                              const std::shared_ptr<TypeDescription> & schema) {
	int row_batch_id = 0;
	for(int col_id = 0; col_id < column_ids.size(); col_id++) {
		if (IsRowIdColumnId(column_ids.at(col_id))) {
			    Value constant_42 = Value::BIGINT(42);
			    output.data[col_id].Reference(constant_42);
			    continue;
		}
		auto col = vectorizedRowBatch->cols.at(row_batch_id);
		auto colSchema = schema->getChildren().at(row_batch_id);
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
		    case TypeDescription::DECIMAL:{
			    auto decimalCol = std::static_pointer_cast<DecimalColumnVector>(col);
			    Vector vector(LogicalType::DECIMAL(colSchema->getPrecision(), colSchema->getScale()),
			                  	(data_ptr_t)decimalCol->vector);
			    output.data.at(col_id).Reference(vector);
			    break;
		    }

			//        case TypeDescription::STRING:
			//            break;
			case TypeDescription::DATE:{
			    auto dateCol = std::static_pointer_cast<DateColumnVector>(col);
			    assert(sizeof(*dateCol->dates) == sizeof(duckdb::date_t));
			    Vector vector(LogicalType::DATE,
			                  (data_ptr_t)dateCol->dates);
			    output.data.at(col_id).Reference(vector);
			    break;
		    }

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
			    for(int row_id = 0; row_id < vectorizedRowBatch->rowCount; row_id++) {
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
		row_batch_id++;
	}
}

bool PixelsScanFunction::PixelsParallelStateNext(ClientContext &context, const PixelsReadBindData &bind_data,
                                                  PixelsReadLocalState &scan_data,
                                                  PixelsReadGlobalState &parallel_state) {
	unique_lock<mutex> parallel_lock(parallel_state.lock);
	if (parallel_state.error_opening_file) {
		throw InvalidArgumentException("PixelsScanInitLocal: file open error.");
	}
	if (parallel_state.file_index >= parallel_state.readers.size()) {
		parallel_lock.unlock();
		return false;
	}
	if(scan_data.reader.get() != nullptr) {
		scan_data.reader->close();
	}
	if (parallel_state.readers[parallel_state.file_index]) {
		scan_data.reader = parallel_state.readers[parallel_state.file_index];
		scan_data.file_index = parallel_state.file_index;
	} else {
		auto footerCache = std::make_shared<PixelsFooterCache>();
		auto builder = std::make_shared<PixelsReaderBuilder>();
		shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
		scan_data.reader = builder->setPath(bind_data.files.at(parallel_state.file_index))
		                     ->setStorage(storage)
		                     ->setPixelsFooterCache(footerCache)
		                     ->build();
		parallel_state.readers[parallel_state.file_index] = scan_data.reader;
		scan_data.file_index = parallel_state.file_index;
	}
	scan_data.batch_index = parallel_state.file_index;
	parallel_state.file_index++;
	parallel_lock.unlock();
	return true;
}

}