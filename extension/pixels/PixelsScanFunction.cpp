//
// Created by liyu on 3/26/23.
//

#include "PixelsScanFunction.hpp"
#include "profiler/TimeProfiler.h"
namespace duckdb {

static idx_t PixelsScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                                     LocalTableFunctionState *local_state,
                                     GlobalTableFunctionState *global_state) {
	auto &data = (PixelsReadLocalState &)*local_state;
	return data.curr_batch_index;
}

static double PixelsProgress(ClientContext &context, const FunctionData *bind_data_p,
                              const GlobalTableFunctionState *global_state) {
	auto &bind_data = (PixelsReadBindData &)*bind_data_p;
	if (bind_data.files.empty()) {
		return 100.0;
	}
	auto percentage = bind_data.curFileId * 100.0 / bind_data.files.size();
	return percentage;
}

static unique_ptr<NodeStatistics> PixelsCardinality(ClientContext &context, const FunctionData *bind_data) {
	auto &data = (PixelsReadBindData &)*bind_data;

	return make_uniq<NodeStatistics>(data.initialPixelsReader->getNumberOfRows() * data.files.size());
}

TableFunctionSet PixelsScanFunction::GetFunctionSet() {
	TableFunctionSet set("pixels_scan");
	TableFunction table_function({LogicalType::VARCHAR}, PixelsScanImplementation, PixelsScanBind,
	                             PixelsScanInitGlobal, PixelsScanInitLocal);
	table_function.projection_pushdown = true;
//	table_function.filter_pushdown = true;
//	table_function.filter_prune = true;
	table_function.get_batch_index = PixelsScanGetBatchIndex;
	table_function.cardinality = PixelsCardinality;
	table_function.table_scan_progress = PixelsProgress;
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

	if(data.currPixelsRecordReader.get() == nullptr ||
            (data.currPixelsRecordReader->isEndOfFile() && data.rowOffset >= data.vectorizedRowBatch->rowCount)) {
        if(data.vectorizedRowBatch.get() != nullptr) {
            data.vectorizedRowBatch->close();
        }
		if(data.currPixelsRecordReader.get() != nullptr) {
            data.currPixelsRecordReader.reset();
        }

		if(!PixelsParallelStateNext(context, bind_data, data, gstate)) {
			return;
		} else {
            // In the last state, we don't generate a new pixelsRecordReader
            // since no file is remaining.
            if(!data.is_last_state) {
                PixelsReaderOption option;
                option.setSkipCorruptRecords(true);
                option.setTolerantSchemaEvolution(true);
                option.setEnableEncodedColumnVector(true);


                // includeCols comes from the caller of PixelsPageSource
                option.setIncludeCols(data.column_names);
                option.setRGRange(0, data.nextReader->getRowGroupNum());
                option.setQueryId(1);
                data.nextPixelsRecordReader = data.nextReader->read(option);
            }
		}
	}
    auto currPixelsRecordReader = std::static_pointer_cast<PixelsRecordReaderImpl>(data.currPixelsRecordReader);
    auto nextPixelsRecordReader = std::static_pointer_cast<PixelsRecordReaderImpl>(data.nextPixelsRecordReader);

    if(data.vectorizedRowBatch != nullptr && data.rowOffset >= data.vectorizedRowBatch->rowCount) {
//		data.vectorizedRowBatch->close();
		data.vectorizedRowBatch = nullptr;
	}
	if(data.vectorizedRowBatch == nullptr) {

        currPixelsRecordReader->asyncReadComplete(data.column_names.size());
        if(!data.is_last_state) {
            nextPixelsRecordReader->read();
        }
		data.vectorizedRowBatch = currPixelsRecordReader->readRowGroup(false);

		data.rowOffset = 0;
	}


	std::shared_ptr<TypeDescription> resultSchema = data.currPixelsRecordReader->getResultSchema();

	auto thisOutputChunkRows = MinValue<idx_t>(STANDARD_VECTOR_SIZE, data.vectorizedRowBatch->rowCount - data.rowOffset);

	output.SetCardinality(thisOutputChunkRows);
	TransformDuckdbChunk(data, output, resultSchema, thisOutputChunkRows);
	data.rowOffset += thisOutputChunkRows;
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

	auto result = make_uniq<PixelsReadBindData>();
	result->initialPixelsReader = pixelsReader;
	result->fileSchema = fileSchema;
	result->files = files;

	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> PixelsScanFunction::PixelsScanInitGlobal(
    						ClientContext &context, TableFunctionInitInput &input) {

	auto &bind_data = (PixelsReadBindData &)*input.bind_data;

	auto result = make_uniq<PixelsReadGlobalState>();

	result->readers = std::vector<shared_ptr<PixelsReader>>(bind_data.files.size(), nullptr);

	result->initialPixelsReader = bind_data.initialPixelsReader;
	result->readers[0] = bind_data.initialPixelsReader;

	result->file_index = 0;
	result->max_threads = 24;

	result->batch_index = 0;

	return std::move(result);
}

unique_ptr<LocalTableFunctionState> PixelsScanFunction::PixelsScanInitLocal(
    						ExecutionContext &context, TableFunctionInitInput &input,
                            GlobalTableFunctionState *gstate_p) {
	auto &bind_data = (PixelsReadBindData &)*input.bind_data;

	auto &gstate = (PixelsReadGlobalState &)*gstate_p;

	auto result = make_uniq<PixelsReadLocalState>();

	result->column_ids = input.column_ids;


	auto fieldNames = bind_data.fileSchema->getFieldNames();


	for(column_t column_id : input.column_ids) {
		if (!IsRowIdColumnId(column_id)) {
			result->column_names.emplace_back(fieldNames.at(column_id));
		}
	}
    result->is_first_state = true;
    result->is_last_state = false;
    result->next_file_index = 0;
    result->next_batch_index = 0;
    result->curr_file_index = 0;
    result->curr_batch_index = 0;
	if(!PixelsParallelStateNext(context.client, bind_data, *result, gstate)) {
		return nullptr;
	}
	PixelsReaderOption option;
	option.setSkipCorruptRecords(true);
	option.setTolerantSchemaEvolution(true);
	option.setEnableEncodedColumnVector(true);

	// includeCols comes from the caller of PixelsPageSource
	option.setIncludeCols(result->column_names);
	option.setRGRange(0, result->nextReader->getRowGroupNum());
	option.setQueryId(1);

//    option.setBatchSize(STANDARD_VECTOR_SIZE);
	result->nextPixelsRecordReader = result->nextReader->read(option);

    result->vectorizedRowBatch = nullptr;
	::DirectUringRandomAccessFile::Initialize();
    auto nextPixelsRecordReader = std::static_pointer_cast<PixelsRecordReaderImpl>(result->nextPixelsRecordReader);
    nextPixelsRecordReader->read();
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
				return_types.emplace_back(LogicalType::INTEGER);
			    break;
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
void PixelsScanFunction::TransformDuckdbChunk(PixelsReadLocalState & data,
                                              DataChunk & output,
                                              const std::shared_ptr<TypeDescription> & schema,
                                              uint64_t thisOutputChunkRows) {

	int row_batch_id = 0;
	int row_offset = data.rowOffset;
	auto column_ids = data.column_ids;
	auto vectorizedRowBatch = data.vectorizedRowBatch;
	for(uint64_t col_id = 0; col_id < column_ids.size(); col_id++) {
		if (IsRowIdColumnId(column_ids.at(col_id))) {
			    Value constant_42 = Value::BIGINT(42);
			    output.data.at(col_id).Reference(constant_42);
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
			case TypeDescription::INT: {
			    auto intCol = std::static_pointer_cast<LongColumnVector>(col);
			    auto result_ptr = FlatVector::GetData<int>(output.data.at(col_id));
			    memcpy(result_ptr, intCol->intVector + row_offset, thisOutputChunkRows * sizeof(int));
//			    for(long i = 0; i < thisOutputChunkRows; i++) {
//				    result_ptr[i] = intCol->intVector[i + row_offset];
//			    }

			    break;
		    }
			case TypeDescription::LONG: {
				auto longCol = std::static_pointer_cast<LongColumnVector>(col);
			    auto result_ptr = FlatVector::GetData<long>(output.data.at(col_id));
			    memcpy(result_ptr, longCol->longVector + row_offset, thisOutputChunkRows * sizeof(long));
//			    for(long i = 0; i < thisOutputChunkRows; i++) {
//				    result_ptr[i] = longCol->longVector[i + row_offset];
//			    }
				break;
			}
			//        case TypeDescription::FLOAT:
			//            break;
			//        case TypeDescription::DOUBLE:
			//            break;
		    case TypeDescription::DECIMAL:{
			    auto decimalCol = std::static_pointer_cast<DecimalColumnVector>(col);
			    auto result_ptr = FlatVector::GetData<long>(output.data.at(col_id));
			    memcpy(result_ptr, decimalCol->vector + row_offset, thisOutputChunkRows * sizeof(long));
//			    for(long i = 0; i < thisOutputChunkRows; i++) {
//				    result_ptr[i] = decimalCol->vector[i + row_offset];
//			    }
			    break;
		    }

			//        case TypeDescription::STRING:
			//            break;
			case TypeDescription::DATE:{
			    auto dateCol = std::static_pointer_cast<DateColumnVector>(col);
			    auto result_ptr = FlatVector::GetData<int>(output.data.at(col_id));
			    memcpy(result_ptr, dateCol->dates + row_offset, thisOutputChunkRows * sizeof(int));
//			    for(long i = 0; i < thisOutputChunkRows; i++) {
//				    result_ptr[i] = dateCol->dates[i + row_offset];
//			    }
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
			    for(uint64_t row_id = 0; row_id < thisOutputChunkRows; row_id++) {
				    int length = binaryCol->lens[row_id + row_offset];
				    const char * data = (const char *)binaryCol->vector[row_id + row_offset] + binaryCol->start[row_id + row_offset];
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

    // In the following two cases, the state ends:
    // 1. When PixelsScanInitLocal invokes this function, if all files are
    // fetched by other threads, this means this thread doesn't need do anything, so just return false;
    // 2. When PixelsScanImplementation invokes this function (scan_data.next_file_index > -1), if
    // scan_data.next_file_index >= (int) parallel_state.readers.size(), it means the current file is already
    // done, so the function return false.
    if ((scan_data.is_first_state && parallel_state.file_index >= parallel_state.readers.size()) ||
            scan_data.is_last_state) {
		::BufferPool::Reset();
		// if async io is enabled, we need to unregister uring buffer
		if(ConfigFactory::Instance().boolCheckProperty("localfs.enable.async.io")) {
			if(ConfigFactory::Instance().getProperty("localfs.async.lib") == "iouring") {
				::DirectUringRandomAccessFile::Reset();
			} else if(ConfigFactory::Instance().getProperty("localfs.async.lib") == "aio") {
				throw InvalidArgumentException("PhysicalLocalReader::readAsync: We don't support aio for our async read yet.");
			}
		}
        parallel_lock.unlock();
        return false;
    }

    scan_data.curr_file_index = scan_data.next_file_index;
    scan_data.curr_batch_index = scan_data.next_batch_index;
    scan_data.next_file_index = parallel_state.file_index;
    scan_data.next_batch_index = scan_data.next_file_index;
    scan_data.curr_file_name = scan_data.next_file_name;
    scan_data.is_first_state = false;
    parallel_state.file_index++;
    parallel_lock.unlock();
    // The below code uses global state but no race happens, so we don't need the lock anymore
    

    if(scan_data.currReader.get() != nullptr) {
        scan_data.currReader->close();
    }

    scan_data.currReader = scan_data.nextReader;
    scan_data.currPixelsRecordReader = scan_data.nextPixelsRecordReader;
    if(scan_data.next_file_index < parallel_state.readers.size()) {
        if (parallel_state.readers[scan_data.next_file_index]) {
            scan_data.next_file_name = bind_data.files.at(scan_data.next_file_index);
            scan_data.nextReader = parallel_state.readers[scan_data.next_file_index];
        } else {
            auto footerCache = std::make_shared<PixelsFooterCache>();
            auto builder = std::make_shared<PixelsReaderBuilder>();
            shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
            scan_data.next_file_name = bind_data.files.at(scan_data.next_file_index);
            scan_data.nextReader = builder->setPath(bind_data.files.at(scan_data.next_file_index))
                    ->setStorage(storage)
                    ->setPixelsFooterCache(footerCache)
                    ->build();
            parallel_state.readers[scan_data.next_file_index] = scan_data.nextReader;
        }
    } else {
        scan_data.is_last_state = true;
        scan_data.nextReader = nullptr;
        scan_data.nextPixelsRecordReader = nullptr;
    }
    ::BufferPool::Switch();
    return true;
}

}
