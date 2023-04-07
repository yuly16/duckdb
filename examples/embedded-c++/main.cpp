#include "duckdb.hpp"
#include <iostream>
using namespace duckdb;

int main() {
	DuckDB db(nullptr);

	Connection con(db);
//	con.Query("CREATE TABLE integers(i INTEGER)");
//	con.Query("INSERT INTO integers VALUES (3)");
//	auto result = con.Query("SELECT * FROM integers");
//	auto result = con.Query("select count(*) from '/home/liyu/duckdb/data/csv/customer.csv'");
//	auto result = con.Query("select * from '/home/yuly/project/duckdb/data/parquet-testing/date.parquet'");

	// large table parquet example
//	{
//		auto result = con.Query("select * from '/home/yuly/project/duckdb/data/parquet-testing/leftdate3_192_loop_1.parquet'");
//		result->Print();
//	}

	//	auto result = con.Query("select * from '/home/liyu/duckdb/data/json/timestamp_example.json'");
//	auto result = con.Query("select * from read_json_objects('/home/liyu/duckdb/data/json/timestamp_example.json')");
//	auto result = con.Query("select fuck('Jane') as result");
//	auto result = con.Query("select n_name from '/home/yuly/project/pixels-reader-cxx/tests/data/nation_0_1.pxl'");

//	auto result1 = con.Query("select * from '/home/yuly/project/pixels-reader-cxx/tests/data/supplier_0_1.pxl'");
//	result1->Print();

	// pixels
	{
		auto result = con.Query("select * from '/home/yuly/project/data/orders_0_1.pxl' limit 100");
		result->Print();
	}

}
