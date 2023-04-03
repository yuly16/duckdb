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
//	auto result = con.Query("select name from '/home/liyu/duckdb/data/parquet-testing/candidate.parquet' limit 2");
//	auto result = con.Query("select l3 from '/home/liyu/duckdb/data/parquet-testing/decimals.parquet'");
//	auto result = con.Query("select * from '/home/liyu/duckdb/data/json/timestamp_example.json'");
//	auto result = con.Query("select * from read_json_objects('/home/liyu/duckdb/data/json/timestamp_example.json')");
//	auto result = con.Query("select fuck('Jane') as result");
	auto result = con.Query("select n_name from '/home/liyu/pixels-reader-cxx/tests/data/20230316154717_0.pxl'");
	result->Print();
//	auto result1 = con.Query("select * from '/home/liyu/pixels-reader-cxx/tests/data/20230316154717_0.pxl'");
//	result1->Print();
}
