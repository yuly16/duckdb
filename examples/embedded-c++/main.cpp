#include "duckdb.hpp"

using namespace duckdb;

int main() {
	DuckDB db(nullptr);

	Connection con(db);
//	con.Query("CREATE TABLE integers(i INTEGER)");
//	con.Query("INSERT INTO integers VALUES (3)");
//	auto result = con.Query("SELECT * FROM integers");
//	auto result = con.Query("select count(*) from '/home/liyu/duckdb/data/csv/customer.csv'");
//	auto result = con.Query("select count(*) from '/home/liyu/duckdb/data/parquet-testing/candidate.parquet'");
//	auto result = con.Query("select sum(l3) from '/home/liyu/duckdb/data/parquet-testing/decimals.parquet'");
//	auto result = con.Query("select * from '/home/liyu/duckdb/data/json/timestamp_example.json'");
//	auto result = con.Query("select * from read_json_objects('/home/liyu/duckdb/data/json/timestamp_example.json')");
//	auto result = con.Query("select fuck('Jane') as result");
	auto result = con.Query("select * from '/scratch/liyu/opt/pixels_file/pixels-tpch-0_1/nation/v-0-order/20230316154717_0.pxl'");
	result->Print();
}
