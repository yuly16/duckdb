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
//	auto result = con.Query("select * from '/home/liyu/duckdb/data/json/timestamp_example.json'");
	auto result = con.Query("select quack('Jane') as result");
	result->Print();
}
