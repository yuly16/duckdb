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

	// multiple table parquet example
//	{
//		auto result = con.Query("select * from '/home/yuly/project/duckdb/data/parquet-testing/glob/t1.parquet'");
//		result->Print();
//	}

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
//	{
//		auto result = con.Query("SELECT\n"
//		                        "    count(*) AS count_order\n"
//		                        "FROM\n"
//		                        "    '/home/yuly/project/data/lineitem/*.pxl'\n"
//		                        "WHERE\n"
//		                        "    l_shipdate <= CAST('1998-09-02' AS date)\n"
//		                        "GROUP BY\n"
//		                        "    l_returnflag,\n"
//		                        "    l_linestatus\n"
//		                        "ORDER BY\n"
//		                        "    l_returnflag,\n"
//		                        "    l_linestatus;");
//		result->Print();
//	}
    {
        auto result = con.Query("SELECT count(*) from '/scratch/liyu/opt/pixels_file/pixels-tpch-10/lineitem/v-0-order/*.pxl';");
        result->Print();
    }
}
