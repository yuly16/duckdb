#include "duckdb.hpp"
#include <iostream>
using namespace duckdb;

string lineitem_path("'/scratch/liyu/opt/pixels_file/pixels-tpch-0_1/lineitem/v-0-order/*.pxl'");
string supplier_path("'/scratch/liyu/opt/pixels_file/pixels-tpch-0_1/supplier/v-0-order/*.pxl'");
string nation_path("'/scratch/liyu/opt/pixels_file/pixels-tpch-0_1/nation/v-0-order/*.pxl'");
string region_path("'/scratch/liyu/opt/pixels_file/pixels-tpch-0_1/region/v-0-order/*.pxl'");
string partsupp_path("'/scratch/liyu/opt/pixels_file/pixels-tpch-0_1/partsupp/v-0-order/*.pxl'");
string part_path("'/scratch/liyu/opt/pixels_file/pixels-tpch-0_1/part/v-0-order/*.pxl'");

void tpch_q02(Connection & con) {
	auto result = con.Query("SELECT\n"
	                        "    s_acctbal,\n"
	                        "    s_name,\n"
	                        "    n_name,\n"
	                        "    p_partkey,\n"
	                        "    p_mfgr,\n"
	                        "    s_address,\n"
	                        "    s_phone,\n"
	                        "    s_comment\n"
	                        "FROM\n"
	                        "    " + part_path + ",\n"
	                        "    " + partsupp_path + ",\n"
							"    " + supplier_path + ",\n"
							"    " + nation_path + ",\n"
							"    " + region_path + "\n"
	                        "WHERE\n"
	                        "    p_partkey = ps_partkey\n"
	                        "    AND s_suppkey = ps_suppkey\n"
	                        "    AND p_size = 15\n"
	                        "    AND p_type LIKE '%BRASS'\n"
	                        "    AND s_nationkey = n_nationkey\n"
	                        "    AND n_regionkey = r_regionkey\n"
	                        "    AND r_name = 'EUROPE'\n"
	                        "    AND ps_supplycost = (\n"
	                        "        SELECT\n"
	                        "            min(ps_supplycost)\n"
	                        "        FROM\n"
	                                      "    " + partsupp_path + ",\n"
	                                      "    " + supplier_path + ",\n"
											"    " + nation_path + ",\n"
	                                      "    " + region_path + "\n"
	                        "        WHERE\n"
	                        "            p_partkey = ps_partkey\n"
	                        "            AND s_suppkey = ps_suppkey\n"
	                        "            AND s_nationkey = n_nationkey\n"
	                        "            AND n_regionkey = r_regionkey\n"
	                        "            AND r_name = 'EUROPE')\n"
	                        "ORDER BY\n"
	                        "    s_acctbal DESC,\n"
	                        "    n_name,\n"
	                        "    s_name,\n"
	                        "    p_partkey\n"
	                        "LIMIT 100;");
	result->Print();
}

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

//	{
//		auto result = con.Query("select count(*) from '/scratch/liyu/opt/duckdb/duckdb_benchmark_data/lineitem_10.parquet'");
//		result->Print();
//	}

	//	auto result = con.Query("select * from '/home/liyu/duckdb/data/json/timestamp_example.json'");
//	auto result = con.Query("select * from read_json_objects('/home/liyu/duckdb/data/json/timestamp_example.json')");
//	auto result = con.Query("select fuck('Jane') as result");
//	auto result = con.Query("select n_name from '/home/yuly/project/pixels-reader-cxx/tests/data/nation_0_1.pxl'");

//	auto result1 = con.Query("select * from '/home/yuly/project/pixels-reader-cxx/tests/data/supplier_0_1.pxl'");
//	result1->Print();


	// pixels tpch q1
//	{
//		auto result = con.Query("SELECT\n"
//		                        "    l_returnflag,\n"
//		                        "    l_linestatus,\n"
//		                        "    sum(l_quantity) AS sum_qty,\n"
//		                        "    sum(l_extendedprice) AS sum_base_price,\n"
//		                        "    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,\n"
//		                        "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,\n"
//		                        "    avg(l_quantity) AS avg_qty,\n"
//		                        "    avg(l_extendedprice) AS avg_price,\n"
//		                        "    avg(l_discount) AS avg_disc,\n"
//		                        "    count(*) AS count_order\n"
//		                        "FROM\n"
//		                        "    '/scratch/liyu/opt/pixels_file/pixels-tpch-10/lineitem/v-0-order/*.pxl'\n"
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
        auto result = con.Query("SELECT\n"
		                        "    p_partkey\n"
		                        "FROM\n"
		                        "    '/scratch/liyu/opt/parquet_file/tpch_0_01/part.parquet',\n"
		                        "    '/scratch/liyu/opt/parquet_file/tpch_0_01/partsupp.parquet'\n"
		                        "WHERE\n"
		                        "    ps_supplycost = (\n"
		                        "        SELECT\n"
		                        "            min(ps_supplycost)\n"
		                        "        FROM\n"
		                        "    '/scratch/liyu/opt/parquet_file/tpch_0_01/partsupp.parquet'\n"
		                        "        WHERE\n"
		                        "            p_partkey = ps_partkey)\n"
		                        "LIMIT 100;");
        result->Print();
    }

//	tpch_q02(con);

//	{
//		auto result = con.Query("SELECT * from '/scratch/liyu/opt/pixels_file/pixels-tpch-0_1/partsupp/v-0-order/*.pxl';");
//		result->Print();
//	}
}
