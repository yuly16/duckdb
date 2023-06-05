#include "duckdb.hpp"
#include <iostream>
#include "profiler/TimeProfiler.h"

using namespace duckdb;

string pixels_lineitem_path("'/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-smallrg/lineitem/v-0-order/*.pxl'");
string pixels_supplier_path("'/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian/supplier/v-0-order/*.pxl'");
string pixels_nation_path("'/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian/nation/v-0-order/*.pxl'");
string pixels_region_path("'/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian/region/v-0-order/*.pxl'");
string pixels_partsupp_path("'/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian/partsupp/v-0-order/*.pxl'");
string pixels_part_path("'/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian/part/v-0-order/*.pxl'");

string parquet_lineitem_path("'/data/s1725-2/liyu/parquet-tpch-1g/lineitem/*'");
string parquet_supplier_path("'/data/s1725-2/liyu/parquet-tpch-1g/supplier/*'");
string parquet_nation_path("'/data/s1725-2/liyu/parquet-tpch-1g/nation/*'");
string parquet_region_path("'/data/s1725-2/liyu/parquet-tpch-1g/region/*'");
string parquet_partsupp_path("'/data/s1725-2/liyu/parquet-tpch-1g/partsupp/*'");
string parquet_part_path("'/data/s1725-2/liyu/parquet-tpch-1g/part/*'");


void tpch_q01(Connection & con) {
	auto result = con.Query("SELECT\n"
	                        "    l_returnflag,\n"
	                        "    l_linestatus,\n"
	                        "    sum(l_quantity) AS sum_qty,\n"
	                        "    sum(l_extendedprice) AS sum_base_price,\n"
	                        "    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,\n"
	                        "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,\n"
	                        "    avg(l_quantity) AS avg_qty,\n"
	                        "    avg(l_extendedprice) AS avg_price,\n"
	                        "    avg(l_discount) AS avg_disc,\n"
	                        "    count(*) AS count_order\n"
	                        "FROM\n"
	                        "    " + pixels_lineitem_path + ",\n"
	                        "WHERE\n"
	                        "    l_shipdate <= CAST('1998-09-02' AS date)\n"
	                        "GROUP BY\n"
	                        "    l_returnflag,\n"
	                        "    l_linestatus\n"
	                        "ORDER BY\n"
	                        "    l_returnflag,\n"
	                        "    l_linestatus;");
	result->Print();
}


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
	                        "    " + pixels_part_path + ",\n"
	                        "    " + pixels_partsupp_path + ",\n"
							"    " + pixels_supplier_path + ",\n"
							"    " + pixels_nation_path + ",\n"
							"    " + pixels_region_path + "\n"
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
	                                      "    " + pixels_partsupp_path + ",\n"
	                                      "    " + pixels_supplier_path + ",\n"
											"    " + pixels_nation_path + ",\n"
	                                      "    " + pixels_region_path + "\n"
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
	std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

//	tpch_q01(con);
//	tpch_q02(con);
//	// parquet example
//	{
//		auto result = con.Query("SELECT * from parquet_scan(" + parquet_nation_path + ");");
//		result->Print();
//	}
	// pixels example
	{
		auto result = con.Query("SELECT * from " + pixels_nation_path + ";");
		result->Print();
	}

	std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
	std::cout << "Time difference = " << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "[µs]" << std::endl;

	TimeProfiler::Instance().Print();
}
