import duckdb
import pyarrow as pa
import tempfile
import pathlib
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.compute as pc
import time



def tpch_q01():
    print("tpch q01:")
    lineitem_path = '/data/s1725-1/liyu/parquet_data/parquet-tpch-300g/lineitem/'
    # connect to an in-memory database
    con = duckdb.connect()
    # Reads Parquet File to an Arrow Table
    start = time.time()
    lineitem = pq.read_table(lineitem_path, columns = ["l_returnflag",
                                                       "l_linestatus",
                                                       "l_quantity",
                                                       "l_extendedprice",
                                                       "l_discount",
                                                       "l_tax",
                                                       "l_shipdate"], use_threads=True)
    end0 = time.time()
    print("parquet read time: " + str(end0 - start))
    # we can run a SQL query on this and print the result
    result = con.execute("SELECT\n"
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
                      "FROM lineitem\n"
                      "WHERE\n"
                      "    l_shipdate <= CAST('1998-09-02' AS date)\n"
                      "GROUP BY\n"
                      "    l_returnflag,\n"
                      "    l_linestatus\n"
                      "ORDER BY\n"
                      "    l_returnflag,\n"
                      "    l_linestatus;").fetchone()
    end = time.time()
    print("total time: " + str(end - start))
    print(result)

def tpch_q03():
    print("tpch q06:")
    lineitem_path = '/data/s1725-1/liyu/parquet_data/parquet-tpch-300g/lineitem/'
    # connect to an in-memory database
    con = duckdb.connect()
    # Reads Parquet File to an Arrow Table
    start = time.time()
    lineitem = pq.read_table(lineitem_path, columns = ["l_quantity",
                                                       "l_extendedprice",
                                                       "l_discount",
                                                       "l_shipdate"], use_threads=True)
    end0 = time.time()
    print("parquet read time: " + str(end0 - start))
    # we can run a SQL query on this and print the result
    result = con.execute("SELECT\n"
                         "    sum(l_extendedprice * l_discount) AS revenue\n"
                         "FROM\n"
                         "    lineitem\n"
                         "WHERE\n"
                         "    l_shipdate >= CAST('1994-01-01' AS date)\n"
                         "    AND l_shipdate < CAST('1995-01-01' AS date)\n"
                         "    AND l_discount BETWEEN 0.05\n"
                         "    AND 0.07\n"
                         "    AND l_quantity < 24;").fetchone()
    end = time.time()
    print("total time: " + str(end - start))
    print(result)


def tpch_q06():
    print("tpch q06:")
    lineitem_path = '/data/s1725-1/liyu/parquet_data/parquet-tpch-300g/lineitem/'
    # connect to an in-memory database
    con = duckdb.connect()
    # Reads Parquet File to an Arrow Table
    start = time.time()
    lineitem = pq.read_table(lineitem_path, columns = ["l_quantity",
                                                       "l_extendedprice",
                                                       "l_discount",
                                                       "l_shipdate"], use_threads=True)
    end0 = time.time()
    print("parquet read time: " + str(end0 - start))
    # we can run a SQL query on this and print the result
    result = con.execute("SELECT\n"
                         "    sum(l_extendedprice * l_discount) AS revenue\n"
                         "FROM\n"
                         "    lineitem\n"
                         "WHERE\n"
                         "    l_shipdate >= CAST('1994-01-01' AS date)\n"
                         "    AND l_shipdate < CAST('1995-01-01' AS date)\n"
                         "    AND l_discount BETWEEN 0.05\n"
                         "    AND 0.07\n"
                         "    AND l_quantity < 24;").fetchone()
    end = time.time()
    print("total time: " + str(end - start))
    print(result)


def main():
    tpch_q01()
    tpch_q06()
if __name__ == "__main__":
    main()