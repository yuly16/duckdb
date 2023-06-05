#include "duckdb.hpp"
#include <iostream>
#include "utils/ConfigFactory.h"

using namespace duckdb;

int main() {
	DuckDB db(nullptr);
	Connection con(db);
	std::string demo = ConfigFactory::Instance().getPixelsDirectory() + "tests/data/nation_multiple/*.pxl";
	auto result = con.Query("SELECT * from '" + demo + "';d");
	result->Print();
}
