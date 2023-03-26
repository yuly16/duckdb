//
// Created by liyu on 3/26/23.
//

#ifndef EXAMPLE_C_PIXELSREADLOCALSTATE_HPP
#define EXAMPLE_C_PIXELSREADLOCALSTATE_HPP

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>


namespace duckdb {

struct PixelsReadLocalState : public LocalTableFunctionState {
	int a;
};

}

#endif // EXAMPLE_C_PIXELSREADLOCALSTATE_HPP
