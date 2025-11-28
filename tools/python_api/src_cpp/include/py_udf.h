#pragma once

#include <string>

#include "common/types/types.h"
#include "function/function.h"
#include "pybind_include.h"

using monad::common::LogicalTypeID;
using monad::function::function_set;

namespace monad {
namespace main {
class ClientContext;
} // namespace main
} // namespace monad

class PyUDF {

public:
    static function_set toFunctionSet(const std::string& name, const py::function& udf,
        const py::list& paramTypes, const std::string& resultType, bool defaultNull,
        bool catchExceptions, monad::main::ClientContext* context);
};
