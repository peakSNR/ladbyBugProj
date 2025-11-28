#pragma once

#include "function/function.h"

namespace monad {
namespace function {

struct NeighborSampleFunction final {
    static constexpr const char* name = "NEIGHBOR_SAMPLE";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace monad

