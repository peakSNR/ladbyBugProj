#pragma once

#include <cstdint>

#include "common/api.h"

namespace monad {
namespace common {

class Value;

class NestedVal {
public:
    MONAD_API static uint32_t getChildrenSize(const Value* val);

    MONAD_API static Value* getChildVal(const Value* val, uint32_t idx);
};

} // namespace common
} // namespace monad
