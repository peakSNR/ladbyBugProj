#pragma once

#include "common/api.h"
#include "exception.h"

namespace monad {
namespace common {

class MONAD_API InternalException : public Exception {
public:
    explicit InternalException(const std::string& msg) : Exception(msg){};
};

} // namespace common
} // namespace monad
