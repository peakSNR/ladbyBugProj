#pragma once

#include "common/api.h"
#include "exception.h"

namespace monad {
namespace common {

class MONAD_API ConnectionException : public Exception {
public:
    explicit ConnectionException(const std::string& msg)
        : Exception("Connection exception: " + msg){};
};

} // namespace common
} // namespace monad
