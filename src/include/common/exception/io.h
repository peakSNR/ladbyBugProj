#pragma once

#include "exception.h"

namespace monad {
namespace common {

class MONAD_API IOException : public Exception {
public:
    explicit IOException(const std::string& msg) : Exception("IO exception: " + msg) {}
};

} // namespace common
} // namespace monad
