#pragma once

#include "common/api.h"
#include "exception.h"

namespace monad {
namespace common {

class MONAD_API InterruptException : public Exception {
public:
    explicit InterruptException() : Exception("Interrupted."){};
};

} // namespace common
} // namespace monad
