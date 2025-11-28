#pragma once

#include "common/api.h"
#include "exception.h"

namespace monad {
namespace common {

class MONAD_API StorageException : public Exception {
public:
    explicit StorageException(const std::string& msg) : Exception("Storage exception: " + msg){};
};

} // namespace common
} // namespace monad
