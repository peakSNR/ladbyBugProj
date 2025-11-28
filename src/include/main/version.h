#pragma once
#include <cstdint>

#include "common/api.h"
namespace monad {
namespace main {

struct Version {
public:
    /**
     * @brief Get the version of the Monad library.
     * @return const char* The version of the Monad library.
     */
    MONAD_API static const char* getVersion();

    /**
     * @brief Get the storage version of the Monad library.
     * @return uint64_t The storage version of the Monad library.
     */
    MONAD_API static uint64_t getStorageVersion();
};
} // namespace main
} // namespace monad
