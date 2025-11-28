#include "main/version.h"

#include "storage/storage_version_info.h"

namespace monad {
namespace main {
const char* Version::getVersion() {
    return MONAD_CMAKE_VERSION;
}

uint64_t Version::getStorageVersion() {
    return storage::StorageVersionInfo::getStorageVersion();
}
} // namespace main
} // namespace monad
