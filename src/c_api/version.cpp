#include "main/version.h"

#include "c_api/helpers.h"
#include "c_api/monad.h"

char* monad_get_version() {
    return convertToOwnedCString(monad::main::Version::getVersion());
}

uint64_t monad_get_storage_version() {
    return monad::main::Version::getStorageVersion();
}
