#include "main/duckdb_extension.h"

#include "connector/duckdb_connector.h"
#include "main/client_context.h"
#include "s3fs_config.h"
#include "storage/duckdb_storage.h"

namespace monad {
namespace duckdb_extension {

void DuckdbExtension::load(main::ClientContext* context) {
    auto db = context->getDatabase();
    db->registerStorageExtension(EXTENSION_NAME, std::make_unique<DuckDBStorageExtension>(*db));
    loadRemoteFSOptions(context);
}

} // namespace duckdb_extension
} // namespace monad

#if defined(BUILD_DYNAMIC_LOAD)
extern "C" {
// Because we link against the static library on windows, we implicitly inherit MONAD_STATIC_DEFINE,
// which cancels out any exporting, so we can't use MONAD_API.
#if defined(_WIN32)
#define INIT_EXPORT __declspec(dllexport)
#else
#define INIT_EXPORT __attribute__((visibility("default")))
#endif
INIT_EXPORT void init(monad::main::ClientContext* context) {
    monad::duckdb_extension::DuckdbExtension::load(context);
}

INIT_EXPORT const char* name() {
    return monad::duckdb_extension::DuckdbExtension::EXTENSION_NAME;
}
}
#endif
