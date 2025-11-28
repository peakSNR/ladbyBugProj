#include "connector/duckdb_secret_manager.h"

#include "s3fs_config.h"

namespace monad {
namespace duckdb_extension {

static std::string getDuckDBExtensionOptions(httpfs_extension::S3AuthParams monadOptions) {
    std::string options = "";
    options.append(common::stringFormat("KEY_ID '{}',", monadOptions.accessKeyID));
    options.append(common::stringFormat("SECRET '{}',", monadOptions.secretAccessKey));
    options.append(common::stringFormat("ENDPOINT '{}',", monadOptions.endpoint));
    options.append(common::stringFormat("URL_STYLE '{}',", monadOptions.urlStyle));
    options.append(common::stringFormat("REGION '{}',", monadOptions.region));
    return options;
}

std::string DuckDBSecretManager::getRemoteS3FSSecret(main::ClientContext* context,
    const httpfs_extension::S3FileSystemConfig& config) {
    KU_ASSERT(config.fsName == "S3" || config.fsName == "GCS");
    std::string templateQuery = R"(CREATE SECRET {}_secret (
        {}
        TYPE {}
    );)";
    return common::stringFormat(templateQuery, config.fsName,
        getDuckDBExtensionOptions(config.getAuthParams(context)), config.fsName);
}

} // namespace duckdb_extension
} // namespace monad
