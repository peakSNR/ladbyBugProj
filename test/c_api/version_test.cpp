#include <filesystem>
#include <fstream>

#include "c_api/monad.h"
#include "c_api_test/c_api_test.h"
#include "gtest/gtest.h"

using namespace monad::main;
using namespace monad::testing;
using namespace monad::common;

class CApiVersionTest : public CApiTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendMonadRootPath("dataset/tinysnb/");
    }

    void TearDown() override { APIDBTest::TearDown(); }
};

class EmptyCApiVersionTest : public CApiVersionTest {
public:
    std::string getInputDir() override { return "empty"; }
};

TEST_F(EmptyCApiVersionTest, GetVersion) {
    monad_connection_destroy(&connection);
    monad_database_destroy(&_database);
    auto version = monad_get_version();
    ASSERT_NE(version, nullptr);
    ASSERT_STREQ(version, MONAD_CMAKE_VERSION);
    monad_destroy_string(version);
}

TEST_F(CApiVersionTest, GetStorageVersion) {
    auto storageVersion = monad_get_storage_version();
    if (inMemMode) {
        GTEST_SKIP();
    }
    // Reset the database to ensure that the lock on db file is released.
    monad_connection_destroy(&connection);
    monad_database_destroy(&_database);
    auto data = std::filesystem::path(databasePath);
    std::ifstream dbFile;
    dbFile.open(data, std::ios::binary);
    ASSERT_TRUE(dbFile.is_open());
    char magic[6];
    dbFile.read(magic, 5);
    magic[5] = '\0';
    ASSERT_STREQ(magic, "MONAD");
    uint64_t actualVersion;
    dbFile.read(reinterpret_cast<char*>(&actualVersion), sizeof(actualVersion));
    dbFile.close();
    ASSERT_EQ(storageVersion, actualVersion);
}

TEST_F(EmptyCApiVersionTest, GetStorageVersion) {
    auto storageVersion = monad_get_storage_version();
    if (inMemMode) {
        GTEST_SKIP();
    }
    // Reset the database to ensure that the lock on db file is released.
    monad_connection_destroy(&connection);
    monad_database_destroy(&_database);
    auto data = std::filesystem::path(databasePath);
    std::ifstream dbFile;
    dbFile.open(data, std::ios::binary);
    ASSERT_TRUE(dbFile.is_open());
    char magic[6];
    dbFile.read(magic, 5);
    magic[5] = '\0';
    ASSERT_STREQ(magic, "MONAD");
    uint64_t actualVersion;
    dbFile.read(reinterpret_cast<char*>(&actualVersion), sizeof(actualVersion));
    dbFile.close();
    ASSERT_EQ(storageVersion, actualVersion);
}
