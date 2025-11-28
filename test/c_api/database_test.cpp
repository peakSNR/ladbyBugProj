#include "c_api/monad.h"
#include "graph_test/base_graph_test.h"
#include "gtest/gtest.h"

using namespace monad::main;
using namespace monad::testing;

// This class starts database without initializing graph.
class APIEmptyDBTest : public BaseGraphTest {
    std::string getInputDir() override { KU_UNREACHABLE; }
};

class CApiDatabaseTest : public APIEmptyDBTest {
public:
    void SetUp() override {
        APIEmptyDBTest::SetUp();
        defaultSystemConfig = monad_default_system_config();

        // limit memory usage by keeping max number of threads small
        defaultSystemConfig.max_num_threads = 2;
        auto maxDBSizeEnv = TestHelper::getSystemEnv("MAX_DB_SIZE");
        if (!maxDBSizeEnv.empty()) {
            defaultSystemConfig.max_db_size = std::stoull(maxDBSizeEnv);
        }
    }

    monad_system_config defaultSystemConfig;
};

TEST_F(CApiDatabaseTest, CreationAndDestroy) {
    monad_database database;
    monad_state state;
    auto databasePathCStr = databasePath.c_str();
    auto systemConfig = defaultSystemConfig;
    state = monad_database_init(databasePathCStr, systemConfig, &database);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(database._database, nullptr);
    auto databaseCpp = static_cast<Database*>(database._database);
    ASSERT_NE(databaseCpp, nullptr);
    monad_database_destroy(&database);
}

TEST_F(CApiDatabaseTest, CreationReadOnly) {
    monad_database database;
    monad_connection connection;
    monad_query_result queryResult;
    monad_state state;
    auto databasePathCStr = databasePath.c_str();
    auto systemConfig = defaultSystemConfig;
    // First, create a read-write database.
    state = monad_database_init(databasePathCStr, systemConfig, &database);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(database._database, nullptr);
    auto databaseCpp = static_cast<Database*>(database._database);
    ASSERT_NE(databaseCpp, nullptr);
    monad_database_destroy(&database);
    // Now, access the same database read-only.
    systemConfig.read_only = true;
    state = monad_database_init(databasePathCStr, systemConfig, &database);
    if (databasePath == "" || databasePath == ":memory:") {
        ASSERT_EQ(state, MonadError);
        ASSERT_EQ(database._database, nullptr);
        return;
    }
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(database._database, nullptr);
    databaseCpp = static_cast<Database*>(database._database);
    ASSERT_NE(databaseCpp, nullptr);
    // Try to write to the database.
    state = monad_connection_init(&database, &connection);
    ASSERT_EQ(state, MonadSuccess);
    state = monad_connection_query(&connection,
        "CREATE NODE TABLE User(name STRING, age INT64, reg_date DATE, PRIMARY KEY (name))",
        &queryResult);
    ASSERT_EQ(state, MonadError);
    ASSERT_FALSE(monad_query_result_is_success(&queryResult));
    monad_query_result_destroy(&queryResult);
    monad_connection_destroy(&connection);
    monad_database_destroy(&database);
}

TEST_F(CApiDatabaseTest, CreationInMemory) {
    monad_database database;
    monad_state state;
    auto databasePathCStr = (char*)"";
    state = monad_database_init(databasePathCStr, defaultSystemConfig, &database);
    ASSERT_EQ(state, MonadSuccess);
    monad_database_destroy(&database);
    databasePathCStr = (char*)":memory:";
    state = monad_database_init(databasePathCStr, defaultSystemConfig, &database);
    ASSERT_EQ(state, MonadSuccess);
    monad_database_destroy(&database);
}

#ifndef __WASM__ // home directory is not available in WASM
TEST_F(CApiDatabaseTest, CreationHomeDir) {
    monad_database database;
    monad_connection connection;
    monad_state state;
    auto databasePathCStr = (char*)"~/ku_test.db";
    state = monad_database_init(databasePathCStr, defaultSystemConfig, &database);
    ASSERT_EQ(state, MonadSuccess);
    state = monad_connection_init(&database, &connection);
    ASSERT_EQ(state, MonadSuccess);
    auto homePath =
        getClientContext(*(Connection*)(connection._connection))->getClientConfig()->homeDirectory;
    monad_connection_destroy(&connection);
    monad_database_destroy(&database);
    std::filesystem::remove_all(homePath + "/ku_test.db");
}
#endif

TEST_F(CApiDatabaseTest, CloseQueryResultAndConnectionAfterDatabaseDestroy) {
    monad_database database;
    auto databasePathCStr = (char*)":memory:";
    auto systemConfig = monad_default_system_config();
    systemConfig.buffer_pool_size = 10 * 1024 * 1024; // 10MB
    systemConfig.max_db_size = 1 << 30;               // 1GB
    systemConfig.max_num_threads = 2;
    monad_state state = monad_database_init(databasePathCStr, systemConfig, &database);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(database._database, nullptr);
    monad_connection conn;
    monad_query_result queryResult;
    state = monad_connection_init(&database, &conn);
    ASSERT_EQ(state, MonadSuccess);
    state = monad_connection_query(&conn, "RETURN 1+1", &queryResult);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&queryResult));
    monad_flat_tuple tuple;
    monad_state resultState = monad_query_result_get_next(&queryResult, &tuple);
    ASSERT_EQ(resultState, MonadSuccess);
    monad_value value;
    monad_state valueState = monad_flat_tuple_get_value(&tuple, 0, &value);
    ASSERT_EQ(valueState, MonadSuccess);
    int64_t valueInt = INT64_MAX;
    monad_state valueIntState = monad_value_get_int64(&value, &valueInt);
    ASSERT_EQ(valueIntState, MonadSuccess);
    ASSERT_EQ(valueInt, 2);
    // Destroy database first, this should not crash
    monad_database_destroy(&database);
    // Call monad_connection_query should not crash, but return an error
    state = monad_connection_query(&conn, "RETURN 1+1", &queryResult);
    ASSERT_EQ(state, MonadError);
    // Call monad_query_result_get_next should not crash, but return an error
    resultState = monad_query_result_get_next(&queryResult, &tuple);
    ASSERT_EQ(resultState, MonadError);
    // Now destroy everything, this should not crash
    monad_query_result_destroy(&queryResult);
    monad_connection_destroy(&conn);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&tuple);
}

TEST_F(CApiDatabaseTest, UseConnectionAfterDatabaseDestroy) {
    monad_database db;
    monad_connection conn;
    monad_query_result result;

    auto systemConfig = monad_default_system_config();
    systemConfig.buffer_pool_size = 10 * 1024 * 1024; // 10MB
    systemConfig.max_db_size = 1 << 30;               // 1GB
    systemConfig.max_num_threads = 2;
    auto state = monad_database_init("", systemConfig, &db);
    ASSERT_EQ(state, MonadSuccess);
    state = monad_connection_init(&db, &conn);
    ASSERT_EQ(state, MonadSuccess);
    monad_database_destroy(&db);
    state = monad_connection_query(&conn, "RETURN 0", &result);
    ASSERT_EQ(state, MonadError);

    monad_connection_destroy(&conn);
}
