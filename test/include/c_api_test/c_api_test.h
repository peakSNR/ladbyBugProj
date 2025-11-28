#pragma once

#include "c_api/monad.h"
#include "graph_test/base_graph_test.h"

namespace monad {
namespace testing {

// This class starts database in on-disk mode.
class APIDBTest : public BaseGraphTest {
public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        createDBAndConn();
        initGraph();
    }
};

class CApiTest : public APIDBTest {
public:
    monad_database _database;
    monad_connection connection;

    void SetUp() override {
        APIDBTest::SetUp();
        auto* connCppPointer = conn.release();
        auto* databaseCppPointer = database.release();
        connection = monad_connection{connCppPointer};
        _database = monad_database{databaseCppPointer};
    }

    std::string getDatabasePath() { return databasePath; }

    monad_database* getDatabase() { return &_database; }

    monad_connection* getConnection() { return &connection; }

    void TearDown() override {
        monad_connection_destroy(&connection);
        monad_database_destroy(&_database);
        APIDBTest::TearDown();
    }
};

} // namespace testing
} // namespace monad
