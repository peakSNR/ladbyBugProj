#include <thread>

#include "c_api_test/c_api_test.h"

using ::testing::Test;
using namespace monad::main;
using namespace monad::testing;

class CApiConnectionTest : public CApiTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendMonadRootPath("dataset/tinysnb/");
    }
};

TEST_F(CApiConnectionTest, CreationAndDestroy) {
    monad_connection connection;
    monad_state state;
    auto _database = getDatabase();
    state = monad_connection_init(_database, &connection);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(connection._connection, nullptr);
    auto connectionCpp = static_cast<Connection*>(connection._connection);
    ASSERT_NE(connectionCpp, nullptr);
    monad_connection_destroy(&connection);
}

TEST_F(CApiConnectionTest, CreationAndDestroyWithNullDatabase) {
    monad_connection connection;
    monad_state state;
    state = monad_connection_init(nullptr, &connection);
    ASSERT_EQ(state, MonadError);
}

TEST_F(CApiConnectionTest, Query) {
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) RETURN a.fName";
    state = monad_connection_query(connection, query, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    ASSERT_NE(resultCpp, nullptr);
    ASSERT_TRUE(resultCpp->isSuccess());
    ASSERT_TRUE(resultCpp->hasNext());
    ASSERT_EQ(resultCpp->getErrorMessage(), "");
    ASSERT_EQ(resultCpp->getNumTuples(), 8);
    ASSERT_EQ(resultCpp->getNumColumns(), 1);
    ASSERT_EQ(resultCpp->getColumnNames()[0], "a.fName");
    monad_query_result_destroy(&result);
}

TEST_F(CApiConnectionTest, SetGetMaxNumThreadForExec) {
    uint64_t maxNumThreadForExec;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_set_max_num_thread_for_exec(connection, 4);
    ASSERT_EQ(state, MonadSuccess);
    auto connectionCpp = static_cast<Connection*>(connection->_connection);
    ASSERT_EQ(connectionCpp->getMaxNumThreadForExec(), 4);
    state = monad_connection_get_max_num_thread_for_exec(connection, &maxNumThreadForExec);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(maxNumThreadForExec, 4);
    state = monad_connection_set_max_num_thread_for_exec(connection, 8);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(connectionCpp->getMaxNumThreadForExec(), 8);
    state = monad_connection_get_max_num_thread_for_exec(connection, &maxNumThreadForExec);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(maxNumThreadForExec, 8);
    monad_connection badConnection;
    ASSERT_EQ(monad_connection_init(nullptr, &badConnection), MonadError);
    state = monad_connection_set_max_num_thread_for_exec(&badConnection, 4);
    ASSERT_EQ(state, MonadError);
    state = monad_connection_get_max_num_thread_for_exec(&badConnection, &maxNumThreadForExec);
    ASSERT_EQ(state, MonadError);
}

TEST_F(CApiConnectionTest, Prepare) {
    monad_prepared_statement statement;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &statement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(statement._prepared_statement, nullptr);
    ASSERT_NE(statement._bound_values, nullptr);
    auto statementCpp = static_cast<PreparedStatement*>(statement._prepared_statement);
    ASSERT_NE(statementCpp, nullptr);
    auto connectionCpp = static_cast<Connection*>(connection->_connection);
    auto result = connectionCpp->execute(statementCpp, std::make_pair(std::string("1"), true));
    ASSERT_TRUE(result->isSuccess());
    ASSERT_TRUE(result->hasNext());
    ASSERT_EQ(result->getErrorMessage(), "");
    ASSERT_EQ(result->getNumTuples(), 1);
    auto tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 3);
    monad_prepared_statement_destroy(&statement);
}

TEST_F(CApiConnectionTest, Execute) {
    monad_prepared_statement statement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &statement);
    ASSERT_EQ(state, MonadSuccess);
    monad_prepared_statement_bind_bool(&statement, "1", true);
    state = monad_connection_execute(connection, &statement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    ASSERT_NE(resultCpp, nullptr);
    ASSERT_TRUE(resultCpp->isSuccess());
    ASSERT_TRUE(resultCpp->hasNext());
    ASSERT_EQ(resultCpp->getErrorMessage(), "");
    ASSERT_EQ(resultCpp->getNumTuples(), 1);
    auto tuple = resultCpp->getNext();
    ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 3);
    monad_prepared_statement_destroy(&statement);
    monad_query_result_destroy(&result);
}

TEST_F(CApiConnectionTest, ExecuteError) {
    monad_prepared_statement preparedStatement;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(preparedStatement._prepared_statement, nullptr);
    ASSERT_EQ(monad_prepared_statement_bind_int64(&preparedStatement, "1", 30), MonadSuccess);
    monad_query_result result;
    ASSERT_EQ(monad_connection_execute(connection, &preparedStatement, &result), MonadError);
    ASSERT_FALSE(monad_query_result_is_success(&result));
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiConnectionTest, QueryTimeout) {
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    ASSERT_EQ(monad_connection_set_query_timeout(connection, 1), MonadSuccess);
    state = monad_connection_query(connection,
        "UNWIND RANGE(1,100000) AS x UNWIND RANGE(1, 100000) AS y RETURN COUNT(x + y);", &result);
    ASSERT_EQ(state, MonadError);
    ASSERT_NE(result._query_result, nullptr);
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    ASSERT_NE(resultCpp, nullptr);
    ASSERT_FALSE(resultCpp->isSuccess());
    ASSERT_EQ(resultCpp->getErrorMessage(), "Interrupted.");
    monad_query_result_destroy(&result);
    monad_connection badConnection;
    ASSERT_EQ(monad_connection_init(nullptr, &badConnection), MonadError);
    ASSERT_EQ(monad_connection_set_query_timeout(&badConnection, 1), MonadError);
}

#ifndef __SINGLE_THREADED__
// The following test is disabled in single-threaded mode because it requires
// a separate thread to run.
TEST_F(CApiConnectionTest, Interrupt) {
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    std::atomic<bool> finished = false;

    // Interrupt the query after 100ms
    // This may happen too early, so try again until the query function finishes.
    std::thread t([&connection, &finished]() {
        do {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            monad_connection_interrupt(connection);
        } while (!finished);
    });
    state = monad_connection_query(connection,
        "UNWIND RANGE(1,100000) AS x UNWIND RANGE(1, 100000) AS y RETURN COUNT(x + y);", &result);
    finished = true;
    ASSERT_EQ(state, MonadError);
    ASSERT_NE(result._query_result, nullptr);
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    ASSERT_NE(resultCpp, nullptr);
    ASSERT_FALSE(resultCpp->isSuccess());
    ASSERT_EQ(resultCpp->getErrorMessage(), "Interrupted.");
    monad_query_result_destroy(&result);
    t.join();
}
#endif
