#include "c_api_test/c_api_test.h"

using namespace monad::main;
using namespace monad::testing;

class CApiPreparedStatementTest : public CApiTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendMonadRootPath("dataset/tinysnb/");
    }
};

TEST_F(CApiPreparedStatementTest, IsSuccess) {
    monad_prepared_statement preparedStatement;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(preparedStatement._prepared_statement, nullptr);
    ASSERT_TRUE(monad_prepared_statement_is_success(&preparedStatement));
    monad_prepared_statement_destroy(&preparedStatement);

    query = "MATCH (a:personnnn) WHERE a.isStudent = $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(preparedStatement._prepared_statement, nullptr);
    ASSERT_FALSE(monad_prepared_statement_is_success(&preparedStatement));
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, GetErrorMessage) {
    monad_prepared_statement preparedStatement;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(preparedStatement._prepared_statement, nullptr);
    ASSERT_EQ(monad_prepared_statement_get_error_message(&preparedStatement), nullptr);
    monad_prepared_statement_destroy(&preparedStatement);

    query = "MATCH (a:personnnn) WHERE a.isStudent = $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(preparedStatement._prepared_statement, nullptr);
    char* message = monad_prepared_statement_get_error_message(&preparedStatement);
    ASSERT_EQ(std::string(message), "Binder exception: Table personnnn does not exist.");
    monad_prepared_statement_destroy(&preparedStatement);
    monad_destroy_string(message);
}

TEST_F(CApiPreparedStatementTest, BindBool) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(monad_prepared_statement_bind_bool(&preparedStatement, "1", true), MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 3);
    monad_query_result_destroy(&result);
    // Bind a different parameter
    ASSERT_EQ(monad_prepared_statement_bind_bool(&preparedStatement, "1", false), MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    resultCpp = static_cast<QueryResult*>(result._query_result);
    tuple = resultCpp->getNext();
    value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 5);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindInt64) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.age > $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(monad_prepared_statement_bind_int64(&preparedStatement, "1", 30), MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 4);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindInt32) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:movies) WHERE a.length > $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(monad_prepared_statement_bind_int32(&preparedStatement, "1", 200), MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindInt16) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.length > $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(monad_prepared_statement_bind_int16(&preparedStatement, "1", 10), MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindInt8) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.level > $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(monad_prepared_statement_bind_int8(&preparedStatement, "1", 3), MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindUInt64) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.code > $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(monad_prepared_statement_bind_uint64(&preparedStatement, "1", 100), MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindUInt32) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.temperature> $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(monad_prepared_statement_bind_uint32(&preparedStatement, "1", 10), MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindUInt16) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.ulength> $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(monad_prepared_statement_bind_uint16(&preparedStatement, "1", 100), MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindUInt8) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.ulevel> $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(monad_prepared_statement_bind_uint8(&preparedStatement, "1", 14), MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindDouble) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.eyeSight > $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(monad_prepared_statement_bind_double(&preparedStatement, "1", 4.5), MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 7);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindFloat) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.height < $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(monad_prepared_statement_bind_float(&preparedStatement, "1", 1.0), MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 1);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindString) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.fName = $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_prepared_statement_is_success(&preparedStatement));
    ASSERT_EQ(monad_prepared_statement_bind_string(&preparedStatement, "1", "Alice"), MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 1);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindDate) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.birthdate > $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_prepared_statement_is_success(&preparedStatement));
    auto date = monad_date_t{0};
    ASSERT_EQ(monad_prepared_statement_bind_date(&preparedStatement, "1", date), MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 4);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindTimestamp) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.registerTime > $1 and cast(a.registerTime, "
                 "\"timestamp_ns\") > $2 and cast(a.registerTime, \"timestamp_ms\") > "
                 "$3 and cast(a.registerTime, \"timestamp_sec\") > $4 and cast(a.registerTime, "
                 "\"timestamp_tz\") > $5 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_prepared_statement_is_success(&preparedStatement));
    auto timestamp = monad_timestamp_t{0};
    auto timestamp_ns = monad_timestamp_ns_t{1};
    auto timestamp_ms = monad_timestamp_ms_t{2};
    auto timestamp_sec = monad_timestamp_sec_t{3};
    auto timestamp_tz = monad_timestamp_tz_t{4};
    ASSERT_EQ(monad_prepared_statement_bind_timestamp(&preparedStatement, "1", timestamp),
        MonadSuccess);
    ASSERT_EQ(monad_prepared_statement_bind_timestamp_ns(&preparedStatement, "2", timestamp_ns),
        MonadSuccess);
    ASSERT_EQ(monad_prepared_statement_bind_timestamp_ms(&preparedStatement, "3", timestamp_ms),
        MonadSuccess);
    ASSERT_EQ(monad_prepared_statement_bind_timestamp_sec(&preparedStatement, "4", timestamp_sec),
        MonadSuccess);
    ASSERT_EQ(monad_prepared_statement_bind_timestamp_tz(&preparedStatement, "5", timestamp_tz),
        MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 7);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindInteval) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.lastJobDuration > $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_prepared_statement_is_success(&preparedStatement));
    auto interval = monad_interval_t{0, 0, 0};
    ASSERT_EQ(monad_prepared_statement_bind_interval(&preparedStatement, "1", interval),
        MonadSuccess);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 8);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindValue) {
    monad_prepared_statement preparedStatement;
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.registerTime > $1 RETURN COUNT(*)";
    state = monad_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_prepared_statement_is_success(&preparedStatement));
    auto timestamp = monad_timestamp_t{0};
    auto timestampValue = monad_value_create_timestamp(timestamp);
    ASSERT_EQ(monad_prepared_statement_bind_value(&preparedStatement, "1", timestampValue),
        MonadSuccess);
    monad_value_destroy(timestampValue);
    state = monad_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(monad_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 7);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&preparedStatement);
}
