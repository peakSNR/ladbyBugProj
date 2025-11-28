#include <fstream>

#include "c_api_test/c_api_test.h"

using namespace monad::main;
using namespace monad::common;
using namespace monad::processor;
using namespace monad::testing;

class CApiQueryResultTest : public CApiTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendMonadRootPath("dataset/tinysnb/");
    }
};

static monad_value* copy_flat_tuple(monad_flat_tuple* tuple, uint32_t tupleLen) {
    monad_value* ret = (monad_value*)malloc(sizeof(monad_value) * tupleLen);
    for (uint32_t i = 0; i < tupleLen; i++) {
        monad_flat_tuple_get_value(tuple, i, &ret[i]);
    }
    return ret;
}

TEST_F(CApiQueryResultTest, GetNextExample) {
    auto conn = getConnection();

    monad_query_result result;
    monad_connection_query(conn, "MATCH (p:person) RETURN p.*", &result);

    uint64_t num_tuples = monad_query_result_get_num_tuples(&result);
    monad_value** tuples = (monad_value**)malloc(sizeof(monad_value*) * num_tuples);
    for (uint64_t i = 0; i < num_tuples; ++i) {
        monad_flat_tuple tuple;
        monad_query_result_get_next(&result, &tuple);
        tuples[i] = copy_flat_tuple(&tuple, monad_query_result_get_num_columns(&result));
        monad_flat_tuple_destroy(&tuple);
    }

    for (uint64_t i = 0; i < num_tuples; ++i) {
        for (uint64_t j = 0; j < monad_query_result_get_num_columns(&result); ++j) {
            ASSERT_FALSE(monad_value_is_null(&tuples[i][j]));
            monad_value_destroy(&tuples[i][j]);
        }
        free(tuples[i]);
    }

    free((void*)tuples);

    monad_query_result_destroy(&result);
}

TEST_F(CApiQueryResultTest, GetErrorMessage) {
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection, "MATCH (a:person) RETURN COUNT(*)", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    char* errorMessage = monad_query_result_get_error_message(&result);
    monad_query_result_destroy(&result);

    state = monad_connection_query(connection, "MATCH (a:personnnn) RETURN COUNT(*)", &result);
    ASSERT_EQ(state, MonadError);
    ASSERT_FALSE(monad_query_result_is_success(&result));
    errorMessage = monad_query_result_get_error_message(&result);
    ASSERT_EQ(std::string(errorMessage), "Binder exception: Table personnnn does not exist.");
    monad_query_result_destroy(&result);
    monad_destroy_string(errorMessage);
}

TEST_F(CApiQueryResultTest, ToString) {
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection, "MATCH (a:person) RETURN COUNT(*)", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    char* str_repr = monad_query_result_to_string(&result);
    ASSERT_EQ(state, MonadSuccess);
    monad_destroy_string(str_repr);
    monad_query_result_destroy(&result);
}

TEST_F(CApiQueryResultTest, GetNumColumns) {
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection, "MATCH (a:person) RETURN a.fName, a.age, a.height",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_EQ(monad_query_result_get_num_columns(&result), 3);
    monad_query_result_destroy(&result);
}

TEST_F(CApiQueryResultTest, GetColumnName) {
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection, "MATCH (a:person) RETURN a.fName, a.age, a.height",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    char* columnName;
    ASSERT_EQ(monad_query_result_get_column_name(&result, 0, &columnName), MonadSuccess);
    ASSERT_EQ(std::string(columnName), "a.fName");
    monad_destroy_string(columnName);
    ASSERT_EQ(monad_query_result_get_column_name(&result, 1, &columnName), MonadSuccess);
    ASSERT_EQ(std::string(columnName), "a.age");
    monad_destroy_string(columnName);
    ASSERT_EQ(monad_query_result_get_column_name(&result, 2, &columnName), MonadSuccess);
    ASSERT_EQ(std::string(columnName), "a.height");
    monad_destroy_string(columnName);
    ASSERT_EQ(monad_query_result_get_column_name(&result, 222, &columnName), MonadError);
    monad_query_result_destroy(&result);
}

TEST_F(CApiQueryResultTest, GetColumnDataType) {
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection, "MATCH (a:person) RETURN a.fName, a.age, a.height",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    monad_logical_type type;
    ASSERT_EQ(monad_query_result_get_column_data_type(&result, 0, &type), MonadSuccess);
    auto typeCpp = (LogicalType*)(type._data_type);
    ASSERT_EQ(typeCpp->getLogicalTypeID(), LogicalTypeID::STRING);
    monad_data_type_destroy(&type);
    ASSERT_EQ(monad_query_result_get_column_data_type(&result, 1, &type), MonadSuccess);
    typeCpp = (LogicalType*)(type._data_type);
    ASSERT_EQ(typeCpp->getLogicalTypeID(), LogicalTypeID::INT64);
    monad_data_type_destroy(&type);
    ASSERT_EQ(monad_query_result_get_column_data_type(&result, 2, &type), MonadSuccess);
    typeCpp = (LogicalType*)(type._data_type);
    ASSERT_EQ(typeCpp->getLogicalTypeID(), LogicalTypeID::FLOAT);
    monad_data_type_destroy(&type);
    ASSERT_EQ(monad_query_result_get_column_data_type(&result, 222, &type), MonadError);
    monad_query_result_destroy(&result);
}

// TODO(Guodong): Fix this test by adding support of STRUCT in arrow table export.
// TEST_F(CApiQueryResultTest, GetArrowSchema) {
//    auto connection = getConnection();
//    auto result = monad_connection_query(
//        connection, "MATCH (p:person)-[k:knows]-(q:person) RETURN p.fName, k, q.fName");
//    ASSERT_TRUE(monad_query_result_is_success(result));
//    auto schema = monad_query_result_get_arrow_schema(result);
//    ASSERT_STREQ(schema.name, "monad_query_result");
//    ASSERT_EQ(schema.n_children, 3);
//    ASSERT_STREQ(schema.children[0]->name, "p.fName");
//    ASSERT_STREQ(schema.children[1]->name, "k");
//    ASSERT_STREQ(schema.children[2]->name, "q.fName");
//
//    schema.release(&schema);
//    monad_query_result_destroy(result);
//}

TEST_F(CApiQueryResultTest, GetQuerySummary) {
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection, "MATCH (a:person) RETURN a.fName, a.age, a.height",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    monad_query_summary summary;
    state = monad_query_result_get_query_summary(&result, &summary);
    ASSERT_EQ(state, MonadSuccess);
    auto compilingTime = monad_query_summary_get_compiling_time(&summary);
    ASSERT_GT(compilingTime, 0);
    auto executionTime = monad_query_summary_get_execution_time(&summary);
    ASSERT_GT(executionTime, 0);
    monad_query_summary_destroy(&summary);
    monad_query_result_destroy(&result);
}

TEST_F(CApiQueryResultTest, GetNext) {
    monad_query_result result;
    monad_flat_tuple row;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        "MATCH (a:person) RETURN a.fName, a.age ORDER BY a.fName", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));

    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &row);
    ASSERT_EQ(state, MonadSuccess);
    auto flatTupleCpp = (FlatTuple*)(row._flat_tuple);
    ASSERT_EQ(flatTupleCpp->getValue(0)->getValue<std::string>(), "Alice");
    ASSERT_EQ(flatTupleCpp->getValue(1)->getValue<int64_t>(), 35);

    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &row);
    ASSERT_EQ(state, MonadSuccess);
    flatTupleCpp = (FlatTuple*)(row._flat_tuple);
    ASSERT_EQ(flatTupleCpp->getValue(0)->getValue<std::string>(), "Bob");
    ASSERT_EQ(flatTupleCpp->getValue(1)->getValue<int64_t>(), 30);
    monad_flat_tuple_destroy(&row);

    while (monad_query_result_has_next(&result)) {
        monad_query_result_get_next(&result, &row);
    }
    ASSERT_FALSE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &row);
    ASSERT_EQ(state, MonadError);
    monad_query_result_destroy(&result);
}

TEST_F(CApiQueryResultTest, ResetIterator) {
    monad_query_result result;
    monad_flat_tuple row;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        "MATCH (a:person) RETURN a.fName, a.age ORDER BY a.fName", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));

    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &row);
    ASSERT_EQ(state, MonadSuccess);
    auto flatTupleCpp = (FlatTuple*)(row._flat_tuple);
    ASSERT_EQ(flatTupleCpp->getValue(0)->getValue<std::string>(), "Alice");
    ASSERT_EQ(flatTupleCpp->getValue(1)->getValue<int64_t>(), 35);

    monad_query_result_reset_iterator(&result);

    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &row);
    ASSERT_EQ(state, MonadSuccess);
    flatTupleCpp = (FlatTuple*)(row._flat_tuple);
    ASSERT_EQ(flatTupleCpp->getValue(0)->getValue<std::string>(), "Alice");
    ASSERT_EQ(flatTupleCpp->getValue(1)->getValue<int64_t>(), 35);
    monad_flat_tuple_destroy(&row);

    monad_query_result_destroy(&result);
}

TEST_F(CApiQueryResultTest, MultipleQuery) {
    monad_query_result result;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection, "return 1; return 2; return 3;", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));

    char* str = monad_query_result_to_string(&result);
    ASSERT_EQ(std::string(str), "1\n1\n");
    monad_destroy_string(str);

    ASSERT_TRUE(monad_query_result_has_next_query_result(&result));
    monad_query_result next_query_result;
    ASSERT_EQ(monad_query_result_get_next_query_result(&result, &next_query_result), MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&next_query_result));
    str = monad_query_result_to_string(&next_query_result);
    ASSERT_EQ(std::string(str), "2\n2\n");
    monad_destroy_string(str);
    monad_query_result_destroy(&next_query_result);

    ASSERT_EQ(monad_query_result_get_next_query_result(&result, &next_query_result), MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&next_query_result));
    str = monad_query_result_to_string(&next_query_result);
    ASSERT_EQ(std::string(str), "3\n3\n");
    monad_destroy_string(str);

    ASSERT_FALSE(monad_query_result_has_next_query_result(&result));
    ASSERT_EQ(monad_query_result_get_next_query_result(&result, &next_query_result), MonadError);
    monad_query_result_destroy(&next_query_result);

    monad_query_result_destroy(&result);
}
