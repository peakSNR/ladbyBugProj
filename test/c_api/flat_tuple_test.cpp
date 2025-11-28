#include "c_api_test/c_api_test.h"

using namespace monad::common;
using namespace monad::main;
using namespace monad::testing;

class CApiFlatTupleTest : public CApiTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendMonadRootPath("dataset/tinysnb/");
    }
};

TEST_F(CApiFlatTupleTest, GetValue) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        "MATCH (a:person) RETURN a.fName, a.age, a.height ORDER BY a.fName LIMIT 1", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_NE(value._value, nullptr);
    auto valueCpp = static_cast<Value*>(value._value);
    ASSERT_NE(valueCpp, nullptr);
    ASSERT_EQ(valueCpp->getDataType().getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(valueCpp->getValue<std::string>(), "Alice");
    monad_value_destroy(&value);
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 1, &value), MonadSuccess);
    ASSERT_NE(value._value, nullptr);
    valueCpp = static_cast<Value*>(value._value);
    ASSERT_NE(valueCpp, nullptr);
    ASSERT_EQ(valueCpp->getDataType().getLogicalTypeID(), LogicalTypeID::INT64);
    ASSERT_EQ(valueCpp->getValue<int64_t>(), 35);
    monad_value_destroy(&value);
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 2, &value), MonadSuccess);
    ASSERT_NE(value._value, nullptr);
    valueCpp = static_cast<Value*>(value._value);
    ASSERT_NE(valueCpp, nullptr);
    ASSERT_EQ(valueCpp->getDataType().getLogicalTypeID(), LogicalTypeID::FLOAT);
    ASSERT_FLOAT_EQ(valueCpp->getValue<float>(), 1.731);
    monad_value_destroy(&value);
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 222, &value), MonadError);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);
}

TEST_F(CApiFlatTupleTest, ToString) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        "MATCH (a:person) RETURN a.fName, a.age, a.height ORDER BY a.fName LIMIT 1", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    auto columnWidths = (uint32_t*)malloc(3 * sizeof(uint32_t));
    columnWidths[0] = 10;
    columnWidths[1] = 5;
    columnWidths[2] = 10;
    char* str = monad_flat_tuple_to_string(&flatTuple);
    ASSERT_EQ(std::string(str), "Alice|35|1.731000\n");
    monad_destroy_string(str);
    free(columnWidths);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);
}
