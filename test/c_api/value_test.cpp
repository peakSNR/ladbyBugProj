#include "c_api_test/c_api_test.h"

using namespace monad::main;
using namespace monad::common;
using namespace monad::testing;

class CApiValueTest : public CApiTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendMonadRootPath("dataset/tinysnb/");
    }
};

TEST(CApiValueTestEmptyDB, CreateNull) {
    monad_value* value = monad_value_create_null();
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::ANY);
    ASSERT_EQ(cppValue->isNull(), true);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateNullWithDatatype) {
    monad_logical_type type;
    monad_data_type_create(monad_data_type_id::MONAD_INT64, nullptr, 0, &type);
    monad_value* value = monad_value_create_null_with_data_type(&type);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    monad_data_type_destroy(&type);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT64);
    ASSERT_EQ(cppValue->isNull(), true);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, IsNull) {
    monad_value* value = monad_value_create_int64(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(value));
    monad_value_destroy(value);
    value = monad_value_create_null();
    ASSERT_TRUE(monad_value_is_null(value));
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, SetNull) {
    monad_value* value = monad_value_create_int64(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(value));
    monad_value_set_null(value, true);
    ASSERT_TRUE(monad_value_is_null(value));
    monad_value_set_null(value, false);
    ASSERT_FALSE(monad_value_is_null(value));
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateDefault) {
    monad_logical_type type;
    monad_data_type_create(monad_data_type_id::MONAD_INT64, nullptr, 0, &type);
    monad_value* value = monad_value_create_default(&type);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    monad_data_type_destroy(&type);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_FALSE(monad_value_is_null(value));
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT64);
    ASSERT_EQ(cppValue->getValue<int64_t>(), 0);
    monad_value_destroy(value);

    monad_data_type_create(monad_data_type_id::MONAD_STRING, nullptr, 0, &type);
    value = monad_value_create_default(&type);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    monad_data_type_destroy(&type);
    cppValue = static_cast<Value*>(value->_value);
    ASSERT_FALSE(monad_value_is_null(value));
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppValue->getValue<std::string>(), "");
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateBool) {
    monad_value* value = monad_value_create_bool(true);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::BOOL);
    ASSERT_EQ(cppValue->getValue<bool>(), true);
    monad_value_destroy(value);

    value = monad_value_create_bool(false);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::BOOL);
    ASSERT_EQ(cppValue->getValue<bool>(), false);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInt8) {
    monad_value* value = monad_value_create_int8(12);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT8);
    ASSERT_EQ(cppValue->getValue<int8_t>(), 12);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInt16) {
    monad_value* value = monad_value_create_int16(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT16);
    ASSERT_EQ(cppValue->getValue<int16_t>(), 123);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInt32) {
    monad_value* value = monad_value_create_int32(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT32);
    ASSERT_EQ(cppValue->getValue<int32_t>(), 123);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInt64) {
    monad_value* value = monad_value_create_int64(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT64);
    ASSERT_EQ(cppValue->getValue<int64_t>(), 123);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateUInt8) {
    monad_value* value = monad_value_create_uint8(12);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::UINT8);
    ASSERT_EQ(cppValue->getValue<uint8_t>(), 12);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateUInt16) {
    monad_value* value = monad_value_create_uint16(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::UINT16);
    ASSERT_EQ(cppValue->getValue<uint16_t>(), 123);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateUInt32) {
    monad_value* value = monad_value_create_uint32(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::UINT32);
    ASSERT_EQ(cppValue->getValue<uint32_t>(), 123);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateUInt64) {
    monad_value* value = monad_value_create_uint64(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::UINT64);
    ASSERT_EQ(cppValue->getValue<uint64_t>(), 123);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateINT128) {
    monad_value* value = monad_value_create_int128(monad_int128_t{211111111, 100000000});
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT128);
    auto cppTimeStamp = cppValue->getValue<int128_t>();
    ASSERT_EQ(cppTimeStamp.high, 100000000);
    ASSERT_EQ(cppTimeStamp.low, 211111111);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateFloat) {
    monad_value* value = monad_value_create_float(123.456);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::FLOAT);
    ASSERT_FLOAT_EQ(cppValue->getValue<float>(), 123.456);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateDouble) {
    monad_value* value = monad_value_create_double(123.456);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::DOUBLE);
    ASSERT_DOUBLE_EQ(cppValue->getValue<double>(), 123.456);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInternalID) {
    auto internalID = monad_internal_id_t{1, 123};
    monad_value* value = monad_value_create_internal_id(internalID);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INTERNAL_ID);
    auto internalIDCpp = cppValue->getValue<internalID_t>();
    ASSERT_EQ(internalIDCpp.tableID, 1);
    ASSERT_EQ(internalIDCpp.offset, 123);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateDate) {
    monad_value* value = monad_value_create_date(monad_date_t{123});
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::DATE);
    auto cppDate = cppValue->getValue<date_t>();
    ASSERT_EQ(cppDate.days, 123);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateTimeStamp) {
    monad_value* value = monad_value_create_timestamp(monad_timestamp_t{123});
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::TIMESTAMP);
    auto cppTimeStamp = cppValue->getValue<timestamp_t>();
    ASSERT_EQ(cppTimeStamp.value, 123);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateTimeStampNonStandard) {
    monad_value* value_ns = monad_value_create_timestamp_ns(monad_timestamp_ns_t{12345});
    monad_value* value_ms = monad_value_create_timestamp_ms(monad_timestamp_ms_t{123456});
    monad_value* value_sec = monad_value_create_timestamp_sec(monad_timestamp_sec_t{1234567});
    monad_value* value_tz = monad_value_create_timestamp_tz(monad_timestamp_tz_t{12345678});

    ASSERT_FALSE(value_ns->_is_owned_by_cpp);
    ASSERT_FALSE(value_ms->_is_owned_by_cpp);
    ASSERT_FALSE(value_sec->_is_owned_by_cpp);
    ASSERT_FALSE(value_tz->_is_owned_by_cpp);
    auto cppValue_ns = static_cast<Value*>(value_ns->_value);
    auto cppValue_ms = static_cast<Value*>(value_ms->_value);
    auto cppValue_sec = static_cast<Value*>(value_sec->_value);
    auto cppValue_tz = static_cast<Value*>(value_tz->_value);
    ASSERT_EQ(cppValue_ns->getDataType().getLogicalTypeID(), LogicalTypeID::TIMESTAMP_NS);
    ASSERT_EQ(cppValue_ms->getDataType().getLogicalTypeID(), LogicalTypeID::TIMESTAMP_MS);
    ASSERT_EQ(cppValue_sec->getDataType().getLogicalTypeID(), LogicalTypeID::TIMESTAMP_SEC);
    ASSERT_EQ(cppValue_tz->getDataType().getLogicalTypeID(), LogicalTypeID::TIMESTAMP_TZ);

    auto cppTimeStamp_ns = cppValue_ns->getValue<timestamp_ns_t>();
    auto cppTimeStamp_ms = cppValue_ms->getValue<timestamp_ms_t>();
    auto cppTimeStamp_sec = cppValue_sec->getValue<timestamp_sec_t>();
    auto cppTimeStamp_tz = cppValue_tz->getValue<timestamp_tz_t>();
    ASSERT_EQ(cppTimeStamp_ns.value, 12345);
    ASSERT_EQ(cppTimeStamp_ms.value, 123456);
    ASSERT_EQ(cppTimeStamp_sec.value, 1234567);
    ASSERT_EQ(cppTimeStamp_tz.value, 12345678);
    monad_value_destroy(value_ns);
    monad_value_destroy(value_ms);
    monad_value_destroy(value_sec);
    monad_value_destroy(value_tz);
}

TEST(CApiValueTestEmptyDB, CreateInterval) {
    monad_value* value = monad_value_create_interval(monad_interval_t{12, 3, 300});
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INTERVAL);
    auto cppTimeStamp = cppValue->getValue<interval_t>();
    ASSERT_EQ(cppTimeStamp.months, 12);
    ASSERT_EQ(cppTimeStamp.days, 3);
    ASSERT_EQ(cppTimeStamp.micros, 300);
    monad_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateString) {
    monad_value* value = monad_value_create_string((char*)"abcdefg");
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppValue->getValue<std::string>(), "abcdefg");
    monad_value_destroy(value);
}

TEST_F(CApiValueTest, CreateList) {
    auto connection = getConnection();
    monad_value* value1 = monad_value_create_int64(123);
    monad_value* value2 = monad_value_create_int64(456);
    monad_value* value3 = monad_value_create_int64(789);
    monad_value* value4 = monad_value_create_int64(101112);
    monad_value* value5 = monad_value_create_int64(131415);
    monad_value* elements[] = {value1, value2, value3, value4, value5};
    monad_value* value = nullptr;
    monad_state state = monad_value_create_list(5, elements, &value);
    ASSERT_EQ(state, MonadSuccess);
    // Destroy the original values, the list should still be valid
    for (int i = 0; i < 5; ++i) {
        monad_value_destroy(elements[i]);
    }
    ASSERT_FALSE(value->_is_owned_by_cpp);
    monad_prepared_statement stmt;
    state = monad_connection_prepare(connection, (char*)"RETURN $1", &stmt);
    ASSERT_EQ(state, MonadSuccess);
    state = monad_prepared_statement_bind_value(&stmt, "1", value);
    ASSERT_EQ(state, MonadSuccess);
    monad_query_result result;
    state = monad_connection_execute(connection, &stmt, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    monad_flat_tuple flatTuple;
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value outValue;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &outValue), MonadSuccess);
    ASSERT_TRUE(outValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&outValue));
    uint64_t size;
    ASSERT_EQ(monad_value_get_list_size(&outValue, &size), MonadSuccess);
    ASSERT_EQ(size, 5);
    monad_value listElement;
    ASSERT_EQ(monad_value_get_list_element(&outValue, 0, &listElement), MonadSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    int64_t int64Result;
    ASSERT_EQ(monad_value_get_int64(&listElement, &int64Result), MonadSuccess);
    ASSERT_EQ(int64Result, 123);
    monad_value_destroy(&listElement);
    ASSERT_EQ(monad_value_get_list_element(&outValue, 1, &listElement), MonadSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    ASSERT_EQ(monad_value_get_int64(&listElement, &int64Result), MonadSuccess);
    ASSERT_EQ(int64Result, 456);
    monad_value_destroy(&listElement);
    ASSERT_EQ(monad_value_get_list_element(&outValue, 2, &listElement), MonadSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    ASSERT_EQ(monad_value_get_int64(&listElement, &int64Result), MonadSuccess);
    ASSERT_EQ(int64Result, 789);
    monad_value_destroy(&listElement);
    ASSERT_EQ(monad_value_get_list_element(&outValue, 3, &listElement), MonadSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    ASSERT_EQ(monad_value_get_int64(&listElement, &int64Result), MonadSuccess);
    ASSERT_EQ(int64Result, 101112);
    monad_value_destroy(&listElement);
    ASSERT_EQ(monad_value_get_list_element(&outValue, 4, &listElement), MonadSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    ASSERT_EQ(monad_value_get_int64(&listElement, &int64Result), MonadSuccess);
    ASSERT_EQ(int64Result, 131415);
    monad_value_destroy(&listElement);
    monad_value_destroy(&outValue);
    monad_value_destroy(value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&stmt);
}

TEST(CApiValueTestEmptyDB, CreateListDifferentTypes) {
    monad_value* value1 = monad_value_create_int64(123);
    monad_value* value2 = monad_value_create_string((char*)"abcdefg");
    monad_value* elements[] = {value1, value2};
    monad_value* value = nullptr;
    monad_state state = monad_value_create_list(2, elements, &value);
    ASSERT_EQ(state, MonadError);
    monad_value_destroy(value1);
    monad_value_destroy(value2);
}

TEST(CApiValueTestEmptyDB, CreateListEmpty) {
    monad_value* elements[] = {nullptr}; // Must be non-empty
    monad_value* value = nullptr;
    monad_state state = monad_value_create_list(0, elements, &value);
    ASSERT_EQ(state, MonadError);
}

TEST_F(CApiValueTest, CreateListNested) {
    auto connection = getConnection();
    monad_value* value1 = monad_value_create_int64(123);
    monad_value* value2 = monad_value_create_int64(456);
    monad_value* value3 = monad_value_create_int64(789);
    monad_value* value4 = monad_value_create_int64(101112);
    monad_value* value5 = monad_value_create_int64(131415);
    monad_value* elements1[] = {value1, value2, value3};
    monad_value* elements2[] = {value4, value5};
    monad_value* list1 = nullptr;
    monad_value* list2 = nullptr;
    monad_value_create_list(3, elements1, &list1);
    ASSERT_FALSE(list1->_is_owned_by_cpp);
    monad_value_create_list(2, elements2, &list2);
    ASSERT_FALSE(list2->_is_owned_by_cpp);
    monad_value* elements[] = {list1, list2};
    monad_value* nestedList = nullptr;
    monad_state state = monad_value_create_list(2, elements, &nestedList);
    ASSERT_EQ(state, MonadSuccess);
    // Destroy the original values, the list should still be valid
    for (int i = 0; i < 3; ++i) {
        monad_value_destroy(elements1[i]);
    }
    for (int i = 0; i < 2; ++i) {
        monad_value_destroy(elements2[i]);
    }
    monad_value_destroy(list1);
    monad_value_destroy(list2);
    ASSERT_FALSE(nestedList->_is_owned_by_cpp);
    monad_prepared_statement stmt;
    state = monad_connection_prepare(connection, (char*)"RETURN $1", &stmt);
    ASSERT_EQ(state, MonadSuccess);
    state = monad_prepared_statement_bind_value(&stmt, "1", nestedList);
    ASSERT_EQ(state, MonadSuccess);
    monad_query_result result;
    state = monad_connection_execute(connection, &stmt, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    monad_flat_tuple flatTuple;
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value outValue;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &outValue), MonadSuccess);
    ASSERT_TRUE(outValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&outValue));
    uint64_t size;
    ASSERT_EQ(monad_value_get_list_size(&outValue, &size), MonadSuccess);
    ASSERT_EQ(size, 2);
    monad_value listElement;
    ASSERT_EQ(monad_value_get_list_element(&outValue, 0, &listElement), MonadSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&listElement));
    ASSERT_EQ(monad_value_get_list_size(&listElement, &size), MonadSuccess);
    ASSERT_EQ(size, 3);
    monad_value innerListElement;
    ASSERT_EQ(monad_value_get_list_element(&listElement, 0, &innerListElement), MonadSuccess);
    ASSERT_TRUE(innerListElement._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&innerListElement));
    int64_t int64Result;
    ASSERT_EQ(monad_value_get_int64(&innerListElement, &int64Result), MonadSuccess);
    ASSERT_EQ(int64Result, 123);
    monad_value_destroy(&innerListElement);
    ASSERT_EQ(monad_value_get_list_element(&listElement, 1, &innerListElement), MonadSuccess);
    ASSERT_TRUE(innerListElement._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&innerListElement));
    ASSERT_EQ(monad_value_get_int64(&innerListElement, &int64Result), MonadSuccess);
    ASSERT_EQ(int64Result, 456);
    monad_value_destroy(&innerListElement);
    ASSERT_EQ(monad_value_get_list_element(&listElement, 2, &innerListElement), MonadSuccess);
    ASSERT_TRUE(innerListElement._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&innerListElement));
    ASSERT_EQ(monad_value_get_int64(&innerListElement, &int64Result), MonadSuccess);
    ASSERT_EQ(int64Result, 789);
    monad_value_destroy(&innerListElement);
    monad_value_destroy(&listElement);
    ASSERT_EQ(monad_value_get_list_element(&outValue, 1, &listElement), MonadSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&listElement));
    ASSERT_EQ(monad_value_get_list_size(&listElement, &size), MonadSuccess);
    ASSERT_EQ(size, 2);
    monad_value_destroy(&listElement);
    monad_value_destroy(&outValue);
    monad_value_destroy(nestedList);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&stmt);
}

TEST_F(CApiValueTest, CreateStruct) {
    auto connection = getConnection();
    monad_value* value1 = monad_value_create_int16(32);
    monad_value* value2 = monad_value_create_string((char*)"Wong");
    monad_value* value3 = monad_value_create_string((char*)"Kelley");
    monad_value* value4 = monad_value_create_int64(123456);
    monad_value* value5 = monad_value_create_string((char*)"CEO");
    monad_value* value6 = monad_value_create_bool(true);
    monad_value* employmentElements[] = {value5, value6};
    const char* employmentFieldNames[] = {(char*)"title", (char*)"is_current"};
    monad_value* employment = nullptr;
    monad_state state =
        monad_value_create_struct(2, employmentFieldNames, employmentElements, &employment);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_FALSE(employment->_is_owned_by_cpp);
    monad_value_destroy(value5);
    monad_value_destroy(value6);
    monad_value* personElements[] = {value1, value2, value3, value4, employment};
    const char* personFieldNames[] = {(char*)"age", (char*)"first_name", (char*)"last_name",
        (char*)"id", (char*)"employment"};
    monad_value* person = nullptr;
    state = monad_value_create_struct(5, personFieldNames, personElements, &person);
    ASSERT_EQ(state, MonadSuccess);
    monad_value_destroy(value1);
    monad_value_destroy(value2);
    monad_value_destroy(value3);
    monad_value_destroy(value4);
    monad_value_destroy(employment);
    ASSERT_FALSE(person->_is_owned_by_cpp);
    monad_prepared_statement stmt;
    state = monad_connection_prepare(connection, (char*)"RETURN $1", &stmt);
    ASSERT_EQ(state, MonadSuccess);
    state = monad_prepared_statement_bind_value(&stmt, "1", person);
    ASSERT_EQ(state, MonadSuccess);
    monad_query_result result;
    state = monad_connection_execute(connection, &stmt, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    monad_flat_tuple flatTuple;
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value outValue;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &outValue), MonadSuccess);
    ASSERT_TRUE(outValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&outValue));
    uint64_t size;
    state = monad_value_get_struct_num_fields(&outValue, &size);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(size, 5);
    char* structFieldName;
    monad_value structFieldValue;
    state = monad_value_get_struct_field_name(&outValue, 0, &structFieldName);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_STREQ(structFieldName, "age");
    state = monad_value_get_struct_field_value(&outValue, 0, &structFieldValue);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(structFieldValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&structFieldValue));
    int16_t int16Result;
    state = monad_value_get_int16(&structFieldValue, &int16Result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(int16Result, 32);
    monad_value_destroy(&structFieldValue);
    monad_destroy_string(structFieldName);
    state = monad_value_get_struct_field_name(&outValue, 1, &structFieldName);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_STREQ(structFieldName, "first_name");
    state = monad_value_get_struct_field_value(&outValue, 1, &structFieldValue);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(structFieldValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&structFieldValue));
    char* stringResult;
    state = monad_value_get_string(&structFieldValue, &stringResult);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_STREQ(stringResult, "Wong");
    monad_value_destroy(&structFieldValue);
    monad_destroy_string(structFieldName);
    monad_destroy_string(stringResult);
    state = monad_value_get_struct_field_name(&outValue, 2, &structFieldName);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_STREQ(structFieldName, "last_name");
    state = monad_value_get_struct_field_value(&outValue, 2, &structFieldValue);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(structFieldValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&structFieldValue));
    state = monad_value_get_string(&structFieldValue, &stringResult);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_STREQ(stringResult, "Kelley");
    monad_value_destroy(&structFieldValue);
    monad_destroy_string(structFieldName);
    monad_destroy_string(stringResult);
    state = monad_value_get_struct_field_name(&outValue, 3, &structFieldName);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_STREQ(structFieldName, "id");
    state = monad_value_get_struct_field_value(&outValue, 3, &structFieldValue);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(structFieldValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&structFieldValue));
    int64_t int64Result;
    state = monad_value_get_int64(&structFieldValue, &int64Result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(int64Result, 123456);
    monad_value_destroy(&structFieldValue);
    monad_destroy_string(structFieldName);
    state = monad_value_get_struct_field_name(&outValue, 4, &structFieldName);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_STREQ(structFieldName, "employment");
    state = monad_value_get_struct_field_value(&outValue, 4, &structFieldValue);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(structFieldValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&structFieldValue));
    state = monad_value_get_struct_num_fields(&structFieldValue, &size);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(size, 2);
    char* employmentFieldName;
    monad_value employmentFieldValue;
    state = monad_value_get_struct_field_name(&structFieldValue, 0, &employmentFieldName);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_STREQ(employmentFieldName, "title");
    state = monad_value_get_struct_field_value(&structFieldValue, 0, &employmentFieldValue);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(employmentFieldValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&employmentFieldValue));
    state = monad_value_get_string(&employmentFieldValue, &stringResult);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_STREQ(stringResult, "CEO");
    monad_value_destroy(&employmentFieldValue);
    monad_destroy_string(employmentFieldName);
    monad_destroy_string(stringResult);
    state = monad_value_get_struct_field_name(&structFieldValue, 1, &employmentFieldName);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_STREQ(employmentFieldName, "is_current");
    state = monad_value_get_struct_field_value(&structFieldValue, 1, &employmentFieldValue);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(employmentFieldValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&employmentFieldValue));
    bool boolResult;
    state = monad_value_get_bool(&employmentFieldValue, &boolResult);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_EQ(boolResult, true);
    monad_value_destroy(&employmentFieldValue);
    monad_destroy_string(employmentFieldName);
    monad_value_destroy(&structFieldValue);
    monad_destroy_string(structFieldName);
    monad_value_destroy(&outValue);
    monad_value_destroy(person);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&stmt);
}

TEST(CApiValueTestEmptyDB, CreateStructEmpty) {
    const char* fieldNames[] = {(char*)"name"}; // Must be non-empty
    monad_value* values[] = {nullptr};           // Must be non-empty
    monad_value* value = nullptr;
    monad_state state = monad_value_create_struct(0, fieldNames, values, &value);
    ASSERT_EQ(state, MonadError);
}

TEST_F(CApiValueTest, CreateMap) {
    auto connection = getConnection();
    monad_value* key1 = monad_value_create_int64(1);
    monad_value* value1 = monad_value_create_string((char*)"one");
    monad_value* key2 = monad_value_create_int64(2);
    monad_value* value2 = monad_value_create_string((char*)"two");
    monad_value* key3 = monad_value_create_int64(3);
    monad_value* value3 = monad_value_create_string((char*)"three");
    monad_value* keys[] = {key1, key2, key3};
    monad_value* values[] = {value1, value2, value3};
    monad_value* map = nullptr;
    monad_state state = monad_value_create_map(3, keys, values, &map);
    ASSERT_EQ(state, MonadSuccess);
    // Destroy the original values, the map should still be valid
    for (int i = 0; i < 3; ++i) {
        monad_value_destroy(keys[i]);
        monad_value_destroy(values[i]);
    }
    ASSERT_FALSE(map->_is_owned_by_cpp);
    monad_prepared_statement stmt;
    state = monad_connection_prepare(connection, (char*)"RETURN $1", &stmt);
    ASSERT_EQ(state, MonadSuccess);
    state = monad_prepared_statement_bind_value(&stmt, "1", map);
    ASSERT_EQ(state, MonadSuccess);
    monad_query_result result;
    state = monad_connection_execute(connection, &stmt, &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    monad_flat_tuple flatTuple;
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value outValue;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &outValue), MonadSuccess);
    ASSERT_TRUE(outValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&outValue));
    uint64_t size;
    ASSERT_EQ(monad_value_get_map_size(&outValue, &size), MonadSuccess);
    ASSERT_EQ(size, 3);
    monad_value mapValue;
    ASSERT_EQ(monad_value_get_map_value(&outValue, 0, &mapValue), MonadSuccess);
    ASSERT_TRUE(mapValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&mapValue));
    char* stringResult;
    ASSERT_EQ(monad_value_get_string(&mapValue, &stringResult), MonadSuccess);
    ASSERT_STREQ(stringResult, "one");
    monad_value_destroy(&mapValue);
    monad_destroy_string(stringResult);
    ASSERT_EQ(monad_value_get_map_value(&outValue, 1, &mapValue), MonadSuccess);
    ASSERT_TRUE(mapValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&mapValue));
    ASSERT_EQ(monad_value_get_string(&mapValue, &stringResult), MonadSuccess);
    ASSERT_STREQ(stringResult, "two");
    monad_value_destroy(&mapValue);
    monad_destroy_string(stringResult);
    ASSERT_EQ(monad_value_get_map_value(&outValue, 2, &mapValue), MonadSuccess);
    ASSERT_TRUE(mapValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&mapValue));
    ASSERT_EQ(monad_value_get_string(&mapValue, &stringResult), MonadSuccess);
    ASSERT_STREQ(stringResult, "three");
    monad_value_destroy(&mapValue);
    monad_destroy_string(stringResult);
    ASSERT_EQ(monad_value_get_map_key(&outValue, 0, &mapValue), MonadSuccess);
    ASSERT_TRUE(mapValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&mapValue));
    int64_t int64Result;
    ASSERT_EQ(monad_value_get_int64(&mapValue, &int64Result), MonadSuccess);
    ASSERT_EQ(int64Result, 1);
    monad_value_destroy(&mapValue);
    ASSERT_EQ(monad_value_get_map_key(&outValue, 1, &mapValue), MonadSuccess);
    ASSERT_TRUE(mapValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&mapValue));
    ASSERT_EQ(monad_value_get_int64(&mapValue, &int64Result), MonadSuccess);
    ASSERT_EQ(int64Result, 2);
    monad_value_destroy(&mapValue);
    ASSERT_EQ(monad_value_get_map_key(&outValue, 2, &mapValue), MonadSuccess);
    ASSERT_TRUE(mapValue._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&mapValue));
    ASSERT_EQ(monad_value_get_int64(&mapValue, &int64Result), MonadSuccess);
    ASSERT_EQ(int64Result, 3);
    monad_value_destroy(&mapValue);
    monad_value_destroy(&outValue);
    monad_value_destroy(map);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);
    monad_prepared_statement_destroy(&stmt);
}

TEST(CApiValueTestEmptyDB, CreateMapEmpty) {
    monad_value* keys[] = {nullptr};   // Must be non-empty
    monad_value* values[] = {nullptr}; // Must be non-empty
    monad_value* map = nullptr;
    monad_state state = monad_value_create_map(0, keys, values, &map);
    ASSERT_EQ(state, MonadError);
}

TEST(CApiValueTestEmptyDB, Clone) {
    monad_value* value = monad_value_create_string((char*)"abcdefg");
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppValue->getValue<std::string>(), "abcdefg");

    monad_value* clone = monad_value_clone(value);
    monad_value_destroy(value);

    ASSERT_FALSE(clone->_is_owned_by_cpp);
    auto cppClone = static_cast<Value*>(clone->_value);
    ASSERT_EQ(cppClone->getDataType().getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppClone->getValue<std::string>(), "abcdefg");
    monad_value_destroy(clone);
}

TEST(CApiValueTestEmptyDB, Copy) {
    monad_value* value = monad_value_create_string((char*)"abc");

    monad_value* value2 = monad_value_create_string((char*)"abcdefg");
    monad_value_copy(value, value2);
    monad_value_destroy(value2);

    ASSERT_FALSE(monad_value_is_null(value));
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppValue->getValue<std::string>(), "abcdefg");
    monad_value_destroy(value);
}

TEST_F(CApiValueTest, GetListSize) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.workedHours ORDER BY a.ID", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    uint64_t size;
    ASSERT_EQ(monad_value_get_list_size(&value, &size), MonadSuccess);
    ASSERT_EQ(size, 2);

    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_list_size(badValue, &size), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetListElement) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.workedHours ORDER BY a.ID", &result);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    uint64_t size;
    ASSERT_EQ(monad_value_get_list_size(&value, &size), MonadSuccess);
    ASSERT_EQ(size, 2);

    monad_value listElement;
    ASSERT_EQ(monad_value_get_list_element(&value, 0, &listElement), MonadSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    int64_t int64Result;
    ASSERT_EQ(monad_value_get_int64(&listElement, &int64Result), MonadSuccess);
    ASSERT_EQ(int64Result, 10);

    ASSERT_EQ(monad_value_get_list_element(&value, 1, &listElement), MonadSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    ASSERT_EQ(monad_value_get_int64(&listElement, &int64Result), MonadSuccess);
    ASSERT_EQ(int64Result, 5);
    monad_value_destroy(&listElement);

    ASSERT_EQ(monad_value_get_list_element(&value, 222, &listElement), MonadError);

    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);
}

TEST_F(CApiValueTest, GetStructNumFields) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    monad_flat_tuple_get_value(&flatTuple, 0, &value);
    uint64_t numFields;
    ASSERT_EQ(monad_value_get_struct_num_fields(&value, &numFields), MonadSuccess);
    ASSERT_EQ(numFields, 14);

    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_struct_num_fields(badValue, &numFields), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetStructFieldName) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    char* fieldName;
    ASSERT_EQ(monad_value_get_struct_field_name(&value, 0, &fieldName), MonadSuccess);
    ASSERT_STREQ(fieldName, "rating");
    monad_destroy_string(fieldName);

    ASSERT_EQ(monad_value_get_struct_field_name(&value, 1, &fieldName), MonadSuccess);
    ASSERT_STREQ(fieldName, "stars");
    monad_destroy_string(fieldName);

    ASSERT_EQ(monad_value_get_struct_field_name(&value, 2, &fieldName), MonadSuccess);
    ASSERT_STREQ(fieldName, "views");
    monad_destroy_string(fieldName);

    ASSERT_EQ(monad_value_get_struct_field_name(&value, 3, &fieldName), MonadSuccess);
    ASSERT_STREQ(fieldName, "release");
    monad_destroy_string(fieldName);

    ASSERT_EQ(monad_value_get_struct_field_name(&value, 4, &fieldName), MonadSuccess);
    ASSERT_STREQ(fieldName, "release_ns");
    monad_destroy_string(fieldName);

    ASSERT_EQ(monad_value_get_struct_field_name(&value, 5, &fieldName), MonadSuccess);
    ASSERT_STREQ(fieldName, "release_ms");
    monad_destroy_string(fieldName);

    ASSERT_EQ(monad_value_get_struct_field_name(&value, 6, &fieldName), MonadSuccess);
    ASSERT_STREQ(fieldName, "release_sec");
    monad_destroy_string(fieldName);

    ASSERT_EQ(monad_value_get_struct_field_name(&value, 7, &fieldName), MonadSuccess);
    ASSERT_STREQ(fieldName, "release_tz");
    monad_destroy_string(fieldName);

    ASSERT_EQ(monad_value_get_struct_field_name(&value, 8, &fieldName), MonadSuccess);
    ASSERT_STREQ(fieldName, "film");
    monad_destroy_string(fieldName);

    ASSERT_EQ(monad_value_get_struct_field_name(&value, 222, &fieldName), MonadError);

    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);
}

TEST_F(CApiValueTest, GetStructFieldValue) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);

    monad_value fieldValue;
    ASSERT_EQ(monad_value_get_struct_field_value(&value, 0, &fieldValue), MonadSuccess);
    monad_logical_type fieldType;
    monad_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(monad_data_type_get_id(&fieldType), MONAD_DOUBLE);
    double doubleValue;
    ASSERT_EQ(monad_value_get_double(&fieldValue, &doubleValue), MonadSuccess);
    ASSERT_DOUBLE_EQ(doubleValue, 1223);
    monad_data_type_destroy(&fieldType);

    ASSERT_EQ(monad_value_get_struct_field_value(&value, 1, &fieldValue), MonadSuccess);
    monad_value_get_data_type(&fieldValue, &fieldType);
    monad_data_type_destroy(&fieldType);

    ASSERT_EQ(monad_value_get_struct_field_value(&value, 2, &fieldValue), MonadSuccess);
    monad_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(monad_data_type_get_id(&fieldType), MONAD_INT64);
    int64_t int64Value;
    ASSERT_EQ(monad_value_get_int64(&fieldValue, &int64Value), MonadSuccess);
    monad_data_type_destroy(&fieldType);

    ASSERT_EQ(monad_value_get_struct_field_value(&value, 3, &fieldValue), MonadSuccess);
    monad_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(monad_data_type_get_id(&fieldType), MONAD_TIMESTAMP);
    monad_timestamp_t timestamp;
    ASSERT_EQ(monad_value_get_timestamp(&fieldValue, &timestamp), MonadSuccess);
    ASSERT_EQ(timestamp.value, 1297442662000000);
    monad_data_type_destroy(&fieldType);

    ASSERT_EQ(monad_value_get_struct_field_value(&value, 4, &fieldValue), MonadSuccess);
    monad_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(monad_data_type_get_id(&fieldType), MONAD_TIMESTAMP_NS);
    monad_timestamp_ns_t timestamp_ns;
    ASSERT_EQ(monad_value_get_timestamp_ns(&fieldValue, &timestamp_ns), MonadSuccess);
    ASSERT_EQ(timestamp_ns.value, 1297442662123456000);
    monad_data_type_destroy(&fieldType);

    ASSERT_EQ(monad_value_get_struct_field_value(&value, 5, &fieldValue), MonadSuccess);
    monad_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(monad_data_type_get_id(&fieldType), MONAD_TIMESTAMP_MS);
    monad_timestamp_ms_t timestamp_ms;
    ASSERT_EQ(monad_value_get_timestamp_ms(&fieldValue, &timestamp_ms), MonadSuccess);
    ASSERT_EQ(timestamp_ms.value, 1297442662123);
    monad_data_type_destroy(&fieldType);

    ASSERT_EQ(monad_value_get_struct_field_value(&value, 6, &fieldValue), MonadSuccess);
    monad_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(monad_data_type_get_id(&fieldType), MONAD_TIMESTAMP_SEC);
    monad_timestamp_sec_t timestamp_sec;
    ASSERT_EQ(monad_value_get_timestamp_sec(&fieldValue, &timestamp_sec), MonadSuccess);
    ASSERT_EQ(timestamp_sec.value, 1297442662);
    monad_data_type_destroy(&fieldType);

    ASSERT_EQ(monad_value_get_struct_field_value(&value, 7, &fieldValue), MonadSuccess);
    monad_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(monad_data_type_get_id(&fieldType), MONAD_TIMESTAMP_TZ);
    monad_timestamp_tz_t timestamp_tz;
    ASSERT_EQ(monad_value_get_timestamp_tz(&fieldValue, &timestamp_tz), MonadSuccess);
    ASSERT_EQ(timestamp_tz.value, 1297442662123456);
    monad_data_type_destroy(&fieldType);

    ASSERT_EQ(monad_value_get_struct_field_value(&value, 8, &fieldValue), MonadSuccess);
    monad_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(monad_data_type_get_id(&fieldType), MONAD_DATE);
    monad_date_t date;
    ASSERT_EQ(monad_value_get_date(&fieldValue, &date), MonadSuccess);
    ASSERT_EQ(date.days, 15758);
    monad_data_type_destroy(&fieldType);
    monad_value_destroy(&fieldValue);

    ASSERT_EQ(monad_value_get_struct_field_value(&value, 222, &fieldValue), MonadError);

    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);
}

TEST_F(CApiValueTest, getMapNumFields) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (m:movies) WHERE m.length = 2544 RETURN m.audience", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_FALSE(monad_query_result_has_next(&result));
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);

    uint64_t mapFields;
    ASSERT_EQ(monad_value_get_map_size(&value, &mapFields), MonadSuccess);
    ASSERT_EQ(mapFields, 1);

    monad_query_result_destroy(&result);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
}

TEST_F(CApiValueTest, getMapKey) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (m:movies) WHERE m.length = 2544 RETURN m.audience", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_FALSE(monad_query_result_has_next(&result));
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);

    monad_value key;
    ASSERT_EQ(monad_value_get_map_key(&value, 0, &key), MonadSuccess);
    monad_logical_type keyType;
    monad_value_get_data_type(&key, &keyType);
    ASSERT_EQ(monad_data_type_get_id(&keyType), MONAD_STRING);
    char* mapName;
    ASSERT_EQ(monad_value_get_string(&key, &mapName), MonadSuccess);
    ASSERT_STREQ(mapName, "audience1");
    monad_destroy_string(mapName);
    monad_data_type_destroy(&keyType);
    monad_value_destroy(&key);

    ASSERT_EQ(monad_value_get_map_key(&value, 1, &key), MonadError);
    monad_query_result_destroy(&result);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
}

TEST_F(CApiValueTest, getMapValue) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (m:movies) WHERE m.length = 2544 RETURN m.audience", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_FALSE(monad_query_result_has_next(&result));
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);

    monad_value mapValue;
    ASSERT_EQ(monad_value_get_map_value(&value, 0, &mapValue), MonadSuccess);
    monad_logical_type mapType;
    monad_value_get_data_type(&mapValue, &mapType);
    ASSERT_EQ(monad_data_type_get_id(&mapType), MONAD_INT64);
    int64_t mapIntValue;
    ASSERT_EQ(monad_value_get_int64(&mapValue, &mapIntValue), MonadSuccess);
    ASSERT_EQ(mapIntValue, 33);

    ASSERT_EQ(monad_value_get_map_value(&value, 1, &mapValue), MonadError);

    monad_data_type_destroy(&mapType);
    monad_query_result_destroy(&result);
    monad_value_destroy(&mapValue);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
}

TEST_F(CApiValueTest, getDecimalAsString) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"UNWIND [1] AS A UNWIND [5.7, 8.3, 8.7, 13.7] AS B WITH cast(CAST(A AS DECIMAL) "
               "* "
               "CAST(B AS DECIMAL) AS DECIMAL(18, 1)) AS PROD RETURN COLLECT(PROD) AS RES",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);

    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);

    monad_logical_type dataType;
    monad_value_get_data_type(&value, &dataType);
    ASSERT_EQ(monad_data_type_get_id(&dataType), MONAD_LIST);
    uint64_t list_size;
    ASSERT_EQ(monad_value_get_list_size(&value, &list_size), MonadSuccess);
    ASSERT_EQ(list_size, 4);
    monad_data_type_destroy(&dataType);

    monad_value decimal_entry;
    char* decimal_value;
    std::string decimal_string_value;
    ASSERT_EQ(monad_value_get_list_element(&value, 0, &decimal_entry), MonadSuccess);
    monad_value_get_data_type(&decimal_entry, &dataType);
    ASSERT_EQ(monad_data_type_get_id(&dataType), MONAD_DECIMAL);
    ASSERT_EQ(monad_value_get_decimal_as_string(&decimal_entry, &decimal_value), MonadSuccess);
    decimal_string_value = std::string(decimal_value);
    ASSERT_EQ(decimal_string_value, "5.7");
    monad_destroy_string(decimal_value);
    monad_data_type_destroy(&dataType);

    ASSERT_EQ(monad_value_get_list_element(&value, 1, &decimal_entry), MonadSuccess);
    monad_value_get_data_type(&decimal_entry, &dataType);
    ASSERT_EQ(monad_data_type_get_id(&dataType), MONAD_DECIMAL);
    ASSERT_EQ(monad_value_get_decimal_as_string(&decimal_entry, &decimal_value), MonadSuccess);
    decimal_string_value = std::string(decimal_value);
    ASSERT_EQ(decimal_string_value, "8.3");
    monad_destroy_string(decimal_value);
    monad_data_type_destroy(&dataType);

    ASSERT_EQ(monad_value_get_list_element(&value, 2, &decimal_entry), MonadSuccess);
    monad_value_get_data_type(&decimal_entry, &dataType);
    ASSERT_EQ(monad_data_type_get_id(&dataType), MONAD_DECIMAL);
    ASSERT_EQ(monad_value_get_decimal_as_string(&decimal_entry, &decimal_value), MonadSuccess);
    decimal_string_value = std::string(decimal_value);
    ASSERT_EQ(decimal_string_value, "8.7");
    monad_destroy_string(decimal_value);
    monad_data_type_destroy(&dataType);

    ASSERT_EQ(monad_value_get_list_element(&value, 3, &decimal_entry), MonadSuccess);
    monad_value_get_data_type(&decimal_entry, &dataType);
    ASSERT_EQ(monad_data_type_get_id(&dataType), MONAD_DECIMAL);
    ASSERT_EQ(monad_value_get_decimal_as_string(&decimal_entry, &decimal_value), MonadSuccess);
    decimal_string_value = std::string(decimal_value);
    ASSERT_EQ(decimal_string_value, "13.7");
    monad_destroy_string(decimal_value);
    monad_data_type_destroy(&dataType);

    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);
    monad_value_destroy(&decimal_entry);
}

TEST_F(CApiValueTest, GetDataType) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.fName, a.isStudent, a.workedHours", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    monad_logical_type dataType;
    monad_value_get_data_type(&value, &dataType);
    ASSERT_EQ(monad_data_type_get_id(&dataType), MONAD_STRING);
    monad_data_type_destroy(&dataType);

    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 1, &value), MonadSuccess);
    monad_value_get_data_type(&value, &dataType);
    ASSERT_EQ(monad_data_type_get_id(&dataType), MONAD_BOOL);
    monad_data_type_destroy(&dataType);

    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 2, &value), MonadSuccess);
    monad_value_get_data_type(&value, &dataType);
    ASSERT_EQ(monad_data_type_get_id(&dataType), MONAD_LIST);
    monad_data_type_destroy(&dataType);
    monad_value_destroy(&value);

    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);
}

TEST_F(CApiValueTest, GetBool) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.isStudent ORDER BY a.ID", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    bool boolValue;
    ASSERT_EQ(monad_value_get_bool(&value, &boolValue), MonadSuccess);
    ASSERT_TRUE(boolValue);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_bool(badValue, &boolValue), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetInt8) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.level ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    int8_t int8Value;
    ASSERT_EQ(monad_value_get_int8(&value, &int8Value), MonadSuccess);
    ASSERT_EQ(int8Value, 5);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_int8(badValue, &int8Value), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetInt16) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.length ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    int16_t int16Value;
    ASSERT_EQ(monad_value_get_int16(&value, &int16Value), MonadSuccess);
    ASSERT_EQ(int16Value, 5);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_int16(badValue, &int16Value), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetInt32) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (m:movies) RETURN m.length ORDER BY m.name", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    int32_t int32Value;
    ASSERT_EQ(monad_value_get_int32(&value, &int32Value), MonadSuccess);
    ASSERT_EQ(int32Value, 298);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_int32(badValue, &int32Value), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetInt64) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection, (char*)"MATCH (a:person) RETURN a.ID ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    int64_t int64Value;
    ASSERT_EQ(monad_value_get_int64(&value, &int64Value), MonadSuccess);
    ASSERT_EQ(int64Value, 0);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_int64(badValue, &int64Value), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetUInt8) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.ulevel ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    uint8_t uint8Value;
    ASSERT_EQ(monad_value_get_uint8(&value, &uint8Value), MonadSuccess);
    ASSERT_EQ(uint8Value, 250);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_uint8(badValue, &uint8Value), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetUInt16) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.ulength ORDER BY "
               "a.ID",
        &result);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    uint16_t uint16Value;
    ASSERT_EQ(monad_value_get_uint16(&value, &uint16Value), MonadSuccess);
    ASSERT_EQ(uint16Value, 33768);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_uint16(badValue, &uint16Value), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetUInt32) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) "
               "RETURN r.temperature ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    uint32_t uint32Value;
    ASSERT_EQ(monad_value_get_uint32(&value, &uint32Value), MonadSuccess);
    ASSERT_EQ(uint32Value, 32800);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_uint32(badValue, &uint32Value), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetUInt64) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.code ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    uint64_t uint64Value;
    ASSERT_EQ(monad_value_get_uint64(&value, &uint64Value), MonadSuccess);
    ASSERT_EQ(uint64Value, 9223372036854775808ull);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_uint64(badValue, &uint64Value), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetInt128) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.hugedata ORDER BY "
               "a.ID",
        &result);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    monad_int128_t int128;
    ASSERT_EQ(monad_value_get_int128(&value, &int128), MonadSuccess);
    ASSERT_EQ(int128.high, 100000000);
    ASSERT_EQ(int128.low, 211111111);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_int128(badValue, &int128), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, StringToInt128Test) {
    char input[] = "1844674407370955161811111111";
    monad_int128_t int128_val;
    ASSERT_EQ(monad_int128_t_from_string(input, &int128_val), MonadSuccess);
    ASSERT_EQ(int128_val.high, 100000000);
    ASSERT_EQ(int128_val.low, 211111111);

    char badInput[] = "this is not a int128";
    monad_int128_t int128_val2;
    ASSERT_EQ(monad_int128_t_from_string(badInput, &int128_val2), MonadError);
}

TEST_F(CApiValueTest, Int128ToStringTest) {
    auto int128_val = monad_int128_t{211111111, 100000000};
    char* str;
    ASSERT_EQ(monad_int128_t_to_string(int128_val, &str), MonadSuccess);
    ASSERT_STREQ(str, "1844674407370955161811111111");
    monad_destroy_string(str);
}

TEST_F(CApiValueTest, GetFloat) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.height ORDER BY a.ID", &result);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    float floatValue;
    ASSERT_EQ(monad_value_get_float(&value, &floatValue), MonadSuccess);
    ASSERT_FLOAT_EQ(floatValue, 1.731);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_float(badValue, &floatValue), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetDouble) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.eyeSight ORDER BY a.ID", &result);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    double doubleValue;
    ASSERT_EQ(monad_value_get_double(&value, &doubleValue), MonadSuccess);
    ASSERT_DOUBLE_EQ(doubleValue, 5.0);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_double(badValue, &doubleValue), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetInternalID) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID",
        &result);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    monad_value nodeIDVal;
    ASSERT_EQ(monad_value_get_struct_field_value(&value, 0 /* internal ID field idx */, &nodeIDVal),
        MonadSuccess);
    monad_internal_id_t internalID;
    ASSERT_EQ(monad_value_get_internal_id(&nodeIDVal, &internalID), MonadSuccess);
    ASSERT_EQ(internalID.table_id, 0);
    ASSERT_EQ(internalID.offset, 0);
    monad_value_destroy(&nodeIDVal);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_internal_id(badValue, &internalID), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetRelVal) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) -[r:knows]-> (b:person) RETURN r ORDER BY a.ID, b.ID", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value rel;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &rel), MonadSuccess);
    ASSERT_TRUE(rel._is_owned_by_cpp);
    monad_value relIdVal;
    ASSERT_EQ(monad_rel_val_get_id_val(&rel, &relIdVal), MonadSuccess);
    monad_internal_id_t relInternalID;
    ASSERT_EQ(monad_value_get_internal_id(&relIdVal, &relInternalID), MonadSuccess);
    ASSERT_EQ(relInternalID.table_id, 3);
    ASSERT_EQ(relInternalID.offset, 0);
    monad_value relSrcIDVal;
    ASSERT_EQ(monad_rel_val_get_src_id_val(&rel, &relSrcIDVal), MonadSuccess);
    monad_internal_id_t relSrcID;
    ASSERT_EQ(monad_value_get_internal_id(&relSrcIDVal, &relSrcID), MonadSuccess);
    ASSERT_EQ(relSrcID.table_id, 0);
    ASSERT_EQ(relSrcID.offset, 0);
    monad_value relDstIDVal;
    ASSERT_EQ(monad_rel_val_get_dst_id_val(&rel, &relDstIDVal), MonadSuccess);
    monad_internal_id_t relDstID;
    ASSERT_EQ(monad_value_get_internal_id(&relDstIDVal, &relDstID), MonadSuccess);
    ASSERT_EQ(relDstID.table_id, 0);
    ASSERT_EQ(relDstID.offset, 1);
    monad_value relLabel;
    ASSERT_EQ(monad_rel_val_get_label_val(&rel, &relLabel), MonadSuccess);
    char* relLabelStr;
    ASSERT_EQ(monad_value_get_string(&relLabel, &relLabelStr), MonadSuccess);
    ASSERT_STREQ(relLabelStr, "knows");
    uint64_t propertiesSize;
    ASSERT_EQ(monad_rel_val_get_property_size(&rel, &propertiesSize), MonadSuccess);
    ASSERT_EQ(propertiesSize, 7);
    monad_destroy_string(relLabelStr);
    monad_value_destroy(&relLabel);
    monad_value_destroy(&relIdVal);
    monad_value_destroy(&relSrcIDVal);
    monad_value_destroy(&relDstIDVal);
    monad_value_destroy(&rel);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_rel_val_get_src_id_val(badValue, &relSrcIDVal), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetDate) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.birthdate ORDER BY a.ID", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    monad_date_t date;
    ASSERT_EQ(monad_value_get_date(&value, &date), MonadSuccess);
    ASSERT_EQ(date.days, -25567);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_date(badValue, &date), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetTimestamp) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.registerTime ORDER BY a.ID", &result);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    monad_timestamp_t timestamp;
    ASSERT_EQ(monad_value_get_timestamp(&value, &timestamp), MonadSuccess);
    ASSERT_EQ(timestamp.value, 1313839530000000);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_timestamp(badValue, &timestamp), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetInterval) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.lastJobDuration ORDER BY a.ID", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    monad_interval_t interval;
    ASSERT_EQ(monad_value_get_interval(&value, &interval), MonadSuccess);
    ASSERT_EQ(interval.months, 36);
    ASSERT_EQ(interval.days, 2);
    ASSERT_EQ(interval.micros, 46920000000);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_interval(badValue, &interval), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetString) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.fName ORDER BY a.ID", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    char* str;
    ASSERT_EQ(monad_value_get_string(&value, &str), MonadSuccess);
    ASSERT_STREQ(str, "Alice");
    monad_destroy_string(str);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_int32(123);
    ASSERT_EQ(monad_value_get_string(badValue, &str), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetBlob) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state =
        monad_connection_query(connection, (char*)R"(RETURN BLOB('\xAA\xBB\xCD\x1A');)", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    uint8_t* blob;
    uint64_t length;
    ASSERT_EQ(monad_value_get_blob(&value, &blob, &length), MonadSuccess);
    ASSERT_EQ(length, 4);
    ASSERT_EQ(blob[0], 0xAA);
    ASSERT_EQ(blob[1], 0xBB);
    ASSERT_EQ(blob[2], 0xCD);
    ASSERT_EQ(blob[3], 0x1A);
    monad_destroy_blob(blob);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_blob(badValue, &blob, &length), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetUUID) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)R"(RETURN UUID("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11");)", &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value value;
    monad_flat_tuple_get_value(&flatTuple, 0, &value);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(monad_value_is_null(&value));
    char* str;
    ASSERT_EQ(monad_value_get_uuid(&value, &str), MonadSuccess);
    ASSERT_STREQ(str, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    monad_destroy_string(str);
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_value_get_uuid(badValue, &str), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, ToSting) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.fName, a.isStudent, a.workedHours ORDER BY "
               "a.ID",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));

    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);

    monad_value value;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &value), MonadSuccess);
    char* str = monad_value_to_string(&value);
    ASSERT_STREQ(str, "Alice");
    monad_destroy_string(str);

    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 1, &value), MonadSuccess);
    str = monad_value_to_string(&value);
    ASSERT_STREQ(str, "True");
    monad_destroy_string(str);

    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 2, &value), MonadSuccess);
    str = monad_value_to_string(&value);
    ASSERT_STREQ(str, "[10,5]");
    monad_destroy_string(str);
    monad_value_destroy(&value);

    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);
}

TEST_F(CApiValueTest, NodeValGetLabelVal) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));

    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value nodeVal;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &nodeVal), MonadSuccess);
    monad_value labelVal;
    ASSERT_EQ(monad_node_val_get_label_val(&nodeVal, &labelVal), MonadSuccess);
    char* labelStr;
    ASSERT_EQ(monad_value_get_string(&labelVal, &labelStr), MonadSuccess);
    ASSERT_STREQ(labelStr, "person");
    monad_destroy_string(labelStr);
    monad_value_destroy(&labelVal);
    monad_value_destroy(&nodeVal);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_node_val_get_label_val(badValue, &labelVal), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, NodeValGetID) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));

    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value nodeVal;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &nodeVal), MonadSuccess);
    monad_value nodeIDVal;
    ASSERT_EQ(monad_node_val_get_id_val(&nodeVal, &nodeIDVal), MonadSuccess);
    ASSERT_NE(nodeIDVal._value, nullptr);
    monad_internal_id_t internalID;
    ASSERT_EQ(monad_value_get_internal_id(&nodeIDVal, &internalID), MonadSuccess);
    ASSERT_EQ(internalID.table_id, 0);
    ASSERT_EQ(internalID.offset, 0);
    monad_value_destroy(&nodeIDVal);
    monad_value_destroy(&nodeVal);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_node_val_get_id_val(badValue, &nodeIDVal), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, NodeValGetLabelName) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));

    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value nodeVal;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &nodeVal), MonadSuccess);
    monad_value labelVal;
    ASSERT_EQ(monad_node_val_get_label_val(&nodeVal, &labelVal), MonadSuccess);
    char* labelStr;
    ASSERT_EQ(monad_value_get_string(&labelVal, &labelStr), MonadSuccess);
    ASSERT_STREQ(labelStr, "person");
    monad_destroy_string(labelStr);
    monad_value_destroy(&labelVal);
    monad_value_destroy(&nodeVal);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_node_val_get_label_val(badValue, &labelVal), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, NodeValGetProperty) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value node;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &node), MonadSuccess);
    char* propertyName;
    ASSERT_EQ(monad_node_val_get_property_name_at(&node, 0, &propertyName), MonadSuccess);
    ASSERT_STREQ(propertyName, "ID");
    monad_destroy_string(propertyName);
    ASSERT_EQ(monad_node_val_get_property_name_at(&node, 1, &propertyName), MonadSuccess);
    ASSERT_STREQ(propertyName, "fName");
    monad_destroy_string(propertyName);
    ASSERT_EQ(monad_node_val_get_property_name_at(&node, 2, &propertyName), MonadSuccess);
    ASSERT_STREQ(propertyName, "gender");
    monad_destroy_string(propertyName);
    ASSERT_EQ(monad_node_val_get_property_name_at(&node, 3, &propertyName), MonadSuccess);
    ASSERT_STREQ(propertyName, "isStudent");
    monad_destroy_string(propertyName);

    monad_value propertyValue;
    ASSERT_EQ(monad_node_val_get_property_value_at(&node, 0, &propertyValue), MonadSuccess);
    int64_t propertyValueID;
    ASSERT_EQ(monad_value_get_int64(&propertyValue, &propertyValueID), MonadSuccess);
    ASSERT_EQ(propertyValueID, 0);
    ASSERT_EQ(monad_node_val_get_property_value_at(&node, 1, &propertyValue), MonadSuccess);
    char* propertyValuefName;
    ASSERT_EQ(monad_value_get_string(&propertyValue, &propertyValuefName), MonadSuccess);
    ASSERT_STREQ(propertyValuefName, "Alice");
    monad_destroy_string(propertyValuefName);
    ASSERT_EQ(monad_node_val_get_property_value_at(&node, 2, &propertyValue), MonadSuccess);
    int64_t propertyValueGender;
    ASSERT_EQ(monad_value_get_int64(&propertyValue, &propertyValueGender), MonadSuccess);
    ASSERT_EQ(propertyValueGender, 1);
    ASSERT_EQ(monad_node_val_get_property_value_at(&node, 3, &propertyValue), MonadSuccess);
    bool propertyValueIsStudent;
    ASSERT_EQ(monad_value_get_bool(&propertyValue, &propertyValueIsStudent), MonadSuccess);
    ASSERT_EQ(propertyValueIsStudent, true);
    monad_value_destroy(&propertyValue);

    monad_value_destroy(&node);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_node_val_get_property_name_at(badValue, 0, &propertyName), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, NodeValToString) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (b:organisation) RETURN b ORDER BY b.ID", &result);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value node;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &node), MonadSuccess);
    ASSERT_TRUE(node._is_owned_by_cpp);

    char* str = monad_value_to_string(&node);
    ASSERT_STREQ(str,
        "{_ID: 1:0, _LABEL: organisation, ID: 1, name: ABFsUni, orgCode: 325, mark: 3.700000, "
        "score: -2, history: 10 years 5 months 13 hours 24 us, licenseValidInterval: 3 years "
        "5 days, rating: 1.000000, state: {revenue: 138, location: ['toronto','montr,eal'], "
        "stock: {price: [96,56], volume: 1000}}, info: 3.120000}");
    monad_destroy_string(str);

    monad_value_destroy(&node);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);
}

TEST_F(CApiValueTest, RelValGetProperty) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) -[e:workAt]-> (b:organisation) RETURN e ORDER BY a.ID, b.ID",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value rel;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &rel), MonadSuccess);
    ASSERT_TRUE(rel._is_owned_by_cpp);
    uint64_t propertiesSize;
    ASSERT_EQ(monad_rel_val_get_property_size(&rel, &propertiesSize), MonadSuccess);
    ASSERT_EQ(propertiesSize, 3);

    char* propertyName;
    ASSERT_EQ(monad_rel_val_get_property_name_at(&rel, 0, &propertyName), MonadSuccess);
    ASSERT_STREQ(propertyName, "year");
    monad_destroy_string(propertyName);

    ASSERT_EQ(monad_rel_val_get_property_name_at(&rel, 1, &propertyName), MonadSuccess);
    ASSERT_STREQ(propertyName, "grading");
    monad_destroy_string(propertyName);
    ASSERT_EQ(monad_rel_val_get_property_name_at(&rel, 2, &propertyName), MonadSuccess);
    ASSERT_STREQ(propertyName, "rating");
    monad_destroy_string(propertyName);

    monad_value propertyValue;
    ASSERT_EQ(monad_rel_val_get_property_value_at(&rel, 0, &propertyValue), MonadSuccess);
    int64_t propertyValueYear;
    ASSERT_EQ(monad_value_get_int64(&propertyValue, &propertyValueYear), MonadSuccess);
    ASSERT_EQ(propertyValueYear, 2015);

    ASSERT_EQ(monad_rel_val_get_property_value_at(&rel, 1, &propertyValue), MonadSuccess);
    monad_value listValue;
    ASSERT_EQ(monad_value_get_list_element(&propertyValue, 0, &listValue), MonadSuccess);
    double listValueGrading;
    ASSERT_EQ(monad_value_get_double(&listValue, &listValueGrading), MonadSuccess);
    ASSERT_DOUBLE_EQ(listValueGrading, 3.8);
    ASSERT_EQ(monad_value_get_list_element(&propertyValue, 1, &listValue), MonadSuccess);
    ASSERT_EQ(monad_value_get_double(&listValue, &listValueGrading), MonadSuccess);
    ASSERT_DOUBLE_EQ(listValueGrading, 2.5);
    monad_value_destroy(&listValue);

    ASSERT_EQ(monad_rel_val_get_property_value_at(&rel, 2, &propertyValue), MonadSuccess);
    float propertyValueRating;
    ASSERT_EQ(monad_value_get_float(&propertyValue, &propertyValueRating), MonadSuccess);
    ASSERT_FLOAT_EQ(propertyValueRating, 8.2);
    monad_value_destroy(&propertyValue);

    monad_value_destroy(&rel);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_rel_val_get_property_name_at(badValue, 0, &propertyName), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, RelValToString) {
    monad_query_result result;
    monad_flat_tuple flatTuple;
    monad_state state;
    auto connection = getConnection();
    state = monad_connection_query(connection,
        (char*)"MATCH (a:person) -[e:workAt]-> (b:organisation) RETURN e ORDER BY a.ID, b.ID",
        &result);
    ASSERT_EQ(state, MonadSuccess);
    ASSERT_TRUE(monad_query_result_is_success(&result));
    ASSERT_TRUE(monad_query_result_has_next(&result));
    state = monad_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, MonadSuccess);
    monad_value rel;
    ASSERT_EQ(monad_flat_tuple_get_value(&flatTuple, 0, &rel), MonadSuccess);
    ASSERT_TRUE(rel._is_owned_by_cpp);
    char* str;
    ASSERT_EQ(monad_rel_val_to_string(&rel, &str), MonadSuccess);
    ASSERT_STREQ(str, "(0:2)-{_LABEL: workAt, _ID: 7:0, year: 2015, grading: [3.800000,2.500000], "
                      "rating: 8.200000}->(1:1)");
    monad_destroy_string(str);
    monad_value_destroy(&rel);
    monad_flat_tuple_destroy(&flatTuple);
    monad_query_result_destroy(&result);

    monad_value* badValue = monad_value_create_string((char*)"abcdefg");
    ASSERT_EQ(monad_rel_val_to_string(badValue, &str), MonadError);
    monad_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetTmFromNonStandardTimestamp) {
    monad_timestamp_ns_t timestamp_ns = monad_timestamp_ns_t{17515323532900000};
    monad_timestamp_ms_t timestamp_ms = monad_timestamp_ms_t{1012323435341};
    monad_timestamp_sec_t timestamp_sec = monad_timestamp_sec_t{1432135648};
    monad_timestamp_tz_t timestamp_tz = monad_timestamp_tz_t{771513532900000};
    struct tm tm;
    ASSERT_EQ(monad_timestamp_ns_to_tm(timestamp_ns, &tm), MonadSuccess);
    ASSERT_EQ(tm.tm_year, 70);
    ASSERT_EQ(tm.tm_mon, 6);
    ASSERT_EQ(tm.tm_mday, 22);
    ASSERT_EQ(tm.tm_hour, 17);
    ASSERT_EQ(tm.tm_min, 22);
    ASSERT_EQ(tm.tm_sec, 3);
    ASSERT_EQ(monad_timestamp_ms_to_tm(timestamp_ms, &tm), MonadSuccess);
    ASSERT_EQ(tm.tm_year, 102);
    ASSERT_EQ(tm.tm_mon, 0);
    ASSERT_EQ(tm.tm_mday, 29);
    ASSERT_EQ(tm.tm_hour, 16);
    ASSERT_EQ(tm.tm_min, 57);
    ASSERT_EQ(tm.tm_sec, 15);
    ASSERT_EQ(monad_timestamp_sec_to_tm(timestamp_sec, &tm), MonadSuccess);
    ASSERT_EQ(tm.tm_year, 115);
    ASSERT_EQ(tm.tm_mon, 4);
    ASSERT_EQ(tm.tm_mday, 20);
    ASSERT_EQ(tm.tm_hour, 15);
    ASSERT_EQ(tm.tm_min, 27);
    ASSERT_EQ(tm.tm_sec, 28);
    ASSERT_EQ(monad_timestamp_tz_to_tm(timestamp_tz, &tm), MonadSuccess);
    ASSERT_EQ(tm.tm_year, 94);
    ASSERT_EQ(tm.tm_mon, 5);
    ASSERT_EQ(tm.tm_mday, 13);
    ASSERT_EQ(tm.tm_hour, 13);
    ASSERT_EQ(tm.tm_min, 18);
    ASSERT_EQ(tm.tm_sec, 52);
}

TEST_F(CApiValueTest, GetTmFromTimestamp) {
    monad_timestamp_t timestamp = monad_timestamp_t{171513532900000};
    struct tm tm;
    ASSERT_EQ(monad_timestamp_to_tm(timestamp, &tm), MonadSuccess);
    ASSERT_EQ(tm.tm_year, 75);
    ASSERT_EQ(tm.tm_mon, 5);
    ASSERT_EQ(tm.tm_mday, 9);
    ASSERT_EQ(tm.tm_hour, 2);
    ASSERT_EQ(tm.tm_min, 38);
    ASSERT_EQ(tm.tm_sec, 52);
}

TEST_F(CApiValueTest, GetTmFromDate) {
    monad_date_t date = monad_date_t{-255};
    struct tm tm;
    ASSERT_EQ(monad_date_to_tm(date, &tm), MonadSuccess);
    ASSERT_EQ(tm.tm_year, 69);
    ASSERT_EQ(tm.tm_mon, 3);
    ASSERT_EQ(tm.tm_mday, 21);
    ASSERT_EQ(tm.tm_hour, 0);
    ASSERT_EQ(tm.tm_min, 0);
    ASSERT_EQ(tm.tm_sec, 0);
}

TEST_F(CApiValueTest, GetTimestampFromTm) {
    struct tm tm;
    tm.tm_year = 75;
    tm.tm_mon = 5;
    tm.tm_mday = 9;
    tm.tm_hour = 2;
    tm.tm_min = 38;
    tm.tm_sec = 52;
    monad_timestamp_t timestamp;
    ASSERT_EQ(monad_timestamp_from_tm(tm, &timestamp), MonadSuccess);
    ASSERT_EQ(timestamp.value, 171513532000000);
}

TEST_F(CApiValueTest, GetNonStandardTimestampFromTm) {
    struct tm tm;
    tm.tm_year = 70;
    tm.tm_mon = 6;
    tm.tm_mday = 22;
    tm.tm_hour = 17;
    tm.tm_min = 22;
    tm.tm_sec = 3;
    monad_timestamp_ns_t timestamp_ns;
    ASSERT_EQ(monad_timestamp_ns_from_tm(tm, &timestamp_ns), MonadSuccess);
    ASSERT_EQ(timestamp_ns.value, 17515323000000000);
    tm.tm_year = 102;
    tm.tm_mon = 0;
    tm.tm_mday = 29;
    tm.tm_hour = 16;
    tm.tm_min = 57;
    tm.tm_sec = 15;
    monad_timestamp_ms_t timestamp_ms;
    ASSERT_EQ(monad_timestamp_ms_from_tm(tm, &timestamp_ms), MonadSuccess);
    ASSERT_EQ(timestamp_ms.value, 1012323435000);
    tm.tm_year = 115;
    tm.tm_mon = 4;
    tm.tm_mday = 20;
    tm.tm_hour = 15;
    tm.tm_min = 27;
    tm.tm_sec = 28;
    monad_timestamp_sec_t timestamp_sec;
    ASSERT_EQ(monad_timestamp_sec_from_tm(tm, &timestamp_sec), MonadSuccess);
    ASSERT_EQ(timestamp_sec.value, 1432135648);
    tm.tm_year = 94;
    tm.tm_mon = 5;
    tm.tm_mday = 13;
    tm.tm_hour = 13;
    tm.tm_min = 18;
    tm.tm_sec = 52;
    monad_timestamp_tz_t timestamp_tz;
    ASSERT_EQ(monad_timestamp_tz_from_tm(tm, &timestamp_tz), MonadSuccess);
    ASSERT_EQ(timestamp_tz.value, 771513532000000);
}

TEST_F(CApiValueTest, GetDateFromTm) {
    struct tm tm;
    tm.tm_year = 69;
    tm.tm_mon = 3;
    tm.tm_mday = 21;
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
    monad_date_t date;
    ASSERT_EQ(monad_date_from_tm(tm, &date), MonadSuccess);
    ASSERT_EQ(date.days, -255);
}

TEST_F(CApiValueTest, GetDateFromString) {
    char input[] = "1969-04-21";
    monad_date_t date;
    ASSERT_EQ(monad_date_from_string(input, &date), MonadSuccess);
    ASSERT_EQ(date.days, -255);

    char badInput[] = "this is not a date";
    ASSERT_EQ(monad_date_from_string(badInput, &date), MonadError);
}

TEST_F(CApiValueTest, GetStringFromDate) {
    monad_date_t date = monad_date_t{-255};
    char* str;
    ASSERT_EQ(monad_date_to_string(date, &str), MonadSuccess);
    ASSERT_STREQ(str, "1969-04-21");
    monad_destroy_string(str);
}

TEST_F(CApiValueTest, GetDifftimeFromInterval) {
    monad_interval_t interval = monad_interval_t{36, 2, 46920000000};
    double difftime;
    monad_interval_to_difftime(interval, &difftime);
    ASSERT_DOUBLE_EQ(difftime, 93531720);
}

TEST_F(CApiValueTest, GetIntervalFromDifftime) {
    double difftime = 211110160.479;
    monad_interval_t interval;
    monad_interval_from_difftime(difftime, &interval);
    ASSERT_EQ(interval.months, 81);
    ASSERT_EQ(interval.days, 13);
    ASSERT_EQ(interval.micros, 34960479000);
}
