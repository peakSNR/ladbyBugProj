#include "main/prepared_statement.h"

#include "c_api/helpers.h"
#include "c_api/monad.h"
#include "common/types/value/value.h"

using namespace monad::common;
using namespace monad::main;

void monad_prepared_statement_bind_cpp_value(monad_prepared_statement* prepared_statement,
    const char* param_name, std::unique_ptr<Value> value) {
    auto* bound_values = static_cast<std::unordered_map<std::string, std::unique_ptr<Value>>*>(
        prepared_statement->_bound_values);
    bound_values->erase(param_name);
    bound_values->insert({param_name, std::move(value)});
}

void monad_prepared_statement_destroy(monad_prepared_statement* prepared_statement) {
    if (prepared_statement == nullptr) {
        return;
    }
    if (prepared_statement->_prepared_statement != nullptr) {
        delete static_cast<PreparedStatement*>(prepared_statement->_prepared_statement);
    }
    if (prepared_statement->_bound_values != nullptr) {
        delete static_cast<std::unordered_map<std::string, std::unique_ptr<Value>>*>(
            prepared_statement->_bound_values);
    }
}

bool monad_prepared_statement_is_success(monad_prepared_statement* prepared_statement) {
    return static_cast<PreparedStatement*>(prepared_statement->_prepared_statement)->isSuccess();
}

char* monad_prepared_statement_get_error_message(monad_prepared_statement* prepared_statement) {
    auto error_message =
        static_cast<PreparedStatement*>(prepared_statement->_prepared_statement)->getErrorMessage();
    if (error_message.empty()) {
        return nullptr;
    }
    return convertToOwnedCString(error_message);
}

monad_state monad_prepared_statement_bind_bool(monad_prepared_statement* prepared_statement,
    const char* param_name, bool value) {
    try {
        auto value_ptr = std::make_unique<Value>(value);
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_int64(monad_prepared_statement* prepared_statement,
    const char* param_name, int64_t value) {
    try {
        auto value_ptr = std::make_unique<Value>(value);
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_int32(monad_prepared_statement* prepared_statement,
    const char* param_name, int32_t value) {
    try {
        auto value_ptr = std::make_unique<Value>(value);
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_int16(monad_prepared_statement* prepared_statement,
    const char* param_name, int16_t value) {
    try {
        auto value_ptr = std::make_unique<Value>(value);
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_int8(monad_prepared_statement* prepared_statement,
    const char* param_name, int8_t value) {
    try {
        auto value_ptr = std::make_unique<Value>(value);
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_uint64(monad_prepared_statement* prepared_statement,
    const char* param_name, uint64_t value) {
    try {
        auto value_ptr = std::make_unique<Value>(value);
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_uint32(monad_prepared_statement* prepared_statement,
    const char* param_name, uint32_t value) {
    try {
        auto value_ptr = std::make_unique<Value>(value);
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_uint16(monad_prepared_statement* prepared_statement,
    const char* param_name, uint16_t value) {
    try {
        auto value_ptr = std::make_unique<Value>(value);
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_uint8(monad_prepared_statement* prepared_statement,
    const char* param_name, uint8_t value) {
    try {
        auto value_ptr = std::make_unique<Value>(value);
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_double(monad_prepared_statement* prepared_statement,
    const char* param_name, double value) {
    try {
        auto value_ptr = std::make_unique<Value>(value);
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_float(monad_prepared_statement* prepared_statement,
    const char* param_name, float value) {
    try {
        auto value_ptr = std::make_unique<Value>(value);
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_date(monad_prepared_statement* prepared_statement,
    const char* param_name, monad_date_t value) {
    try {
        auto value_ptr = std::make_unique<Value>(date_t(value.days));
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_timestamp_ns(monad_prepared_statement* prepared_statement,
    const char* param_name, monad_timestamp_ns_t value) {
    try {
        auto value_ptr = std::make_unique<Value>(timestamp_ns_t(value.value));
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_timestamp_ms(monad_prepared_statement* prepared_statement,
    const char* param_name, monad_timestamp_ms_t value) {
    try {
        auto value_ptr = std::make_unique<Value>(timestamp_ms_t(value.value));
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_timestamp_sec(monad_prepared_statement* prepared_statement,
    const char* param_name, monad_timestamp_sec_t value) {
    try {
        auto value_ptr = std::make_unique<Value>(timestamp_sec_t(value.value));
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_timestamp_tz(monad_prepared_statement* prepared_statement,
    const char* param_name, monad_timestamp_tz_t value) {
    try {
        auto value_ptr = std::make_unique<Value>(timestamp_tz_t(value.value));
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_timestamp(monad_prepared_statement* prepared_statement,
    const char* param_name, monad_timestamp_t value) {
    try {
        auto value_ptr = std::make_unique<Value>(timestamp_t(value.value));
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_interval(monad_prepared_statement* prepared_statement,
    const char* param_name, monad_interval_t value) {
    try {
        auto value_ptr =
            std::make_unique<Value>(interval_t(value.months, value.days, value.micros));
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_string(monad_prepared_statement* prepared_statement,
    const char* param_name, const char* value) {
    try {
        auto value_ptr = std::make_unique<Value>(std::string(value));
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_prepared_statement_bind_value(monad_prepared_statement* prepared_statement,
    const char* param_name, monad_value* value) {
    try {
        auto value_ptr = std::make_unique<Value>(*static_cast<Value*>(value->_value));
        monad_prepared_statement_bind_cpp_value(prepared_statement, param_name,
            std::move(value_ptr));
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}
