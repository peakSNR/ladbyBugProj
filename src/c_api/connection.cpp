#include "c_api/monad.h"
#include "common/exception/exception.h"
#include "main/monad.h"

namespace monad {
namespace common {
class Value;
}
} // namespace monad

using namespace monad::common;
using namespace monad::main;

monad_state monad_connection_init(monad_database* database, monad_connection* out_connection) {
    if (database == nullptr || database->_database == nullptr) {
        out_connection->_connection = nullptr;
        return MonadError;
    }
    try {
        out_connection->_connection = new Connection(static_cast<Database*>(database->_database));
    } catch (Exception& e) {
        out_connection->_connection = nullptr;
        return MonadError;
    }
    return MonadSuccess;
}

void monad_connection_destroy(monad_connection* connection) {
    if (connection == nullptr) {
        return;
    }
    if (connection->_connection != nullptr) {
        delete static_cast<Connection*>(connection->_connection);
    }
}

monad_state monad_connection_set_max_num_thread_for_exec(monad_connection* connection,
    uint64_t num_threads) {
    if (connection == nullptr || connection->_connection == nullptr) {
        return MonadError;
    }
    try {
        static_cast<Connection*>(connection->_connection)->setMaxNumThreadForExec(num_threads);
    } catch (Exception& e) {
        return MonadError;
    }
    return MonadSuccess;
}

monad_state monad_connection_get_max_num_thread_for_exec(monad_connection* connection,
    uint64_t* out_result) {
    if (connection == nullptr || connection->_connection == nullptr) {
        return MonadError;
    }
    try {
        *out_result = static_cast<Connection*>(connection->_connection)->getMaxNumThreadForExec();
    } catch (Exception& e) {
        return MonadError;
    }
    return MonadSuccess;
}

monad_state monad_connection_query(monad_connection* connection, const char* query,
    monad_query_result* out_query_result) {
    if (connection == nullptr || connection->_connection == nullptr) {
        return MonadError;
    }
    try {
        auto query_result =
            static_cast<Connection*>(connection->_connection)->query(query).release();
        if (query_result == nullptr) {
            return MonadError;
        }
        out_query_result->_query_result = query_result;
        out_query_result->_is_owned_by_cpp = false;
        if (!query_result->isSuccess()) {
            return MonadError;
        }
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_connection_prepare(monad_connection* connection, const char* query,
    monad_prepared_statement* out_prepared_statement) {
    if (connection == nullptr || connection->_connection == nullptr) {
        return MonadError;
    }
    try {
        auto prepared_statement =
            static_cast<Connection*>(connection->_connection)->prepare(query).release();
        if (prepared_statement == nullptr) {
            return MonadError;
        }
        out_prepared_statement->_prepared_statement = prepared_statement;
        out_prepared_statement->_bound_values =
            new std::unordered_map<std::string, std::unique_ptr<Value>>;
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
    return MonadSuccess;
}

monad_state monad_connection_execute(monad_connection* connection,
    monad_prepared_statement* prepared_statement, monad_query_result* out_query_result) {
    if (connection == nullptr || connection->_connection == nullptr ||
        prepared_statement == nullptr || prepared_statement->_prepared_statement == nullptr ||
        prepared_statement->_bound_values == nullptr) {
        return MonadError;
    }
    try {
        auto prepared_statement_ptr =
            static_cast<PreparedStatement*>(prepared_statement->_prepared_statement);
        auto bound_values = static_cast<std::unordered_map<std::string, std::unique_ptr<Value>>*>(
            prepared_statement->_bound_values);

        // Must copy the parameters for safety, and so that the parameters in the prepared statement
        // stay the same.
        std::unordered_map<std::string, std::unique_ptr<Value>> copied_bound_values;
        for (auto& [name, value] : *bound_values) {
            copied_bound_values.emplace(name, value->copy());
        }

        auto query_result =
            static_cast<Connection*>(connection->_connection)
                ->executeWithParams(prepared_statement_ptr, std::move(copied_bound_values))
                .release();
        if (query_result == nullptr) {
            return MonadError;
        }
        out_query_result->_query_result = query_result;
        out_query_result->_is_owned_by_cpp = false;
        if (!query_result->isSuccess()) {
            return MonadError;
        }
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}
void monad_connection_interrupt(monad_connection* connection) {
    static_cast<Connection*>(connection->_connection)->interrupt();
}

monad_state monad_connection_set_query_timeout(monad_connection* connection, uint64_t timeout_in_ms) {
    if (connection == nullptr || connection->_connection == nullptr) {
        return MonadError;
    }
    try {
        static_cast<Connection*>(connection->_connection)->setQueryTimeOut(timeout_in_ms);
    } catch (Exception& e) {
        return MonadError;
    }
    return MonadSuccess;
}
