#include "main/query_result.h"

#include "c_api/helpers.h"
#include "c_api/monad.h"

using namespace monad::main;
using namespace monad::common;
using namespace monad::processor;

void monad_query_result_destroy(monad_query_result* query_result) {
    if (query_result == nullptr) {
        return;
    }
    if (query_result->_query_result != nullptr) {
        if (!query_result->_is_owned_by_cpp) {
            delete static_cast<QueryResult*>(query_result->_query_result);
        }
    }
}

bool monad_query_result_is_success(monad_query_result* query_result) {
    return static_cast<QueryResult*>(query_result->_query_result)->isSuccess();
}

char* monad_query_result_get_error_message(monad_query_result* query_result) {
    auto error_message = static_cast<QueryResult*>(query_result->_query_result)->getErrorMessage();
    if (error_message.empty()) {
        return nullptr;
    }
    return convertToOwnedCString(error_message);
}

uint64_t monad_query_result_get_num_columns(monad_query_result* query_result) {
    return static_cast<QueryResult*>(query_result->_query_result)->getNumColumns();
}

monad_state monad_query_result_get_column_name(monad_query_result* query_result, uint64_t index,
    char** out_column_name) {
    auto column_names = static_cast<QueryResult*>(query_result->_query_result)->getColumnNames();
    if (index >= column_names.size()) {
        return MonadError;
    }
    *out_column_name = convertToOwnedCString(column_names[index]);
    return MonadSuccess;
}

monad_state monad_query_result_get_column_data_type(monad_query_result* query_result, uint64_t index,
    monad_logical_type* out_column_data_type) {
    auto column_data_types =
        static_cast<QueryResult*>(query_result->_query_result)->getColumnDataTypes();
    if (index >= column_data_types.size()) {
        return MonadError;
    }
    const auto& column_data_type = column_data_types[index];
    out_column_data_type->_data_type = new LogicalType(column_data_type.copy());
    return MonadSuccess;
}

uint64_t monad_query_result_get_num_tuples(monad_query_result* query_result) {
    return static_cast<QueryResult*>(query_result->_query_result)->getNumTuples();
}

monad_state monad_query_result_get_query_summary(monad_query_result* query_result,
    monad_query_summary* out_query_summary) {
    if (out_query_summary == nullptr) {
        return MonadError;
    }
    auto query_summary = static_cast<QueryResult*>(query_result->_query_result)->getQuerySummary();
    out_query_summary->_query_summary = query_summary;
    return MonadSuccess;
}

bool monad_query_result_has_next(monad_query_result* query_result) {
    return static_cast<QueryResult*>(query_result->_query_result)->hasNext();
}

bool monad_query_result_has_next_query_result(monad_query_result* query_result) {
    return static_cast<QueryResult*>(query_result->_query_result)->hasNextQueryResult();
}

monad_state monad_query_result_get_next_query_result(monad_query_result* query_result,
    monad_query_result* out_query_result) {
    if (!monad_query_result_has_next_query_result(query_result)) {
        return MonadError;
    }
    auto next_query_result =
        static_cast<QueryResult*>(query_result->_query_result)->getNextQueryResult();
    if (next_query_result == nullptr) {
        return MonadError;
    }
    out_query_result->_query_result = next_query_result;
    out_query_result->_is_owned_by_cpp = true;
    return MonadSuccess;
}

monad_state monad_query_result_get_next(monad_query_result* query_result,
    monad_flat_tuple* out_flat_tuple) {
    try {
        auto flat_tuple = static_cast<QueryResult*>(query_result->_query_result)->getNext();
        out_flat_tuple->_flat_tuple = flat_tuple.get();
        out_flat_tuple->_is_owned_by_cpp = true;
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

char* monad_query_result_to_string(monad_query_result* query_result) {
    std::string result_string = static_cast<QueryResult*>(query_result->_query_result)->toString();
    return convertToOwnedCString(result_string);
}

void monad_query_result_reset_iterator(monad_query_result* query_result) {
    static_cast<QueryResult*>(query_result->_query_result)->resetIterator();
}

monad_state monad_query_result_get_arrow_schema(monad_query_result* query_result,
    ArrowSchema* out_schema) {
    try {
        *out_schema = *static_cast<QueryResult*>(query_result->_query_result)->getArrowSchema();
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}

monad_state monad_query_result_get_next_arrow_chunk(monad_query_result* query_result,
    int64_t chunk_size, ArrowArray* out_arrow_array) {
    try {
        *out_arrow_array =
            *static_cast<QueryResult*>(query_result->_query_result)->getNextArrowChunk(chunk_size);
        return MonadSuccess;
    } catch (Exception& e) {
        return MonadError;
    }
}
