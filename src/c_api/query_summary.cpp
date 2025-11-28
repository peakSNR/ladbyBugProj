#include "main/query_summary.h"

#include <cstdlib>

#include "c_api/monad.h"

using namespace monad::main;

void monad_query_summary_destroy(monad_query_summary* query_summary) {
    if (query_summary == nullptr) {
        return;
    }
    // The query summary is owned by the query result, so it should not be deleted here.
    query_summary->_query_summary = nullptr;
}

double monad_query_summary_get_compiling_time(monad_query_summary* query_summary) {
    return static_cast<QuerySummary*>(query_summary->_query_summary)->getCompilingTime();
}

double monad_query_summary_get_execution_time(monad_query_summary* query_summary) {
    return static_cast<QuerySummary*>(query_summary->_query_summary)->getExecutionTime();
}
