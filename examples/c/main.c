#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

#include "monad.h"

int main() {
    monad_database db;
    monad_connection conn;
    monad_database_init("" /* fill db path */, monad_default_system_config(), &db);
    monad_connection_init(&db, &conn);

    // Create schema.
    monad_query_result result;
    monad_connection_query(
        &conn, "CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));", &result);
    monad_query_result_destroy(&result);
    // Create nodes.
    monad_connection_query(&conn, "CREATE (:Person {name: 'Alice', age: 25});", &result);
    monad_query_result_destroy(&result);
    monad_connection_query(&conn, "CREATE (:Person {name: 'Bob', age: 30});", &result);
    monad_query_result_destroy(&result);

    // Execute a simple query.
    monad_connection_query(&conn, "MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;", &result);

    // Fetch each value.
    monad_flat_tuple tuple;
    monad_value value;
    while (monad_query_result_has_next(&result)) {
        monad_query_result_get_next(&result, &tuple);

        monad_flat_tuple_get_value(&tuple, 0, &value);
        char* name;
        monad_value_get_string(&value, &name);

        monad_flat_tuple_get_value(&tuple, 1, &value);
        int64_t age;
        monad_value_get_int64(&value, &age);

        printf("name: %s, age: %" PRIi64 " \n", name, age);
        monad_destroy_string(name);
    }
    monad_value_destroy(&value);
    monad_flat_tuple_destroy(&tuple);

    // Print query result.
    char* result_string = monad_query_result_to_string(&result);
    printf("%s", result_string);
    monad_destroy_string(result_string);

    monad_query_result_destroy(&result);
    monad_connection_destroy(&conn);
    monad_database_destroy(&db);
    return 0;
}
