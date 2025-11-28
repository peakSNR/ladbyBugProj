add_library(monad_duckdb_storage
        OBJECT
        duckdb_storage.cpp)

set(DUCKDB_EXTENSION_OBJECT_FILES
        ${DUCKDB_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:monad_duckdb_storage>
        PARENT_SCOPE)
