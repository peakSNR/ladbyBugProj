add_library(monad_hnsw_index_catalog
        OBJECT
        hnsw_index_catalog_entry.cpp)

set(VECTOR_EXTENSION_OBJECT_FILES
        ${VECTOR_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:monad_hnsw_index_catalog>
        PARENT_SCOPE)
