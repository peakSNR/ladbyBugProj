add_library(monad_storage_compression
        OBJECT
        compression.cpp
        float_compression.cpp
        bitpacking_int128.cpp
        bitpacking_utils.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:monad_storage_compression>
        PARENT_SCOPE)

target_link_libraries(monad_storage_compression PRIVATE fastpfor libalp)
