add_library(monad_processor_operator_hash_join
        OBJECT
        hash_join_build.cpp
        hash_join_probe.cpp
        join_hash_table.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:monad_processor_operator_hash_join>
        PARENT_SCOPE)
