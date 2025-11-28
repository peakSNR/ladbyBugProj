add_library(monad_json_common
        OBJECT
        json_common.cpp)

set(JSON_EXTENSION_OBJECT_FILES
        ${JSON_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:monad_json_common>
        PARENT_SCOPE)
