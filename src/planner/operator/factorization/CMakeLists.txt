add_library(monad_planner_factorization
        OBJECT
        flatten_resolver.cpp
        sink_util.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:monad_planner_factorization>
        PARENT_SCOPE)
