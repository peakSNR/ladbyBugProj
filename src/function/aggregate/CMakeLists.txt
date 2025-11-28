add_library(monad_function_aggregate
        OBJECT
        count.cpp
        count_star.cpp
        collect.cpp
        min_max.cpp
        sum.cpp
        avg.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:monad_function_aggregate>
        PARENT_SCOPE)
