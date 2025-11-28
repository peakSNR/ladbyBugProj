add_library(monad_parser_antlr_parser
        OBJECT
        monad_cypher_parser.cpp
        parser_error_listener.cpp
        parser_error_strategy.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:monad_parser_antlr_parser>
        PARENT_SCOPE)
