include_directories(
    ${PROJECT_SOURCE_DIR}/third_party/yyjson/src
    ${PROJECT_SOURCE_DIR}/extension/json/src/include
)

add_library(monad_shell_printer
        OBJECT
        json_printer.cpp
        printer_factory.cpp
        ${PROJECT_SOURCE_DIR}/extension/json/src/utils/json_utils.cpp
        ${PROJECT_SOURCE_DIR}/extension/json/src/type/json_type.cpp
)

set(MONAD_SHELL_OBJECT_FILES
        ${MONAD_SHELL_OBJECT_FILES} $<TARGET_OBJECTS:monad_shell_printer>
        PARENT_SCOPE)
