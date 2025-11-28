include_directories(
        include
        ${PROJECT_SOURCE_DIR}/third_party/taywee_args/include
        ${PROJECT_SOURCE_DIR}/third_party/yyjson/src
        ${PROJECT_SOURCE_DIR}/extension/json/src/include
)

add_subdirectory(printer)

add_executable(monad_shell
        ${MONAD_SHELL_OBJECT_FILES}
        embedded_shell.cpp
        linenoise.cpp
        shell_runner.cpp)

if (NOT MSVC)
    # On Windows, rename the executable to monad.exe will cause an error
    # "multiple rules generate src/monad.lib", so we do not set the output
    # name for MSVC.
    set_target_properties(monad_shell PROPERTIES OUTPUT_NAME monad)
endif ()


target_link_libraries(monad_shell monad)
if (ENABLE_BACKTRACES)
    target_link_libraries(monad_shell register_backtrace_signal_handler)
endif ()
if (MSVC)
    target_link_libraries(monad_shell ws2_32)
endif ()

install(TARGETS monad_shell)
