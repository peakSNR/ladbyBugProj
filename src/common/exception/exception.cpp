#include "common/exception/exception.h"

#ifdef MONAD_BACKTRACE
#include <cpptrace/cpptrace.hpp>
#endif

namespace monad {
namespace common {

Exception::Exception(std::string msg) : exception(), exception_message_(std::move(msg)) {
#ifdef MONAD_BACKTRACE
    cpptrace::generate_trace(1 /*skip this function's frame*/).print();
#endif
}

} // namespace common
} // namespace monad
