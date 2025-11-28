#include "processor/operator/empty_result.h"

namespace monad {
namespace processor {

bool EmptyResult::getNextTuplesInternal(ExecutionContext*) {
    return false;
}

} // namespace processor
} // namespace monad
