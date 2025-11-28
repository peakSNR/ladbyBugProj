#include "planner/operator/logical_flatten.h"

using namespace monad::common;

namespace monad {
namespace planner {

void LogicalFlatten::computeFactorizedSchema() {
    copyChildSchema(0);
    schema->flattenGroup(groupPos);
}

void LogicalFlatten::computeFlatSchema() {
    throw InternalException("LogicalFlatten::computeFlatSchema() should never be used.");
}

} // namespace planner
} // namespace monad
