#include "function/aggregate/sum.h"

namespace monad {
namespace function {

using namespace monad::common;

function_set AggregateSumFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        AggregateFunctionUtils::appendSumOrAvgFuncs<SumFunction>(name, typeID, result);
    }
    return result;
}

} // namespace function
} // namespace monad
