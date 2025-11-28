#pragma once

#include "binder/expression/expression.h"
#include "common/types/value/value.h"

namespace monad {
namespace evaluator {

struct ExpressionEvaluatorUtils {
    static MONAD_API common::Value evaluateConstantExpression(
        std::shared_ptr<binder::Expression> expression, main::ClientContext* clientContext);
};

} // namespace evaluator
} // namespace monad
