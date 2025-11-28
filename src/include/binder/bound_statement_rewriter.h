#pragma once

#include "bound_statement.h"

namespace monad {
namespace binder {

// Perform semantic rewrite over bound statement.
class BoundStatementRewriter {
public:
    static void rewrite(BoundStatement& boundStatement, main::ClientContext& clientContext);
};

} // namespace binder
} // namespace monad
