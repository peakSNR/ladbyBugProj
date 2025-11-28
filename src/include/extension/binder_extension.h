#pragma once

#include "binder/bound_statement.h"
#include "parser/statement.h"

namespace monad {
namespace extension {

class MONAD_API BinderExtension {
public:
    BinderExtension() {}

    virtual ~BinderExtension() = default;

    virtual std::unique_ptr<binder::BoundStatement> bind(const parser::Statement& statement) = 0;
};

} // namespace extension
} // namespace monad
