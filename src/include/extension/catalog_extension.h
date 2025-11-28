#pragma once

#include "catalog/catalog.h"

namespace monad {
namespace extension {

class MONAD_API CatalogExtension : public catalog::Catalog {
public:
    CatalogExtension() : Catalog() {}

    virtual void init() = 0;

    void invalidateCache();
};

} // namespace extension
} // namespace monad
