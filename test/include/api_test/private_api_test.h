#pragma once

#include "graph_test/private_graph_test.h"

namespace monad {
namespace testing {

class PrivateApiTest : public DBTest {
public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        createDBAndConn();
        initGraph();
    }

    std::string getInputDir() override {
        return TestHelper::appendMonadRootPath("dataset/tinysnb/");
    }
};

} // namespace testing
} // namespace monad
