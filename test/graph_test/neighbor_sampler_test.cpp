#include "gtest/gtest.h"

#include <fstream>

#include "main/monad.h"
#include "common/string_format.h"
#include "test_helper/test_helper.h"

using namespace monad::main;
using namespace monad::testing;
using namespace monad::common;

TEST(NeighborSamplerGTest, ReplaceAndFanout) {
    SystemConfig config;
    Database db(":memory:", config);
    Connection conn(&db);

    ASSERT_TRUE(conn.query("CREATE NODE TABLE person(id STRING, PRIMARY KEY(id));")->isSuccess());
    ASSERT_TRUE(conn.query("CREATE REL TABLE knows (FROM person TO person, MANY_MANY);")->isSuccess());

    // Use COPY instead of INSERT (INSERT not supported in Cypher grammar).
    auto tmpDir = TestHelper::getTempDir("neighbor_sampler");
    auto personPath = tmpDir + "/person.csv";
    auto relPath = tmpDir + "/knows.csv";
    {
        std::ofstream f(personPath);
        f << "id\n"
          << "a\n"
          << "b\n"
          << "c\n";
    }
    {
        std::ofstream f(relPath);
        f << "from_id,to_id\n"
          << "a,b\n"
          << "a,c\n";
    }

    ASSERT_TRUE(conn.query(monad::common::stringFormat("COPY person FROM \"{}\" (HEADER=true);", personPath))->isSuccess());
    ASSERT_TRUE(conn.query(monad::common::stringFormat("COPY knows FROM \"{}\" (HEADER=true);", relPath))->isSuccess());
    ASSERT_TRUE(conn.query("CALL PROJECT_GRAPH('NS', ['person'], ['knows']);")->isSuccess());

    auto result = conn.query("CALL neighbor_sample('NS', {person: [0]}, [3], {replace: true, seed: 7}) RETURN COUNT(*) AS c;");
    ASSERT_TRUE(result->isSuccess());
    auto row = result->getNext();
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(row->getValue(0)->template getValue<int64_t>(), 3);
}

TEST(NeighborSamplerGTest, TemporalAsOf) {
    SystemConfig config;
    Database db(":memory:", config);
    Connection conn(&db);

    ASSERT_TRUE(conn.query("CREATE NODE TABLE person(id STRING, PRIMARY KEY(id));")->isSuccess());
    ASSERT_TRUE(conn.query("CREATE REL TABLE knows (FROM person TO person, start_at DATE, end_at DATE, MANY_MANY);")->isSuccess());

    auto tmpDir = TestHelper::getTempDir("neighbor_sampler_time");
    auto personPath = tmpDir + "/person.csv";
    auto relPath = tmpDir + "/knows.csv";
    {
        std::ofstream f(personPath);
        f << "id\n"
          << "a\n"
          << "b\n"
          << "c\n";
    }
    {
        std::ofstream f(relPath);
        f << "from_id,to_id,start_at,end_at\n"
          << "a,b,2024-01-01,2024-12-31\n"
          << "a,c,2025-01-01,\n";
    }

    ASSERT_TRUE(conn.query(monad::common::stringFormat("COPY person FROM \"{}\" (HEADER=true);", personPath))->isSuccess());
    ASSERT_TRUE(conn.query(monad::common::stringFormat("COPY knows FROM \"{}\" (HEADER=true);", relPath))->isSuccess());
    ASSERT_TRUE(conn.query("CALL PROJECT_GRAPH('NS_TIME', ['person'], ['knows']);")->isSuccess());

    auto r1 = conn.query("CALL neighbor_sample('NS_TIME', {person: [0]}, [10], {time: date('2024-06-01')}) RETURN dst;");
    ASSERT_TRUE(r1->isSuccess());
    auto t1 = r1->getNext();
    ASSERT_NE(t1, nullptr);
    auto r2 = conn.query("CALL neighbor_sample('NS_TIME', {person: [0]}, [10], {time: date('2025-06-01')}) RETURN dst;");
    ASSERT_TRUE(r2->isSuccess());
    auto t2 = r2->getNext();
    ASSERT_NE(t2, nullptr);
    auto dstID2024 = t1->getValue(0)->template getValue<internalID_t>();
    auto dstID2025 = t2->getValue(0)->template getValue<internalID_t>();
    EXPECT_NE(dstID2024.offset, dstID2025.offset);
}
