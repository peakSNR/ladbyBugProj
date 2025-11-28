#pragma once

#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/enums/extend_direction.h"
#include "common/types/types.h"
#include "common/types/timestamp_t.h"
#include "graph/graph.h"

namespace monad {
namespace graph {

struct HopFanout {
    common::ExtendDirection direction = common::ExtendDirection::FWD;
    int64_t defaultFanout = -1; // -1 means sample all neighbors
    std::unordered_map<common::oid_t, int64_t> relFanouts;
};

struct NeighborSamplingConfig {
    std::vector<HopFanout> hopFanouts;
    common::ExtendDirection defaultDirection = common::ExtendDirection::FWD;
    bool replace = false;
    std::optional<uint64_t> randomSeed;
    std::optional<common::timestamp_t> asOf;
    std::vector<std::string> temporalStartFields{"start_at"};
    std::vector<std::string> temporalEndFields{"end_at"};
};

struct NeighborSamplingInput {
    std::vector<common::nodeID_t> seedNodes;
};

struct SampledNodeInfo {
    common::nodeID_t node;
    uint32_t hop;
};

struct SampledEdgeInfo {
    common::nodeID_t src;
    common::nodeID_t dst;
    uint32_t hop;
    uint64_t srcLocal;
    uint64_t dstLocal;
    common::oid_t relTableID;
};

struct NeighborSamplingResult {
    std::vector<SampledNodeInfo> nodes;
    std::vector<SampledEdgeInfo> edges;
    std::vector<uint64_t> numSampledNodesPerHop;
    std::vector<uint64_t> numSampledEdgesPerHop;
};

struct RelationScanner {
    GraphRelInfo info;
    std::unique_ptr<NbrScanState> state;
    std::optional<size_t> startPropIdx;
    std::optional<size_t> endPropIdx;
    common::LogicalTypeID startPropType = common::LogicalTypeID::ANY;
    common::LogicalTypeID endPropType = common::LogicalTypeID::ANY;

    bool requiresTemporalFilter() const { return startPropIdx.has_value() || endPropIdx.has_value(); }

    RelationScanner(GraphRelInfo info, std::unique_ptr<NbrScanState> state,
        std::optional<size_t> startIdx = std::nullopt, std::optional<size_t> endIdx = std::nullopt,
        common::LogicalTypeID startType = common::LogicalTypeID::ANY,
        common::LogicalTypeID endType = common::LogicalTypeID::ANY)
        : info{std::move(info)}, state{std::move(state)}, startPropIdx{startIdx}, endPropIdx{endIdx},
          startPropType{startType}, endPropType{endType} {}
};

using ScannerBucket = std::vector<RelationScanner>;
using ScannerMap = std::unordered_map<common::table_id_t, ScannerBucket>;

struct NeighborScannerCache {
    std::shared_ptr<ScannerMap> forwardTemporal;
    std::shared_ptr<ScannerMap> forwardNonTemporal;
    std::shared_ptr<ScannerMap> backwardTemporal;
    std::shared_ptr<ScannerMap> backwardNonTemporal;
};

class NeighborSampler {
public:
    NeighborSampler(Graph& graph, NeighborSamplingConfig config,
        std::shared_ptr<NeighborScannerCache> cache = nullptr);

    NeighborSamplingResult run(const NeighborSamplingInput& input);

    // Reuse prepared scanner maps to avoid rebuilding across calls when config is unchanged.
    void preloadScanners();

private:
    Graph& graph;
    NeighborSamplingConfig config;
    std::shared_ptr<NeighborScannerCache> externalCache;

    // Optional cached scanners keyed by direction.
    std::shared_ptr<ScannerMap> cachedForward;
    std::shared_ptr<ScannerMap> cachedBackward;
    bool cachedForwardTemporal = false;
    bool cachedBackwardTemporal = false;
};

} // namespace graph
} // namespace monad
