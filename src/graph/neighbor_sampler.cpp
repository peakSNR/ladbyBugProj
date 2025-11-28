#include "graph/neighbor_sampler.h"

#include <algorithm>
#include <memory>
#include <numeric>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <string>

#include "common/exception/runtime.h"
#include "function/hash/hash_functions.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "graph/graph.h"
#include "common/string_utils.h"
#include "common/types/timestamp_t.h"
#include "common/types/date_t.h"

using namespace monad::common;
using namespace monad::function;

namespace monad {
namespace graph {

namespace {

int64_t resolveFanout(const HopFanout& hopFanout, table_id_t relTableID) {
    auto it = hopFanout.relFanouts.find(relTableID);
    if (it != hopFanout.relFanouts.end()) {
        return it->second;
    }
    return hopFanout.defaultFanout;
}

bool directionAvailable(const GraphRelInfo& info, bool forward) {
    const auto directions = info.relGroupEntry->constCast<catalog::RelGroupCatalogEntry>().getRelDataDirections();
    const auto required =
        forward ? common::RelDataDirection::FWD : common::RelDataDirection::BWD;
    return std::find(directions.begin(), directions.end(), required) != directions.end();
}

ScannerMap buildScannerMap(Graph& graph, bool forward, bool includeTemporal,
    const std::vector<std::string>& temporalStarts, const std::vector<std::string>& temporalEnds) {
    ScannerMap scanners;
    std::unordered_set<std::string> startNames;
    std::unordered_set<std::string> endNames;
    if (includeTemporal) {
        for (const auto& s : temporalStarts) {
            startNames.insert(common::StringUtils::getLower(s));
        }
        for (const auto& e : temporalEnds) {
            endNames.insert(common::StringUtils::getLower(e));
        }
    }
    auto nodeTableIDs = graph.getNodeTableIDs();
    for (auto tableID : nodeTableIDs) {
        auto relInfos = graph.getRelInfos(tableID);
        for (auto& info : relInfos) {
            if (!directionAvailable(info, forward)) {
                continue;
            }
            auto nbrTableID = forward ? info.dstTableID : info.srcTableID;
            std::vector<std::string> relProps;
            std::optional<size_t> startIdx;
            std::optional<size_t> endIdx;
            common::LogicalTypeID startType = common::LogicalTypeID::ANY;
            common::LogicalTypeID endType = common::LogicalTypeID::ANY;
            if (includeTemporal) {
                auto props = info.relGroupEntry->getProperties();
                for (auto& prop : props) {
                    auto name = common::StringUtils::getLower(prop.getName());
                    if (startNames.contains(name)) {
                        startIdx = relProps.size();
                        startType = prop.getType().getLogicalTypeID();
                        relProps.emplace_back(prop.getName());
                    } else if (endNames.contains(name)) {
                        endIdx = relProps.size();
                        endType = prop.getType().getLogicalTypeID();
                        relProps.emplace_back(prop.getName());
                    }
                }
            }
            auto state = graph.prepareRelScan(*info.relGroupEntry, info.relTableID, nbrTableID,
                relProps, true /* random lookup */);
            auto keyTableID = forward ? info.srcTableID : info.dstTableID;
            scanners[keyTableID].emplace_back(info, std::move(state), startIdx, endIdx, startType,
                endType);
        }
    }
    return scanners;
}

bool validAt(const RelationScanner& scanner,
    std::span<const std::shared_ptr<ValueVector>> props, sel_t idx,
    const std::optional<timestamp_t>& asOf) {
    if (!asOf || !scanner.requiresTemporalFilter()) {
        return true;
    }
    const auto ts = asOf.value();
    auto convertToTimestamp = [](const ValueVector& vec, sel_t pos, LogicalTypeID type) {
        switch (type) {
        case LogicalTypeID::DATE: {
            auto d = vec.getValue<date_t>(pos);
            return Timestamp::fromDateTime(d, dtime_t{0});
        }
        case LogicalTypeID::TIMESTAMP:
        case LogicalTypeID::TIMESTAMP_TZ:
        case LogicalTypeID::TIMESTAMP_MS:
        case LogicalTypeID::TIMESTAMP_SEC:
        case LogicalTypeID::TIMESTAMP_NS:
            return vec.getValue<timestamp_t>(pos);
        default:
            return vec.getValue<timestamp_t>(pos); // best effort
        }
    };

    if (scanner.startPropIdx.has_value()) {
        const auto& vec = *props[scanner.startPropIdx.value()];
        if (!vec.isNull(idx)) {
            auto startVal = convertToTimestamp(vec, idx, scanner.startPropType);
            if (ts < startVal) {
                return false;
            }
        }
    }
    if (scanner.endPropIdx.has_value()) {
        const auto& vec = *props[scanner.endPropIdx.value()];
        if (!vec.isNull(idx)) {
            auto endVal = convertToTimestamp(vec, idx, scanner.endPropType);
            if (ts >= endVal) {
                return false;
            }
        }
    }
    return true;
}

void collectNeighbors(Graph& graph, RelationScanner& scanner, nodeID_t nodeID, bool forward,
    std::vector<nodeID_t>& out, const std::optional<timestamp_t>& asOf) {
    auto iterator = forward ? graph.scanFwd(nodeID, *scanner.state) :
                              graph.scanBwd(nodeID, *scanner.state);
    for (const auto chunk : iterator) {
        chunk.forEach([&](auto neighbors, auto props, auto i) {
            if (validAt(scanner, props, i, asOf)) {
                out.push_back(neighbors[i]);
            }
        });
    }
}

std::vector<nodeID_t> reservoirSampleNeighbors(Graph& graph, RelationScanner& scanner,
    nodeID_t nodeID, bool forward, size_t fanout, std::mt19937_64& rng,
    const std::optional<timestamp_t>& asOf) {
    std::vector<nodeID_t> reservoir;
    reservoir.reserve(fanout);
    auto iterator = forward ? graph.scanFwd(nodeID, *scanner.state) :
                              graph.scanBwd(nodeID, *scanner.state);
    size_t seen = 0;
    for (const auto chunk : iterator) {
        chunk.forEach([&](auto neighbors, auto props, auto i) {
            if (!validAt(scanner, props, i, asOf)) {
                return;
            }
            if (seen < fanout) {
                reservoir.push_back(neighbors[i]);
            } else {
                std::uniform_int_distribution<size_t> dist(0, seen);
                size_t j = dist(rng);
                if (j < fanout) {
                    reservoir[j] = neighbors[i];
                }
            }
            ++seen;
        });
    }
    return reservoir;
}

std::vector<size_t> selectIndices(size_t population, int64_t fanout, bool replace,
    std::mt19937_64& rng) {
    std::vector<size_t> indices;
    if (population == 0 || fanout == 0) {
        return indices;
    }
    if (fanout < 0 || (!replace && static_cast<size_t>(fanout) >= population)) {
        indices.resize(population);
        std::iota(indices.begin(), indices.end(), 0);
        return indices;
    }
    auto target = static_cast<size_t>(fanout);
    if (replace) {
        std::uniform_int_distribution<size_t> dist(0, population - 1);
        indices.resize(target);
        for (size_t i = 0; i < target; ++i) {
            indices[i] = dist(rng);
        }
        return indices;
    }
    std::vector<size_t> perm(population);
    std::iota(perm.begin(), perm.end(), 0);
    for (size_t i = 0; i < target; ++i) {
        std::uniform_int_distribution<size_t> dist(i, population - 1);
        auto idx = dist(rng);
        std::swap(perm[i], perm[idx]);
    }
    perm.resize(target);
    return perm;
}

} // namespace

NeighborSampler::NeighborSampler(Graph& graph, NeighborSamplingConfig config,
    std::shared_ptr<NeighborScannerCache> cache)
    : graph{graph}, config{std::move(config)}, externalCache{std::move(cache)} {}

void NeighborSampler::preloadScanners() {
    auto includeTemporal = config.asOf.has_value();

    auto prepareScanner = [&](bool forward) -> std::shared_ptr<ScannerMap> {
        std::shared_ptr<ScannerMap> cachePtr;
        if (externalCache) {
            cachePtr = forward ? (includeTemporal ? externalCache->forwardTemporal
                                                  : externalCache->forwardNonTemporal)
                               : (includeTemporal ? externalCache->backwardTemporal
                                                  : externalCache->backwardNonTemporal);
        }
        if (cachePtr) {
            return cachePtr;
        }
        auto built = std::make_shared<ScannerMap>(buildScannerMap(graph, forward, includeTemporal,
            config.temporalStartFields, config.temporalEndFields));
        if (externalCache) {
            if (forward) {
                if (includeTemporal) {
                    externalCache->forwardTemporal = built;
                } else {
                    externalCache->forwardNonTemporal = built;
                }
            } else {
                if (includeTemporal) {
                    externalCache->backwardTemporal = built;
                } else {
                    externalCache->backwardNonTemporal = built;
                }
            }
        }
        return built;
    };

    if (!cachedForward || (includeTemporal && !cachedForwardTemporal)) {
        cachedForward = prepareScanner(true /* forward */);
        cachedForwardTemporal = includeTemporal;
    }
    if (config.defaultDirection != ExtendDirection::FWD) {
        if (!cachedBackward || (includeTemporal && !cachedBackwardTemporal)) {
            cachedBackward = prepareScanner(false /* backward */);
            cachedBackwardTemporal = includeTemporal;
        }
    }
}

NeighborSamplingResult NeighborSampler::run(const NeighborSamplingInput& input) {
    if (input.seedNodes.empty()) {
        throw RuntimeException("Neighbor sampling requires at least one seed node.");
    }
    if (config.hopFanouts.empty()) {
        throw RuntimeException("Neighbor sampling requires at least one hop fanout.");
    }
    if (config.replace) {
        for (const auto& hf : config.hopFanouts) {
            if (hf.defaultFanout < 0) {
                throw RuntimeException("replace=true cannot be combined with fanout = -1 (sample all).");
            }
            for (const auto& kv : hf.relFanouts) {
                if (kv.second < 0) {
                    throw RuntimeException("replace=true cannot be combined with fanout = -1 (sample all).");
                }
            }
        }
    }

    auto includeTemporal = config.asOf.has_value();
    if (!cachedForward || (includeTemporal && !cachedForwardTemporal)) {
        cachedForward = std::make_shared<ScannerMap>(buildScannerMap(graph, true /* forward */, includeTemporal,
            config.temporalStartFields, config.temporalEndFields));
        cachedForwardTemporal = includeTemporal;
    }
    auto& forwardScanners = *cachedForward;

    std::shared_ptr<ScannerMap> backwardScannersLocal;
    auto getBackwardScanners = [&]() -> const ScannerMap& {
        if (cachedBackward) {
            return *cachedBackward;
        }
        if (!backwardScannersLocal) {
            backwardScannersLocal = std::make_shared<ScannerMap>(buildScannerMap(graph, false, includeTemporal,
                config.temporalStartFields, config.temporalEndFields));
        }
        return *backwardScannersLocal;
    };
    auto hasBackwardScanners = [&]() -> bool {
        return !getBackwardScanners().empty();
    };

    std::unordered_map<nodeID_t, uint64_t, InternalIDHasher> nodeToLocal;
    NeighborSamplingResult result;
    result.nodes.reserve(input.seedNodes.size());

    for (auto nodeID : input.seedNodes) {
        if (nodeToLocal.contains(nodeID)) {
            continue;
        }
        auto localID = static_cast<uint64_t>(result.nodes.size());
        nodeToLocal.emplace(nodeID, localID);
        result.nodes.push_back({nodeID, 0u});
    }

    size_t begin = 0;
    size_t end = result.nodes.size();
    result.numSampledNodesPerHop.push_back(end - begin);
    result.numSampledEdgesPerHop.reserve(config.hopFanouts.size());
    result.nodes.reserve(end + config.hopFanouts.size() * 8);

    auto rngSeed = config.randomSeed.has_value() ? config.randomSeed.value() :
                                                     std::random_device{}();
    std::mt19937_64 rng(rngSeed);
    std::vector<nodeID_t> neighbors;

    for (size_t hop = 0; hop < config.hopFanouts.size() && begin < end; ++hop) {
        const auto& hopFanout = config.hopFanouts[hop];
        uint64_t hopEdgeCount = 0;
        for (size_t idx = begin; idx < end; ++idx) {
            auto tableID = result.nodes[idx].node.tableID;
            auto processBucket = [&](const ScannerMap& scanners, bool forwardDir) {
                auto bucketIt = scanners.find(tableID);
                if (bucketIt == scanners.end()) {
                    return;
                }
                auto& bucket = const_cast<ScannerBucket&>(bucketIt->second);
                for (auto& scanner : bucket) {
                    auto fanout = resolveFanout(hopFanout, scanner.info.relTableID);
                    if (fanout >= 0 && !config.replace) {
                        auto sampledNeighbors = reservoirSampleNeighbors(graph, scanner, result.nodes[idx].node,
                            forwardDir, static_cast<size_t>(fanout), rng, config.asOf);
                        for (auto& neighborNode : sampledNeighbors) {
                            auto [it, inserted] = nodeToLocal.emplace(neighborNode,
                                static_cast<uint64_t>(result.nodes.size()));
                            uint64_t neighborLocal = it->second;
                            if (inserted) {
                                result.nodes.push_back({neighborNode, static_cast<uint32_t>(hop + 1)});
                            }
                            if (forwardDir) {
                                result.edges.push_back({result.nodes[idx].node, neighborNode,
                                    static_cast<uint32_t>(hop + 1), static_cast<uint64_t>(idx), neighborLocal,
                                    scanner.info.relTableID});
                            } else {
                                result.edges.push_back({neighborNode, result.nodes[idx].node,
                                    static_cast<uint32_t>(hop + 1), neighborLocal, static_cast<uint64_t>(idx),
                                    scanner.info.relTableID});
                            }
                            hopEdgeCount++;
                        }
                        continue;
                    }

                    if (fanout >= 0 && config.replace) {
                        neighbors.clear();
                        collectNeighbors(graph, scanner, result.nodes[idx].node, forwardDir, neighbors, config.asOf);
                        if (neighbors.empty()) {
                            continue;
                        }
                        std::uniform_int_distribution<size_t> dist(0, neighbors.size() - 1);
                        for (int64_t i = 0; i < fanout; ++i) {
                            auto neighborNode = neighbors[dist(rng)];
                            auto [it, inserted] = nodeToLocal.emplace(
                                neighborNode, static_cast<uint64_t>(result.nodes.size()));
                            uint64_t neighborLocal = it->second;
                            if (inserted) {
                                result.nodes.push_back({neighborNode, static_cast<uint32_t>(hop + 1)});
                            }
                            if (forwardDir) {
                                result.edges.push_back({result.nodes[idx].node, neighborNode,
                                    static_cast<uint32_t>(hop + 1), static_cast<uint64_t>(idx), neighborLocal,
                                    scanner.info.relTableID});
                            } else {
                                result.edges.push_back({neighborNode, result.nodes[idx].node,
                                    static_cast<uint32_t>(hop + 1), neighborLocal, static_cast<uint64_t>(idx),
                                    scanner.info.relTableID});
                            }
                            hopEdgeCount++;
                        }
                        continue;
                    }

                    neighbors.clear();
                    collectNeighbors(graph, scanner, result.nodes[idx].node, forwardDir, neighbors, config.asOf);
                    if (neighbors.empty()) {
                        continue;
                    }
                    auto selected = selectIndices(neighbors.size(), fanout, config.replace, rng);
                    for (auto nbrIdx : selected) {
                        auto neighborNode = neighbors[nbrIdx];
                        auto [it, inserted] =
                            nodeToLocal.emplace(neighborNode, static_cast<uint64_t>(result.nodes.size()));
                        uint64_t neighborLocal = it->second;
                        if (inserted) {
                            result.nodes.push_back({neighborNode, static_cast<uint32_t>(hop + 1)});
                        }
                        if (forwardDir) {
                            result.edges.push_back({result.nodes[idx].node, neighborNode,
                                static_cast<uint32_t>(hop + 1), static_cast<uint64_t>(idx), neighborLocal,
                                scanner.info.relTableID});
                        } else {
                            result.edges.push_back({neighborNode, result.nodes[idx].node,
                                static_cast<uint32_t>(hop + 1), neighborLocal, static_cast<uint64_t>(idx),
                                scanner.info.relTableID});
                        }
                        hopEdgeCount++;
                    }
                }
            };

            switch (hopFanout.direction) {
            case ExtendDirection::FWD:
                processBucket(forwardScanners, true);
                break;
            case ExtendDirection::BWD:
                if (!hasBackwardScanners()) {
                    throw RuntimeException(
                        "Requested backward neighbor sampling, but the graph was not stored with "
                        "backward adjacency for the selected relationships. Re-create the "
                        "projection with BOTH/BWD storage_direction.");
                }
                processBucket(getBackwardScanners(), false);
                break;
            case ExtendDirection::BOTH:
                processBucket(forwardScanners, true);
                if (!hasBackwardScanners()) {
                    throw RuntimeException(
                        "Requested BOTH neighbor sampling, but backward adjacency is unavailable "
                        "for at least one hop. Re-create the projection with BOTH storage_direction.");
                }
                // Ensure every forward relation also has backward adjacency to avoid silent drops.
                {
                    const auto& backwardMap = getBackwardScanners();
                    auto fwdIt = forwardScanners.find(tableID);
                    if (fwdIt != forwardScanners.end()) {
                        std::unordered_set<oid_t> backwardRels;
                        auto bwdIt = backwardMap.find(tableID);
                        if (bwdIt != backwardMap.end()) {
                            for (const auto& scanner : bwdIt->second) {
                                backwardRels.insert(scanner.info.relTableID);
                            }
                        }
                        for (const auto& scanner : fwdIt->second) {
                            if (!backwardRels.contains(scanner.info.relTableID)) {
                                throw RuntimeException(
                                    "Requested BOTH neighbor sampling, but backward adjacency is unavailable for "
                                    "relationship table ID " + std::to_string(scanner.info.relTableID) + ". "
                                    "Re-create the projection with BOTH storage_direction for that relationship.");
                            }
                        }
                    }
                }
                processBucket(getBackwardScanners(), false);
                break;
            default:
                throw RuntimeException("Unsupported sampling direction.");
            }
        }
        result.numSampledEdgesPerHop.push_back(hopEdgeCount);
        result.numSampledNodesPerHop.push_back(result.nodes.size() - end);
        begin = end;
        end = result.nodes.size();
    }

    return result;
}

} // namespace graph
} // namespace monad
