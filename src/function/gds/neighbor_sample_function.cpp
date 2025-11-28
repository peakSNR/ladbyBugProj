#include "function/gds/neighbor_sample_function.h"

#include <memory>
#include <unordered_map>
#include <utility>

#include "binder/binder.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "common/enums/extend_direction_util.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "common/type_utils.h"
#include "common/types/value/nested.h"
#include "common/types/value/node.h"
#include "common/types/value/value.h"
#include "common/types/timestamp_t.h"
#include "function/gds/gds.h"
#include "function/table/simple_table_function.h"
#include "graph/neighbor_sampler.h"
#include "graph/on_disk_graph.h"
#include "main/client_context.h"

using namespace monad::common;
using namespace monad::graph;

namespace monad {
namespace function {

namespace {

struct NeighborSampleBindData final : TableFuncBindData {
    NeighborSampleBindData(const NeighborSampleBindData& other)
        : TableFuncBindData{other}, graphEntry{other.graphEntry.copy()},
          samplingConfig{other.samplingConfig}, seedNodes{other.seedNodes}, result{other.result} {}

    NeighborSampleBindData(const graph::NativeGraphEntry& entry,
        graph::NeighborSamplingConfig config, std::vector<nodeID_t> seeds,
        std::shared_ptr<graph::NeighborSamplingResult> samplingResult,
        binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), samplingResult ? samplingResult->edges.size() : 0},
          graphEntry{entry.copy()}, samplingConfig{std::move(config)}, seedNodes{std::move(seeds)},
          result{std::move(samplingResult)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<NeighborSampleBindData>(*this);
    }

    graph::NativeGraphEntry graphEntry;
    graph::NeighborSamplingConfig samplingConfig;
    std::vector<nodeID_t> seedNodes;
    std::shared_ptr<graph::NeighborSamplingResult> result;
};

using NodeTableMap = std::unordered_map<std::string, table_id_t>;
using RelTableMap = std::unordered_map<std::string, std::vector<oid_t>>;

NodeTableMap buildNodeTableMap(const NativeGraphEntry& entry) {
    NodeTableMap map;
    for (const auto& info : entry.nodeInfos) {
        auto normalized = StringUtils::getLower(info.entry->getName());
        map.insert({normalized, info.entry->getTableID()});
    }
    return map;
}

RelTableMap buildRelTableMap(const NativeGraphEntry& entry) {
    RelTableMap map;
    for (const auto& info : entry.relInfos) {
        auto normalized = StringUtils::getLower(info.entry->getName());
        std::vector<oid_t> oids;
        auto* relGroupEntry = info.entry->ptrCast<catalog::RelGroupCatalogEntry>();
        for (const auto& relInfo : relGroupEntry->getRelEntryInfos()) {
            oids.push_back(relInfo.oid);
        }
        map.insert({normalized, std::move(oids)});
    }
    return map;
}

nodeID_t extractNodeID(const Value& value) {
    switch (value.getDataType().getLogicalTypeID()) {
    case LogicalTypeID::INTERNAL_ID:
        return value.getValue<internalID_t>();
    case LogicalTypeID::NODE: {
        auto nodeIDVal = NodeVal::getNodeIDVal(&value);
        return nodeIDVal->getValue<internalID_t>();
    }
    default:
        throw BinderException(stringFormat(
            "Unsupported seed literal type {}. Expected INTERNAL_ID or NODE.",
            value.getDataType().toString()));
    }
}

void appendOffsetList(const Value& value, table_id_t tableID, std::vector<nodeID_t>& output) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::LIST) {
        throw BinderException("Seed struct entries must be LIST values.");
    }
    auto numChildren = NestedVal::getChildrenSize(&value);
    for (auto i = 0u; i < numChildren; ++i) {
        auto child = NestedVal::getChildVal(&value, i);
        auto type = child->getDataType().getLogicalTypeID();
        if (type == LogicalTypeID::INT64 || type == LogicalTypeID::INT32) {
            auto rawOffset = child->getValue<int64_t>();
            if (rawOffset < 0) {
                throw BinderException("Seed offsets must be non-negative.");
            }
            auto offset = static_cast<offset_t>(rawOffset);
            output.push_back(nodeID_t{offset, tableID});
        } else if (type == LogicalTypeID::INTERNAL_ID || type == LogicalTypeID::NODE) {
            auto node = extractNodeID(*child);
            if (node.tableID != tableID) {
                throw BinderException("Seed node belongs to unexpected table.");
            }
            output.push_back(node);
        } else {
            throw BinderException("Seed lists must contain offsets or INTERNAL_ID values.");
        }
    }
}

std::vector<nodeID_t> parseSeedArgument(const Value& value, const NativeGraphEntry& entry) {
    auto nodeTableMap = buildNodeTableMap(entry);
    std::vector<nodeID_t> seeds;
    if (value.getDataType().getLogicalTypeID() == LogicalTypeID::LIST) {
        auto numChildren = NestedVal::getChildrenSize(&value);
        seeds.reserve(numChildren);
        for (auto i = 0u; i < numChildren; ++i) {
            auto child = NestedVal::getChildVal(&value, i);
            seeds.push_back(extractNodeID(*child));
        }
        return seeds;
    }
    if (value.getDataType().getLogicalTypeID() == LogicalTypeID::STRUCT) {
        auto numFields = StructType::getNumFields(value.getDataType());
        for (auto i = 0u; i < numFields; ++i) {
            auto& field = StructType::getField(value.getDataType(), i);
            auto normalized = StringUtils::getLower(field.getName());
            if (!nodeTableMap.contains(normalized)) {
                throw BinderException(
                    stringFormat("{} is not part of the projected graph.", field.getName()));
            }
            auto tableID = nodeTableMap.at(normalized);
            appendOffsetList(*NestedVal::getChildVal(&value, i), tableID, seeds);
        }
        return seeds;
    }
    throw BinderException("Seed argument must be a LIST or STRUCT literal.");
}

HopFanout parseHopFanoutLiteral(const Value& literal, const RelTableMap& relTableMap,
    ExtendDirection defaultDirection) {
    HopFanout hop;
    hop.direction = defaultDirection;
    hop.defaultFanout = -1;
    switch (literal.getDataType().getLogicalTypeID()) {
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT32:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::UINT32:
        hop.defaultFanout = literal.getValue<int64_t>();
        if (hop.defaultFanout < -1) {
            throw BinderException("Fanout values must be >= -1.");
        }
        return hop;
    case LogicalTypeID::STRUCT: {
        auto numFields = StructType::getNumFields(literal.getDataType());
        for (auto i = 0u; i < numFields; ++i) {
            auto& field = StructType::getField(literal.getDataType(), i);
            auto key = StringUtils::getLower(field.getName());
            auto child = NestedVal::getChildVal(&literal, i);
            if (key == "direction") {
                if (child->getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
                    throw BinderException("direction must be STRING.");
                }
                hop.direction = ExtendDirectionUtil::fromString(child->getValue<std::string>());
        } else if (key == "fanout" || key == "default") {
                hop.defaultFanout = child->getValue<int64_t>();
                if (hop.defaultFanout < -1) {
                    throw BinderException("Fanout values must be >= -1.");
                }
            } else if (key == "rels") {
                if (child->getDataType().getLogicalTypeID() != LogicalTypeID::STRUCT) {
                    throw BinderException("rels must be a STRUCT literal.");
                }
                auto relFields = StructType::getNumFields(child->getDataType());
                for (auto j = 0u; j < relFields; ++j) {
                    auto& relField = StructType::getField(child->getDataType(), j);
                    auto relName = StringUtils::getLower(relField.getName());
                    if (!relTableMap.contains(relName)) {
                        throw BinderException(stringFormat(
                            "{} is not part of the projected graph.", relField.getName()));
                    }
                    auto relVal = NestedVal::getChildVal(child, j);
                    auto relFanout = relVal->getValue<int64_t>();
                    if (relFanout < -1) {
                        throw BinderException("Fanout values must be >= -1.");
                    }
                    for (auto oid : relTableMap.at(relName)) {
                        hop.relFanouts.insert({oid, relFanout});
                    }
                }
            } else {
                throw BinderException(stringFormat("Unknown hop fanout key {}.", field.getName()));
            }
        }
        return hop;
    }
    default:
        throw BinderException("Fanout list entries must be INT or STRUCT literals.");
    }
}

std::vector<HopFanout> parseFanouts(const Value& value, const NativeGraphEntry& entry,
    ExtendDirection defaultDirection) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::LIST) {
        throw BinderException("Fanouts argument must be a LIST literal.");
    }
    auto relTableMap = buildRelTableMap(entry);
    std::vector<HopFanout> fanouts;
    auto numChildren = NestedVal::getChildrenSize(&value);
    fanouts.reserve(numChildren);
    for (auto i = 0u; i < numChildren; ++i) {
        auto child = NestedVal::getChildVal(&value, i);
        fanouts.push_back(parseHopFanoutLiteral(*child, relTableMap, defaultDirection));
    }
    return fanouts;
}

void applyOptions(const Value& value, NeighborSamplingConfig& config) {
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::STRUCT) {
        throw BinderException("Options argument must be a STRUCT literal.");
    }
    auto numFields = StructType::getNumFields(value.getDataType());
    for (auto i = 0u; i < numFields; ++i) {
        auto& field = StructType::getField(value.getDataType(), i);
        auto key = StringUtils::getLower(field.getName());
        auto child = NestedVal::getChildVal(&value, i);
        if (key == "replace") {
            if (child->getDataType().getLogicalTypeID() != LogicalTypeID::BOOL) {
                throw BinderException("replace option must be BOOLEAN.");
            }
            config.replace = child->getValue<bool>();
        } else if (key == "direction") {
            if (child->getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
                throw BinderException("direction option must be STRING.");
            }
            config.defaultDirection = ExtendDirectionUtil::fromString(child->getValue<std::string>());
        } else if (key == "seed") {
            if (!LogicalTypeUtils::isNumerical(child->getDataType())) {
                throw BinderException("seed option must be numeric.");
            }
            auto seedValue = child->getValue<int64_t>();
            if (seedValue < 0) {
                throw BinderException("seed option must be non-negative.");
            }
            config.randomSeed = static_cast<uint64_t>(seedValue);
        } else if (key == "time" || key == "as_of") {
            auto type = child->getDataType().getLogicalTypeID();
            timestamp_t ts;
            if (type == LogicalTypeID::STRING) {
                auto s = child->getValue<std::string>();
                if (!Timestamp::tryConvertTimestamp(s.c_str(), s.size(), ts)) {
                    throw BinderException("Failed to parse time option as TIMESTAMP.");
                }
            } else if (type == LogicalTypeID::DATE) {
                auto d = child->getValue<date_t>();
                ts = Timestamp::fromDateTime(d, dtime_t{0});
            } else if (type == LogicalTypeID::TIMESTAMP || type == LogicalTypeID::TIMESTAMP_TZ ||
                type == LogicalTypeID::TIMESTAMP_MS || type == LogicalTypeID::TIMESTAMP_NS ||
                type == LogicalTypeID::TIMESTAMP_SEC) {
                ts = child->getValue<timestamp_t>();
            } else {
                throw BinderException("time/as_of option must be STRING, DATE, or TIMESTAMP.");
            }
            config.asOf = ts;
        } else if (key == "time_fields" || key == "temporal_fields") {
            if (child->getDataType().getLogicalTypeID() != LogicalTypeID::STRUCT) {
                throw BinderException("time_fields must be a STRUCT with start/end.");
            }
            std::vector<std::string> starts;
            std::vector<std::string> ends;
            auto fields = StructType::getNumFields(child->getDataType());
            for (auto j = 0u; j < fields; ++j) {
                auto& f = StructType::getField(child->getDataType(), j);
                auto k = StringUtils::getLower(f.getName());
                auto v = NestedVal::getChildVal(child, j);
                auto addVals = [&](std::vector<std::string>& target) {
                    if (v->getDataType().getLogicalTypeID() == LogicalTypeID::STRING) {
                        target.push_back(StringUtils::getLower(v->getValue<std::string>()));
                    } else if (v->getDataType().getLogicalTypeID() == LogicalTypeID::LIST) {
                        auto n = NestedVal::getChildrenSize(v);
                        for (auto idx = 0u; idx < n; ++idx) {
                            auto childVal = NestedVal::getChildVal(v, idx);
                            if (childVal->getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
                                throw BinderException("time_fields entries must be STRING.");
                            }
                            target.push_back(StringUtils::getLower(childVal->getValue<std::string>()));
                        }
                    } else {
                        throw BinderException("time_fields start/end must be STRING or LIST of STRING.");
                    }
                };
                if (k == "start") {
                    addVals(starts);
                } else if (k == "end") {
                    addVals(ends);
                } else {
                    throw BinderException("time_fields supports only 'start' and 'end'.");
                }
            }
            if (!starts.empty()) {
                config.temporalStartFields = std::move(starts);
            }
            if (!ends.empty()) {
                config.temporalEndFields = std::move(ends);
            }
        } else {
            throw BinderException(stringFormat("Unknown neighbor_sample option {}.", field.getName()));
        }
    }
}

std::shared_ptr<NeighborSamplingResult> runSampling(const main::ClientContext* context,
    const NativeGraphEntry& entry, const std::vector<nodeID_t>& seeds,
    const NeighborSamplingConfig& config) {
    auto graph = std::make_unique<OnDiskGraph>(const_cast<main::ClientContext*>(context), entry.copy());
    NeighborSampler sampler(*graph, config);
    auto result = sampler.run(NeighborSamplingInput{seeds});
    return std::make_shared<NeighborSamplingResult>(std::move(result));
}

offset_t tableFunc(const TableFuncMorsel& morsel, const TableFuncInput& input,
    DataChunk& output) {
    auto bindData = input.bindData->constPtrCast<NeighborSampleBindData>();
    const auto& edges = bindData->result->edges;
    const auto start = morsel.startOffset;
    const auto end = morsel.endOffset;
    const auto numRows = end - start;
    for (auto i = 0u; i < numRows; ++i) {
        const auto& edge = edges[start + i];
        output.getValueVectorMutable(0).setValue(i, static_cast<int32_t>(edge.hop));
        output.getValueVectorMutable(1).setValue(i, edge.src);
        output.getValueVectorMutable(2).setValue(i, edge.dst);
        output.getValueVectorMutable(3).setValue(i, edge.srcLocal);
        output.getValueVectorMutable(4).setValue(i, edge.dstLocal);
    }
    return numRows;
}

std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*const_cast<main::ClientContext*>(context), graphName);
    auto seeds = parseSeedArgument(input->getValue(1), graphEntry);
    if (seeds.empty()) {
        throw BinderException("Seed argument must contain at least one node.");
    }
    NeighborSamplingConfig config;
    if (input->params.size() > 3) {
        applyOptions(input->getValue(3), config);
    }
    config.hopFanouts = parseFanouts(input->getValue(2), graphEntry, config.defaultDirection);
    auto samplingResult = runSampling(context, graphEntry, seeds, config);
    std::vector<std::string> columnNames{"hop", "src", "dst", "src_local_id", "dst_local_id"};
    std::vector<LogicalType> columnTypes;
    columnTypes.reserve(5);
    columnTypes.push_back(LogicalType::INT32());
    columnTypes.push_back(LogicalType::INTERNAL_ID());
    columnTypes.push_back(LogicalType::INTERNAL_ID());
    columnTypes.push_back(LogicalType::UINT64());
    columnTypes.push_back(LogicalType::UINT64());
    columnNames = TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<NeighborSampleBindData>(graphEntry, config, std::move(seeds),
        std::move(samplingResult), std::move(columns));
}

} // namespace

function_set NeighborSampleFunction::getFunctionSet() {
    function_set set;
    auto makeFunction = [&](std::vector<LogicalTypeID> params) {
        auto func = std::make_unique<TableFunction>(name, std::move(params));
        func->bindFunc = bindFunc;
        func->tableFunc = SimpleTableFunc::getTableFunc(tableFunc);
        func->initSharedStateFunc = SimpleTableFunc::initSharedState;
        func->initLocalStateFunc = TableFunction::initEmptyLocalState;
        func->canParallelFunc = []() { return true; };
        return func;
    };
    set.push_back(makeFunction({LogicalTypeID::STRING, LogicalTypeID::ANY, LogicalTypeID::ANY}));
    set.push_back(makeFunction({LogicalTypeID::STRING, LogicalTypeID::ANY, LogicalTypeID::ANY,
        LogicalTypeID::ANY}));
    return set;
}

} // namespace function
} // namespace monad
