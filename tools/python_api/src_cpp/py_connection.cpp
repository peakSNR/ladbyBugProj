#include "include/py_connection.h"

#include <algorithm>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cached_import/py_cached_import.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "common/constants.h"
#include "common/enums/extend_direction_util.h"
#include "common/exception/not_implemented.h"
#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "common/types/uuid.h"
#include "common/types/date_t.h"
#include "common/types/dtime_t.h"
#include "common/utils.h"
#include "datetime.h" // from Python
#include "function/cast/functions/cast_string_non_nested_functions.h"
#include "function/gds/gds.h"
#include "graph/neighbor_sampler.h"
#include "graph/on_disk_graph.h"
#include "include/py_udf.h"
#include "main/connection.h"
#include "main/query_result/materialized_query_result.h"
#include "pybind11/numpy.h"
#include "pandas/pandas_scan.h"
#include "processor/result/factorized_table.h"
#include "pyarrow/pyarrow_scan.h"
#include "transaction/transaction_context.h"

using namespace monad::common;
using namespace monad;
using namespace monad::graph;

void PyConnection::initialize(py::handle& m) {
    py::class_<PyConnection>(m, "Connection")
        .def(py::init<PyDatabase*, uint64_t>(), py::arg("database"), py::arg("num_threads") = 0)
        .def("close", &PyConnection::close)
        .def("execute", &PyConnection::execute, py::arg("prepared_statement"),
            py::arg("parameters") = py::dict())
        .def("query", &PyConnection::query, py::arg("statement"))
        .def("set_max_threads_for_exec", &PyConnection::setMaxNumThreadForExec,
            py::arg("num_threads"))
        .def("prepare", &PyConnection::prepare, py::arg("query"),
            py::arg("parameters") = py::dict())
        .def("set_query_timeout", &PyConnection::setQueryTimeout, py::arg("timeout_in_ms"))
        .def("interrupt", &PyConnection::interrupt)
        .def("get_num_nodes", &PyConnection::getNumNodes, py::arg("node_name"))
        .def("get_num_rels", &PyConnection::getNumRels, py::arg("rel_name"))
        .def("get_all_edges_for_torch_geometric", &PyConnection::getAllEdgesForTorchGeometric,
            py::arg("np_array"), py::arg("src_table_name"), py::arg("rel_name"),
            py::arg("dst_table_name"), py::arg("query_batch_size"))
        .def("get_graph_relinfo", &PyConnection::getGraphRelInfo, py::arg("graph_name"))
        .def("neighbor_sample", &PyConnection::neighborSample, py::arg("graph_name"),
            py::arg("seed_offsets"), py::arg("fanouts"), py::arg("replace") = false,
            py::arg("direction") = "fwd", py::arg("seed") = py::none(), py::arg("time") = py::none(),
            py::arg("time_fields") = py::none())
        .def("create_function", &PyConnection::createScalarFunction, py::arg("name"),
            py::arg("udf"), py::arg("params_type"), py::arg("return_value"),
            py::arg("default_null"), py::arg("catch_exceptions"))
        .def("remove_function", &PyConnection::removeScalarFunction, py::arg("name"));
    PyDateTime_IMPORT;
}

static std::vector<function::scan_replace_handle_t> lookupPythonObject(
    const std::string& objectName) {
    std::vector<function::scan_replace_handle_t> ret;

    py::gil_scoped_acquire acquire;
    auto pyTableName = py::str(objectName);
    // Here we do an exhaustive search on the frame lineage.
    auto currentFrame = importCache->inspect.currentframe()();
    while (hasattr(currentFrame, "f_locals")) {
        auto localDict = py::cast<py::dict>(currentFrame.attr("f_locals"));
        auto hasLocalDict = !py::none().is(localDict);
        if (hasLocalDict) {
            if (localDict.contains(pyTableName)) {
                ret.push_back(reinterpret_cast<function::scan_replace_handle_t>(
                    localDict[pyTableName].ptr()));
            }
        }
        auto globalDict = py::reinterpret_borrow<py::dict>(currentFrame.attr("f_globals"));
        if (globalDict) {
            if (globalDict.contains(pyTableName)) {
                ret.push_back(reinterpret_cast<function::scan_replace_handle_t>(
                    globalDict[pyTableName].ptr()));
            }
        }
        currentFrame = currentFrame.attr("f_back");
    }
    return ret;
}

static std::unique_ptr<function::ScanReplacementData> tryReplacePolars(py::handle& entry) {
    if (PyConnection::isPolarsDataframe(entry)) {
        auto scanReplacementData = std::make_unique<function::ScanReplacementData>();
        scanReplacementData->func = PyArrowTableScanFunction::getFunction();
        auto bindInput = function::TableFuncBindInput();
        bindInput.addLiteralParam(Value::createValue(reinterpret_cast<uint8_t*>(entry.ptr())));
        scanReplacementData->bindInput = std::move(bindInput);
        return scanReplacementData;
    } else {
        return nullptr;
    }
}

static std::unique_ptr<function::ScanReplacementData> tryReplacePyArrow(py::handle& entry) {
    if (PyConnection::isPyArrowTable(entry)) {
        auto scanReplacementData = std::make_unique<function::ScanReplacementData>();
        scanReplacementData->func = PyArrowTableScanFunction::getFunction();
        auto bindInput = function::TableFuncBindInput();
        bindInput.addLiteralParam(Value::createValue(reinterpret_cast<uint8_t*>(entry.ptr())));
        scanReplacementData->bindInput = std::move(bindInput);
        return scanReplacementData;
    } else {
        return nullptr;
    }
}

static std::unique_ptr<function::ScanReplacementData> replacePythonObject(
    std::span<function::scan_replace_handle_t> candidateHandles) {
    py::gil_scoped_acquire acquire;
    for (auto* handle : candidateHandles) {
        auto entry = py::handle(reinterpret_cast<PyObject*>(handle));
        auto result = tryReplacePD(entry);
        if (!result) {
            result = tryReplacePolars(entry);
        }
        if (!result) {
            result = tryReplacePyArrow(entry);
        }
        if (result) {
            return result;
        }
    }
    if (!candidateHandles.empty()) {
        throw BinderException("Attempted to scan from unsupported python object. Can only scan "
                              "from pandas/polars dataframes and pyarrow tables.");
    }
    return nullptr;
}

namespace {

using NodeTableMap = std::unordered_map<std::string, table_id_t>;
using RelTableMap = std::unordered_map<std::string, std::vector<oid_t>>;

NodeTableMap buildNodeTableMap(const NativeGraphEntry& entry) {
    NodeTableMap map;
    for (const auto& info : entry.nodeInfos) {
        map.emplace(monad::common::StringUtils::getLower(info.entry->getName()),
            info.entry->getTableID());
    }
    return map;
}

RelTableMap buildRelTableMap(const NativeGraphEntry& entry) {
    RelTableMap map;
    for (const auto& info : entry.relInfos) {
        std::vector<oid_t> oids;
        auto* relGroupEntry = info.entry->ptrCast<catalog::RelGroupCatalogEntry>();
        for (const auto& relInfo : relGroupEntry->getRelEntryInfos()) {
            oids.push_back(relInfo.oid);
        }
        map.emplace(monad::common::StringUtils::getLower(info.entry->getName()), std::move(oids));
    }
    return map;
}

void appendOffsetsFromObject(const py::handle& obj, table_id_t tableID,
    std::vector<nodeID_t>& output) {
    if (py::isinstance<py::str>(obj)) {
        throw std::runtime_error("Seed offsets must not be strings.");
    }
    if (py::isinstance<py::array>(obj)) {
        py::array_t<int64_t, py::array::c_style | py::array::forcecast> arr(
            py::reinterpret_borrow<py::object>(obj));
        auto buffer = arr.request();
        auto* data = static_cast<int64_t*>(buffer.ptr);
        for (py::ssize_t i = 0; i < buffer.size; ++i) {
            if (data[i] < 0) {
                throw std::runtime_error("Seed offsets must be non-negative.");
            }
            output.push_back(nodeID_t{static_cast<offset_t>(data[i]), tableID});
        }
        return;
    }
    if (py::isinstance<py::sequence>(obj)) {
        py::sequence seq = obj.cast<py::sequence>();
        for (const auto& item : seq) {
            auto offset = py::cast<int64_t>(item);
            if (offset < 0) {
                throw std::runtime_error("Seed offsets must be non-negative.");
            }
            output.push_back(nodeID_t{static_cast<offset_t>(offset), tableID});
        }
        return;
    }
    throw std::runtime_error("Seed values must be numpy arrays or sequences.");
}

std::vector<nodeID_t> buildSeedVector(const py::dict& seeds, const NativeGraphEntry& entry) {
    auto tableMap = buildNodeTableMap(entry);
    std::vector<nodeID_t> result;
    for (auto item : seeds) {
        auto key = py::cast<std::string>(item.first);
        auto normalized = monad::common::StringUtils::getLower(key);
        if (!tableMap.contains(normalized)) {
            throw std::runtime_error("Seed dictionary contains unknown node table: " + key);
        }
        appendOffsetsFromObject(item.second, tableMap.at(normalized), result);
    }
    return result;
}

HopFanout parseHopFanoutPy(const py::handle& obj, const RelTableMap& relTableMap,
    ExtendDirection defaultDirection) {
    HopFanout hop;
    hop.direction = defaultDirection;
    hop.defaultFanout = -1;
    if (py::isinstance<py::int_>(obj)) {
        auto fanout = py::cast<int64_t>(obj);
        if (fanout < -1) {
            throw std::runtime_error("Fanout values must be >= -1.");
        }
        hop.defaultFanout = fanout;
        return hop;
    }
    if (!py::isinstance<py::dict>(obj)) {
        throw std::runtime_error("Fanout list entries must be integers or dicts.");
    }
    py::dict dict = py::reinterpret_borrow<py::dict>(obj);
    for (auto item : dict) {
        auto key = monad::common::StringUtils::getLower(py::cast<std::string>(item.first));
        if (key == "direction") {
            hop.direction = ExtendDirectionUtil::fromString(py::cast<std::string>(item.second));
        } else if (key == "fanout" || key == "default") {
            auto fanout = py::cast<int64_t>(item.second);
            if (fanout < -1) {
                throw std::runtime_error("Fanout values must be >= -1.");
            }
            hop.defaultFanout = fanout;
        } else if (key == "rels") {
            if (!py::isinstance<py::dict>(item.second)) {
                throw std::runtime_error("rels overrides must be a dict.");
            }
            py::dict rels = py::reinterpret_borrow<py::dict>(item.second);
            for (auto relItem : rels) {
                auto relName = monad::common::StringUtils::getLower(py::cast<std::string>(relItem.first));
                if (!relTableMap.contains(relName)) {
                    throw std::runtime_error("Unknown relationship table in rels override: " +
                                            py::cast<std::string>(relItem.first));
                }
                auto relFanout = py::cast<int64_t>(relItem.second);
                if (relFanout < -1) {
                    throw std::runtime_error("Fanout values must be >= -1.");
                }
                for (auto oid : relTableMap.at(relName)) {
                    hop.relFanouts.emplace(oid, relFanout);
                }
            }
        } else {
            throw std::runtime_error("Unknown fanout option: " + py::cast<std::string>(item.first));
        }
    }
    return hop;
}

std::vector<HopFanout> buildHopFanouts(const py::sequence& seq, const NativeGraphEntry& entry,
    ExtendDirection defaultDirection) {
    auto relTableMap = buildRelTableMap(entry);
    std::vector<HopFanout> hopFanouts;
    hopFanouts.reserve(seq.size());
    for (const auto& item : seq) {
        hopFanouts.push_back(parseHopFanoutPy(item, relTableMap, defaultDirection));
    }
    return hopFanouts;
}

py::dict convertSamplingResultToDict(const NeighborSamplingResult& result) {
    py::dict pyResult;
    auto numNodes = result.nodes.size();
    py::array_t<int64_t> nodeOffsets(numNodes);
    py::array_t<int64_t> nodeTableIDs(numNodes);
    py::array_t<int32_t> nodeHops(numNodes);
    auto* offsetPtr = nodeOffsets.mutable_data();
    auto* tablePtr = nodeTableIDs.mutable_data();
    auto* hopPtr = nodeHops.mutable_data();
    for (size_t i = 0; i < numNodes; ++i) {
        offsetPtr[i] = static_cast<int64_t>(result.nodes[i].node.offset);
        tablePtr[i] = static_cast<int64_t>(result.nodes[i].node.tableID);
        hopPtr[i] = static_cast<int32_t>(result.nodes[i].hop);
    }

    auto numEdges = result.edges.size();
    py::array_t<int64_t> srcLocal(numEdges);
    py::array_t<int64_t> dstLocal(numEdges);
    py::array_t<int32_t> edgeHops(numEdges);
    py::array_t<int64_t> relTableIDs(numEdges);
    auto* srcPtr = srcLocal.mutable_data();
    auto* dstPtr = dstLocal.mutable_data();
    auto* edgeHopPtr = edgeHops.mutable_data();
    auto* relPtr = relTableIDs.mutable_data();
    for (size_t i = 0; i < numEdges; ++i) {
        srcPtr[i] = static_cast<int64_t>(result.edges[i].srcLocal);
        dstPtr[i] = static_cast<int64_t>(result.edges[i].dstLocal);
        edgeHopPtr[i] = static_cast<int32_t>(result.edges[i].hop);
        relPtr[i] = static_cast<int64_t>(result.edges[i].relTableID);
    }

    py::array_t<uint64_t> nodesPerHop(result.numSampledNodesPerHop.size());
    py::array_t<uint64_t> edgesPerHop(result.numSampledEdgesPerHop.size());
    std::copy(result.numSampledNodesPerHop.begin(), result.numSampledNodesPerHop.end(),
        nodesPerHop.mutable_data());
    std::copy(result.numSampledEdgesPerHop.begin(), result.numSampledEdgesPerHop.end(),
        edgesPerHop.mutable_data());

    pyResult["node_offsets"] = std::move(nodeOffsets);
    pyResult["node_table_ids"] = std::move(nodeTableIDs);
    pyResult["node_hops"] = std::move(nodeHops);
    pyResult["src_local_ids"] = std::move(srcLocal);
    pyResult["dst_local_ids"] = std::move(dstLocal);
    pyResult["edge_hops"] = std::move(edgeHops);
    pyResult["rel_table_ids"] = std::move(relTableIDs);
    pyResult["num_sampled_nodes_per_hop"] = std::move(nodesPerHop);
    pyResult["num_sampled_edges_per_hop"] = std::move(edgesPerHop);
    return pyResult;
}

std::optional<timestamp_t> parsePyTimestamp(const py::object& obj) {
    if (obj.is_none()) {
        return std::nullopt;
    }
    if (PyDateTime_Check(obj.ptr())) {
        auto pyObj = obj;
        auto year = PyDateTime_GET_YEAR(pyObj.ptr());
        auto month = PyDateTime_GET_MONTH(pyObj.ptr());
        auto day = PyDateTime_GET_DAY(pyObj.ptr());
        auto hour = PyDateTime_DATE_GET_HOUR(pyObj.ptr());
        auto minute = PyDateTime_DATE_GET_MINUTE(pyObj.ptr());
        auto second = PyDateTime_DATE_GET_SECOND(pyObj.ptr());
        auto micros = PyDateTime_DATE_GET_MICROSECOND(pyObj.ptr());
        auto date = Date::fromDate(year, month, day);
        auto time = Time::fromTime(hour, minute, second, micros);
        return Timestamp::fromDateTime(date, time);
    }
    if (PyDate_Check(obj.ptr())) {
        auto year = PyDateTime_GET_YEAR(obj.ptr());
        auto month = PyDateTime_GET_MONTH(obj.ptr());
        auto day = PyDateTime_GET_DAY(obj.ptr());
        auto date = Date::fromDate(year, month, day);
        return Timestamp::fromDateTime(date, dtime_t{0});
    }
    if (py::isinstance<py::str>(obj)) {
        auto s = obj.cast<std::string>();
        timestamp_t ts;
        if (!Timestamp::tryConvertTimestamp(s.c_str(), s.size(), ts)) {
            throw std::runtime_error("Failed to parse time string; expected TIMESTAMP-compatible literal.");
        }
        return ts;
    }
    if (py::isinstance<py::int_>(obj)) {
        auto v = obj.cast<int64_t>();
        return Timestamp::fromEpochSeconds(v);
    }
    if (py::isinstance<py::float_>(obj)) {
        auto v = obj.cast<double>();
        return Timestamp::fromEpochSeconds(static_cast<int64_t>(v));
    }
    throw std::runtime_error("Unsupported time value; provide datetime/date/string or epoch seconds.");
}

void parseTimeFieldsPy(const py::object& obj, std::vector<std::string>& starts,
    std::vector<std::string>& ends) {
    if (obj.is_none()) {
        return;
    }
    if (!py::isinstance<py::dict>(obj)) {
        throw std::runtime_error("time_fields must be a dict with start/end keys.");
    }
    py::dict d = py::reinterpret_borrow<py::dict>(obj);
    auto lower = [](const std::string& s) { return monad::common::StringUtils::getLower(s); };
    for (auto item : d) {
        auto key = lower(py::cast<std::string>(item.first));
        auto val = item.second;
        auto addList = [&](std::vector<std::string>& target) {
            if (py::isinstance<py::str>(val)) {
                target.push_back(lower(py::cast<std::string>(val)));
            } else if (py::isinstance<py::list>(val) || py::isinstance<py::tuple>(val)) {
                py::sequence seq = py::reinterpret_borrow<py::sequence>(val);
                for (auto v : seq) {
                    if (!py::isinstance<py::str>(v)) {
                        throw std::runtime_error("time_fields entries must be strings.");
                    }
                    target.push_back(lower(py::cast<std::string>(v)));
                }
            } else {
                throw std::runtime_error("time_fields start/end must be string or list of strings.");
            }
        };
        if (key == "start") {
            addList(starts);
        } else if (key == "end") {
            addList(ends);
        } else {
            throw std::runtime_error("time_fields supports only start and end keys.");
        }
    }
}

py::list buildGraphRelInfoList(const graph::NativeGraphEntry& entry) {
    std::unordered_map<table_id_t, std::string> nodeNames;
    for (const auto& n : entry.nodeInfos) {
        nodeNames.emplace(n.entry->getTableID(), n.entry->getName());
    }
    py::list rels;
    for (const auto& info : entry.relInfos) {
        auto* relGroupEntry = info.entry->ptrCast<catalog::RelGroupCatalogEntry>();
        for (const auto& relInfo : relGroupEntry->getRelEntryInfos()) {
            auto srcName = nodeNames.at(relInfo.nodePair.srcTableID);
            auto dstName = nodeNames.at(relInfo.nodePair.dstTableID);
            py::dict item;
            item["rel_table_id"] = py::int_(relInfo.oid);
            item["name"] = py::str(info.entry->getName());
            item["src"] = py::str(srcName);
            item["dst"] = py::str(dstName);
            rels.append(std::move(item));
        }
    }
    return rels;
}

struct AutoReadTransaction {
    explicit AutoReadTransaction(main::ClientContext* context)
        : txContext{transaction::TransactionContext::Get(*context)} {
        ownsTransaction = !txContext->hasActiveTransaction();
        if (ownsTransaction) {
            txContext->beginAutoTransaction(true /* readOnly */);
        }
    }

    ~AutoReadTransaction() {
        if (ownsTransaction) {
            txContext->commit();
        }
    }

private:
    transaction::TransactionContext* txContext;
    bool ownsTransaction;
};

} // namespace

PyConnection::PyConnection(PyDatabase* pyDatabase, uint64_t numThreads) {
    storageDriver = std::make_unique<monad::main::StorageDriver>(pyDatabase->database.get());
    conn = std::make_unique<Connection>(pyDatabase->database.get());
    conn->getClientContext()->addScanReplace(
        function::ScanReplacement(lookupPythonObject, replacePythonObject));
    if (numThreads > 0) {
        conn->setMaxNumThreadForExec(numThreads);
    }
}

PyConnection::~PyConnection() {
    close();
}

void PyConnection::close() {
    conn.reset();
}

void PyConnection::setQueryTimeout(uint64_t timeoutInMS) {
    conn->setQueryTimeOut(timeoutInMS);
}

void PyConnection::interrupt() {
    conn->interrupt();
}

static std::unordered_map<std::string, std::unique_ptr<Value>> transformPythonParameters(
    const py::dict& params, Connection* conn);

std::unique_ptr<PyQueryResult> PyConnection::execute(PyPreparedStatement* preparedStatement,
    const py::dict& params) {
    auto parameters = transformPythonParameters(params, conn.get());
    py::gil_scoped_release release;
    auto queryResult =
        conn->executeWithParams(preparedStatement->preparedStatement.get(), std::move(parameters));
    py::gil_scoped_acquire acquire;
    return checkAndWrapQueryResult(queryResult);
}

std::unique_ptr<PyQueryResult> PyConnection::query(const std::string& statement) {
    py::gil_scoped_release release;
    auto queryResult = conn->query(statement);
    py::gil_scoped_acquire acquire;
    return checkAndWrapQueryResult(queryResult);
}

void PyConnection::setMaxNumThreadForExec(uint64_t numThreads) {
    conn->setMaxNumThreadForExec(numThreads);
}

PyPreparedStatement PyConnection::prepare(const std::string& query, const py::dict& parameters) {
    auto params = transformPythonParameters(parameters, conn.get());
    auto preparedStatement = conn->prepareWithParams(query, std::move(params));
    PyPreparedStatement pyPreparedStatement;
    pyPreparedStatement.preparedStatement = std::move(preparedStatement);
    return pyPreparedStatement;
}

uint64_t PyConnection::getNumNodes(const std::string& nodeName) {
    return storageDriver->getNumNodes(nodeName);
}

uint64_t PyConnection::getNumRels(const std::string& relName) {
    return storageDriver->getNumRels(relName);
}

void PyConnection::getAllEdgesForTorchGeometric(py::array_t<int64_t>& npArray,
    const std::string& srcTableName, const std::string& relName, const std::string& dstTableName,
    size_t queryBatchSize) {
    // Get the number of nodes in the dst table for batching.
    auto numDstNodes = getNumNodes(dstTableName);
    uint64_t batches = numDstNodes / queryBatchSize;
    if (numDstNodes % queryBatchSize != 0) {
        batches += 1;
    }
    auto numRels = getNumRels(relName);

    auto bufferInfo = npArray.request();
    auto buffer = (int64_t*)bufferInfo.ptr;

    // Set the number of threads to 1 for fetching edges to ensure ordering.
    auto numThreadsForExec = conn->getMaxNumThreadForExec();
    conn->setMaxNumThreadForExec(1);
    // Run queries in batch to fetch edges.
    auto queryString = "MATCH (a:{})-[:{}]->(b:{}) WHERE offset(id(b)) >= $s AND offset(id(b)) < "
                       "$e RETURN offset(id(a)), offset(id(b))";
    auto query = stringFormat(queryString, srcTableName, relName, dstTableName);
    auto preparedStatement = conn->prepare(query);
    auto srcBuffer = buffer;
    auto dstBuffer = buffer + numRels;
    for (uint64_t batch = 0; batch < batches; ++batch) {
        // Must be int64_t for parameter typing.
        int64_t start = batch * queryBatchSize;
        int64_t end = (batch + 1) * queryBatchSize;
        end = (uint64_t)end > numDstNodes ? numDstNodes : end;
        std::unordered_map<std::string, std::unique_ptr<Value>> parameters;
        parameters["s"] = std::make_unique<Value>(start);
        parameters["e"] = std::make_unique<Value>(end);
        auto result = conn->executeWithParams(preparedStatement.get(), std::move(parameters));
        if (!result->isSuccess()) {
            throw std::runtime_error(result->getErrorMessage());
        }
        KU_ASSERT(result->getType() == QueryResultType::FTABLE);
        auto& table = result->constCast<MaterializedQueryResult>().getFactorizedTable();
        auto tableSchema = table.getTableSchema();
        if (tableSchema->getColumn(0)->isFlat() && !tableSchema->getColumn(1)->isFlat()) {
            for (auto i = 0u; i < table.getNumTuples(); ++i) {
                auto tuple = table.getTuple(i);
                auto overflowValue = (overflow_value_t*)(tuple + tableSchema->getColOffset(1));
                for (auto j = 0u; j < overflowValue->numElements; ++j) {
                    srcBuffer[j] = *(int64_t*)(tuple + tableSchema->getColOffset(0));
                }
                for (auto j = 0u; j < overflowValue->numElements; ++j) {
                    dstBuffer[j] = ((int64_t*)overflowValue->value)[j];
                }
                srcBuffer += overflowValue->numElements;
                dstBuffer += overflowValue->numElements;
            }
        } else if (tableSchema->getColumn(1)->isFlat() && !tableSchema->getColumn(0)->isFlat()) {
            for (auto i = 0u; i < table.getNumTuples(); ++i) {
                auto tuple = table.getTuple(i);
                auto overflowValue = (overflow_value_t*)(tuple + tableSchema->getColOffset(0));
                for (auto j = 0u; j < overflowValue->numElements; ++j) {
                    srcBuffer[j] = ((int64_t*)overflowValue->value)[j];
                }
                for (auto j = 0u; j < overflowValue->numElements; ++j) {
                    dstBuffer[j] = *(int64_t*)(tuple + tableSchema->getColOffset(1));
                }
                srcBuffer += overflowValue->numElements;
                dstBuffer += overflowValue->numElements;
            }
        } else {
            throw std::runtime_error("Wrong result table schema.");
        }
    }
    conn->setMaxNumThreadForExec(numThreadsForExec);
}

py::dict PyConnection::neighborSample(const std::string& graphName, const py::dict& seedOffsets,
    const py::sequence& fanouts, bool replace, const std::string& direction,
    const std::optional<int64_t>& randomSeed, py::object timeObj, py::object timeFieldsObj) {
    if (seedOffsets.empty()) {
        throw std::runtime_error("seed_offsets dictionary must not be empty.");
    }
    auto clientContext = conn->getClientContext();
    AutoReadTransaction txnGuard(clientContext);
    auto graphEntry = function::GDSFunction::bindGraphEntry(*clientContext, graphName);
    auto seeds = buildSeedVector(seedOffsets, graphEntry);
    if (seeds.empty()) {
        throw std::runtime_error("seed_offsets must contain at least one node.");
    }
    NeighborSamplingConfig config;
    config.defaultDirection = ExtendDirectionUtil::fromString(direction);
    config.hopFanouts = buildHopFanouts(fanouts, graphEntry, config.defaultDirection);
    config.replace = replace;
    if (randomSeed.has_value()) {
        if (randomSeed.value() < 0) {
            throw std::runtime_error("seed must be non-negative.");
        }
        config.randomSeed = static_cast<uint64_t>(randomSeed.value());
    }
    auto asOfTs = parsePyTimestamp(timeObj);
    if (asOfTs.has_value()) {
        config.asOf = asOfTs.value();
    }

    std::vector<std::string> starts, ends;
    parseTimeFieldsPy(timeFieldsObj, starts, ends);
    if (!starts.empty()) {
        config.temporalStartFields = std::move(starts);
    }
    if (!ends.empty()) {
        config.temporalEndFields = std::move(ends);
    }

    std::shared_ptr<OnDiskGraph> graph;
    {
        std::lock_guard<std::mutex> lk(cacheMutex);
        auto graphIt = graphCache.find(graphName);
        if (graphIt != graphCache.end()) {
            graph = graphIt->second;
        } else {
            graph = std::make_shared<OnDiskGraph>(clientContext, graphEntry.copy());
            graphCache.emplace(graphName, graph);
        }
    }

    NeighborSampler sampler(*graph, config);
    sampler.preloadScanners();
    py::gil_scoped_release release;
    auto samplingResult = sampler.run(NeighborSamplingInput{seeds});
    py::gil_scoped_acquire acquire;
    return convertSamplingResultToDict(samplingResult);
}

py::list PyConnection::getGraphRelInfo(const std::string& graphName) {
    auto clientContext = conn->getClientContext();
    AutoReadTransaction txnGuard(clientContext);
    auto graphEntry = function::GDSFunction::bindGraphEntry(*clientContext, graphName);
    return buildGraphRelInfoList(graphEntry);
}

bool PyConnection::isPandasDataframe(const py::handle& object) {
    if (!doesPyModuleExist("pandas")) {
        return false;
    }
    return py::isinstance(object, importCache->pandas.DataFrame());
}

bool PyConnection::isPolarsDataframe(const py::handle& object) {
    if (!doesPyModuleExist("polars")) {
        return false;
    }
    return py::isinstance(object, importCache->polars.DataFrame());
}

bool PyConnection::isPyArrowTable(const py::handle& object) {
    if (!doesPyModuleExist("pyarrow")) {
        return false;
    }
    return py::isinstance(object, importCache->pyarrow.lib.Table());
}

static std::unordered_map<std::string, std::unique_ptr<Value>> transformPythonParameters(
    const py::dict& params, Connection* conn) {
    std::unordered_map<std::string, std::unique_ptr<Value>> result;
    for (auto& [key, value] : params) {
        if (!py::isinstance<py::str>(key)) {
            // TODO(Chang): remove ROLLBACK once we can guarantee database is deleted after conn
            conn->query("ROLLBACK");
            throw std::runtime_error("Parameter name must be of type string but got " +
                                     py::str(key.get_type()).cast<std::string>());
        }
        auto name = key.cast<std::string>();
        auto val = std::make_unique<Value>(PyConnection::transformPythonValueFromParameter(value));
        result.insert({name, std::move(val)});
    }
    return result;
}

template<typename T>
bool integerFitsIn(int64_t val);

template<>
bool integerFitsIn<int64_t>(int64_t) {
    return true;
}

template<>
bool integerFitsIn<int32_t>(int64_t val) {
    return val >= INT32_MIN && val <= INT32_MAX;
}

template<>
bool integerFitsIn<int16_t>(int64_t val) {
    return val >= INT16_MIN && val <= INT16_MAX;
}

template<>
bool integerFitsIn<int8_t>(int64_t val) {
    return val >= INT8_MIN && val <= INT8_MAX;
}

template<>
bool integerFitsIn<uint64_t>(int64_t val) {
    return val >= 0;
}

template<>
bool integerFitsIn<uint32_t>(int64_t val) {
    return val >= 0 && val <= UINT32_MAX;
}

template<>
bool integerFitsIn<uint16_t>(int64_t val) {
    return val >= 0 && val <= UINT16_MAX;
}

template<>
bool integerFitsIn<uint8_t>(int64_t val) {
    return val >= 0 && val <= UINT8_MAX;
}

static LogicalType pyLogicalType(const py::handle& val) {
    auto datetime_datetime = importCache->datetime.datetime();
    auto time_delta = importCache->datetime.timedelta();
    auto datetime_date = importCache->datetime.date();
    auto uuid = importCache->uuid.UUID();
    auto Decimal = importCache->decimal.Decimal();
    if (val.is_none()) {
        return LogicalType::ANY();
    } else if (py::isinstance<py::bool_>(val)) {
        return LogicalType::BOOL();
    } else if (py::isinstance<py::int_>(val)) {
        auto nativeValue = val.cast<int64_t>();
        if (integerFitsIn<int8_t>(nativeValue)) {
            return LogicalType::INT8();
        } else if (integerFitsIn<uint8_t>(nativeValue)) {
            return LogicalType::UINT8();
        } else if (integerFitsIn<int16_t>(nativeValue)) {
            return LogicalType::INT16();
        } else if (integerFitsIn<uint16_t>(nativeValue)) {
            return LogicalType::UINT16();
        } else if (integerFitsIn<int32_t>(nativeValue)) {
            return LogicalType::INT32();
        } else if (integerFitsIn<uint32_t>(nativeValue)) {
            return LogicalType::UINT32();
        } else {
            return LogicalType::INT64();
        }
    } else if (py::isinstance<py::float_>(val)) {
        return LogicalType::DOUBLE();
    } else if (py::isinstance(val, Decimal)) {
        auto as_tuple = val.attr("as_tuple")();
        auto precision = py::len(as_tuple.attr("digits"));
        auto exponent = py::cast<int32_t>(as_tuple.attr("exponent"));
        if (exponent > 0) {
            precision += exponent;
            exponent = 0;
        }
        if (precision > monad::common::DECIMAL_PRECISION_LIMIT) {
            throw monad::common::NotImplementedException(
                stringFormat("Decimal precision cannot be greater than {}"
                             "Note: positive exponents contribute to precision",
                    monad::common::DECIMAL_PRECISION_LIMIT));
        }
        return LogicalType::DECIMAL(precision, -exponent);
    } else if (py::isinstance<py::str>(val)) {
        return LogicalType::STRING();
    } else if (py::isinstance<py::bytes>(val)) {
        return LogicalType::BLOB();
    } else if (py::isinstance(val, datetime_datetime)) {
        return LogicalType::TIMESTAMP();
    } else if (py::isinstance(val, datetime_date)) {
        return LogicalType::DATE();
    } else if (py::isinstance(val, time_delta)) {
        return LogicalType::INTERVAL();
    } else if (py::isinstance(val, uuid)) {
        return LogicalType::UUID();
    } else if (py::isinstance<py::dict>(val)) {
        py::dict dict = py::reinterpret_borrow<py::dict>(val);
        auto childKeyType = LogicalType::ANY(), childValueType = LogicalType::ANY();
        for (auto child : dict) {
            auto curChildKeyType = pyLogicalType(child.first),
                 curChildValueType = pyLogicalType(child.second);
            LogicalType resultKey, resultValue;
            if (!LogicalTypeUtils::tryGetMaxLogicalType(childKeyType, curChildKeyType, resultKey)) {
                throw RuntimeException(stringFormat(
                    "Cannot convert Python object to Monad value : {}  is incompatible with {}",
                    childKeyType.toString(), curChildKeyType.toString()));
            }
            if (!LogicalTypeUtils::tryGetMaxLogicalType(childValueType, curChildValueType,
                    resultValue)) {
                throw RuntimeException(stringFormat(
                    "Cannot convert Python object to Monad value : {}  is incompatible with {}",
                    childValueType.toString(), curChildValueType.toString()));
            }
            childKeyType = std::move(resultKey);
            childValueType = std::move(resultValue);
        }
        return LogicalType::MAP(std::move(childKeyType), std::move(childValueType));
    } else if (py::isinstance<py::list>(val)) {
        py::list lst = py::reinterpret_borrow<py::list>(val);
        auto childType = LogicalType::ANY();
        for (auto child : lst) {
            auto curChildType = pyLogicalType(child);
            LogicalType result;
            if (!LogicalTypeUtils::tryGetMaxLogicalType(childType, curChildType, result)) {
                throw RuntimeException(stringFormat(
                    "Cannot convert Python object to Monad value : {}  is incompatible with {}",
                    childType.toString(), curChildType.toString()));
            }
            childType = std::move(result);
        }
        return LogicalType::LIST(std::move(childType));
    } else if (PyConnection::isPyArrowTable(val) || PyConnection::isPandasDataframe(val) ||
               PyConnection::isPolarsDataframe(val)) {
        return LogicalType::POINTER();
    } else {
        // LCOV_EXCL_START
        throw monad::common::RuntimeException(
            "Unknown parameter type " + py::str(val.get_type()).cast<std::string>());
        // LCOV_EXCL_STOP
    }
}

static bool validateMapFields(py::dict& dict) {
    for (auto& field : dict) {
        auto keyType = pyLogicalType(field.first).getLogicalTypeID();
        if (keyType != LogicalTypeID::STRING) {
            return false;
        }
        auto valType = pyLogicalType(field.second).getLogicalTypeID();
        if (valType != LogicalTypeID::LIST) {
            return false;
        }
    }
    std::string keyName = py::str(dict.begin()->first);
    if (keyName != "key") {
        return false;
    }
    std::string valueName = py::str((++dict.begin())->first);
    if (valueName != "value") {
        return false;
    }
    return true;
}

static LogicalType pyLogicalTypeFromParameter(const py::handle& val);

// If we want to interpret a python dict as MAP, it has to satisfy the following two conditions:
// 1. The dictionary has only two fields.
// 2. The first field name is "key", while the second field name is "value".
// 3. Values of both first and second fields are list of values with the same type.
// Sample:
// my_map_dict = {
//    "key": [
//        1, 2, 3
//    ],
//    "value": [
//        "one", "two", "three"
//    ]
//  }
static bool tryCastToMap(py::dict& dict, LogicalType& result) {
    if (dict.size() != 2) {
        return false;
    }
    if (!validateMapFields(dict)) {
        return false;
    }
    auto keyList = dict.begin()->second;
    auto valList = (++dict.begin())->second;
    if (py::reinterpret_borrow<py::list>(keyList).size() !=
        py::reinterpret_borrow<py::list>(valList).size()) {
        return false;
    }
    auto keyListType = pyLogicalTypeFromParameter(keyList);
    auto valListType = pyLogicalTypeFromParameter(valList);
    result = LogicalType::MAP(ListType::getChildType(keyListType).copy(),
        ListType::getChildType(valListType).copy());
    return true;
}

static LogicalType pyLogicalTypeFromParameter(const py::handle& val) {
    if (py::isinstance<py::dict>(val)) {
        auto dict = py::reinterpret_borrow<py::dict>(val);
        LogicalType resultType;
        if (tryCastToMap(dict, resultType)) {
            return resultType;
        }
        auto structFields = std::vector<StructField>{};
        for (auto child : dict) {
            auto keyName = py::cast<std::string>(py::str(child.first));
            auto keyType = pyLogicalTypeFromParameter(child.second);
            structFields.emplace_back(std::move(keyName), std::move(keyType));
        }
        return LogicalType::STRUCT(std::move(structFields));
    } else if (py::isinstance<py::list>(val)) {
        py::list lst = py::reinterpret_borrow<py::list>(val);
        auto childType = LogicalType::ANY();
        for (auto child : lst) {
            auto curChildType = pyLogicalTypeFromParameter(child);
            LogicalType result;
            if (!LogicalTypeUtils::tryGetMaxLogicalType(childType, curChildType, result)) {
                throw RuntimeException(stringFormat(
                    "Cannot convert Python object to Monad value : {}  is incompatible with {}",
                    childType.toString(), curChildType.toString()));
            }
            childType = std::move(result);
        }
        return LogicalType::LIST(std::move(childType));
    } else {
        return pyLogicalType(val);
    }
}

Value PyConnection::transformPythonValueAs(const py::handle& val, const LogicalType& type) {
    // ignore the type of the actual python object, just directly cast
    auto datetime_datetime = importCache->datetime.datetime();
    auto time_delta = importCache->datetime.timedelta();
    auto datetime_date = importCache->datetime.date();
    if (val.is_none()) {
        return Value::createNullValue(type);
    }
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::ANY:
        return Value::createNullValue();
    case LogicalTypeID::BOOL:
        return Value::createValue<bool>(py::cast<py::bool_>(val).cast<bool>());
    case LogicalTypeID::INT64:
        return Value::createValue<int64_t>(py::cast<py::int_>(val).cast<int64_t>());
    case LogicalTypeID::UINT32:
        return Value::createValue<uint32_t>(py::cast<py::int_>(val).cast<uint32_t>());
    case LogicalTypeID::INT32:
        return Value::createValue<int32_t>(py::cast<py::int_>(val).cast<int32_t>());
    case LogicalTypeID::UINT16:
        return Value::createValue<uint16_t>(py::cast<py::int_>(val).cast<uint16_t>());
    case LogicalTypeID::INT16:
        return Value::createValue<int16_t>(py::cast<py::int_>(val).cast<int16_t>());
    case LogicalTypeID::UINT8:
        return Value::createValue<uint8_t>(py::cast<py::int_>(val).cast<uint8_t>());
    case LogicalTypeID::INT8:
        return Value::createValue<int8_t>(py::cast<py::int_>(val).cast<int8_t>());
    case LogicalTypeID::DOUBLE:
        return Value::createValue<double>(py::cast<py::float_>(val).cast<double>());
    case LogicalTypeID::DECIMAL: {
        auto str = py::cast<std::string>(py::str(val));
        int128_t result = 0;
        function::decimalCast(str.c_str(), str.size(), result, type);
        auto val = Value::createDefaultValue(type);
        val.val.int128Val = result;
        return val;
    }
    case LogicalTypeID::STRING:
        if (py::isinstance<py::str>(val)) {
            return Value::createValue<std::string>(val.cast<std::string>());
        } else {
            return Value::createValue<std::string>(py::str(val));
        }
    case LogicalTypeID::BLOB: {
        auto bytes = py::cast<py::bytes>(val);
        const char* data = PyBytes_AsString(bytes.ptr());
        Py_ssize_t size = PyBytes_Size(bytes.ptr());
        std::string blobStr(data, size);
        return Value(LogicalType::BLOB(), blobStr);
    }
    case LogicalTypeID::TIMESTAMP: {
        // LCOV_EXCL_START
        if (!py::isinstance(val, datetime_datetime)) {
            throw RuntimeException("Error: parameter is not of type datetime.datetime, \
                but was resolved to type datetime.datetime");
        }
        // LCOV_EXCL_STOP
        auto ptr = val.ptr();
        auto year = PyDateTime_GET_YEAR(ptr);
        auto month = PyDateTime_GET_MONTH(ptr);
        auto day = PyDateTime_GET_DAY(ptr);
        auto hour = PyDateTime_DATE_GET_HOUR(ptr);
        auto minute = PyDateTime_DATE_GET_MINUTE(ptr);
        auto second = PyDateTime_DATE_GET_SECOND(ptr);
        auto micros = PyDateTime_DATE_GET_MICROSECOND(ptr);
        auto date = Date::fromDate(year, month, day);
        auto time = Time::fromTime(hour, minute, second, micros);
        return Value::createValue<timestamp_t>(Timestamp::fromDateTime(date, time));
    }
    case LogicalTypeID::DATE: {
        // LCOV_EXCL_START
        if (!py::isinstance(val, datetime_date)) {
            throw RuntimeException("Error: parameter is not of type datetime.date, \
                but was resolved to type datetime.date");
        }
        // LCOV_EXCL_STOP
        auto ptr = val.ptr();
        auto year = PyDateTime_GET_YEAR(ptr);
        auto month = PyDateTime_GET_MONTH(ptr);
        auto day = PyDateTime_GET_DAY(ptr);
        return Value::createValue<date_t>(Date::fromDate(year, month, day));
    }
    case LogicalTypeID::INTERVAL: {
        // LCOV_EXCL_START
        if (!py::isinstance(val, time_delta)) {
            throw RuntimeException("Error: parameter is not of type datetime.timedelta, \
                but was resolved to type datetime.timedelta");
        }
        // LCOV_EXCL_STOP
        auto ptr = val.ptr();
        auto days = PyDateTime_DELTA_GET_DAYS(ptr);
        auto seconds = PyDateTime_DELTA_GET_SECONDS(ptr);
        auto microseconds = PyDateTime_DELTA_GET_MICROSECONDS(ptr);
        interval_t interval;
        Interval::addition(interval, days, "days");
        Interval::addition(interval, seconds, "seconds");
        Interval::addition(interval, microseconds, "microseconds");
        return Value::createValue<interval_t>(interval);
    }
    case LogicalTypeID::UUID: {
        auto strVal = py::str(val).cast<std::string>();
        auto uuidVal = UUID::fromString(strVal);
        ku_uuid_t uuidToAppend{uuidVal};
        return Value{uuidToAppend};
    }
    case LogicalTypeID::LIST: {
        py::list lst = py::reinterpret_borrow<py::list>(val);
        std::vector<std::unique_ptr<Value>> children;
        for (auto child : lst) {
            children.push_back(std::make_unique<Value>(
                transformPythonValueAs(child, ListType::getChildType(type))));
        }
        return Value(type.copy(), std::move(children));
    }
    case LogicalTypeID::MAP: {
        py::dict dict = py::reinterpret_borrow<py::dict>(val);
        std::vector<std::unique_ptr<Value>> children;
        const auto& childKeyType = MapType::getKeyType(type);
        const auto& childValueType = MapType::getValueType(type);
        for (auto child : dict) {
            // type construction is inefficient, we have to create duplicates because it asks for
            // a unique ptr
            std::vector<StructField> fields;
            fields.emplace_back(InternalKeyword::MAP_KEY, childKeyType.copy());
            fields.emplace_back(InternalKeyword::MAP_VALUE, childValueType.copy());
            std::vector<std::unique_ptr<Value>> structValues;
            structValues.push_back(
                std::make_unique<Value>(transformPythonValueAs(child.first, childKeyType)));
            structValues.push_back(
                std::make_unique<Value>(transformPythonValueAs(child.second, childValueType)));
            children.push_back(std::make_unique<Value>(LogicalType::STRUCT(std::move(fields)),
                std::move(structValues)));
        }
        return Value(type.copy(), std::move(children));
    }
    case LogicalTypeID::STRUCT: {
        auto dict = py::reinterpret_borrow<py::dict>(val);
        std::vector<std::unique_ptr<Value>> children;
        auto fieldIdx = 0u;
        for (auto field : dict) {
            auto fieldType = StructType::getFieldType(type, fieldIdx++).copy();
            children.push_back(
                std::make_unique<Value>(transformPythonValueAs(field.second, fieldType)));
        }
        return Value(type.copy(), std::move(children));
    }
    // LCOV_EXCL_START
    default:
        KU_UNREACHABLE;
        // LCOV_EXCL_STOP
    }
}

Value PyConnection::transformPythonValueFromParameterAs(const py::handle& val,
    const LogicalType& type) {
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::LIST: {
        py::list lst = py::reinterpret_borrow<py::list>(val);
        std::vector<std::unique_ptr<Value>> children;
        for (auto child : lst) {
            children.push_back(std::make_unique<Value>(
                transformPythonValueFromParameterAs(child, ListType::getChildType(type))));
        }
        return Value(type.copy(), std::move(children));
    }
    case LogicalTypeID::MAP: {
        auto dict = py::reinterpret_borrow<py::dict>(val);
        std::vector<std::unique_ptr<Value>> children;
        auto keys = transformPythonValueFromParameter(
            py::reinterpret_borrow<py::list>(dict.begin()->second));
        auto vals = transformPythonValueFromParameter(
            py::reinterpret_borrow<py::list>((++dict.begin())->second));
        auto numKeys = NestedVal::getChildrenSize(&keys);
        // LCOV_EXCL_START
        if (NestedVal::getChildrenSize(&keys) != NestedVal::getChildrenSize(&vals)) {
            throw RuntimeException("Map Key and Value lengths do not match!");
        }
        // LCOV_EXCL_STOP
        for (auto i = 0u; i < numKeys; i++) {
            std::vector<std::unique_ptr<Value>> structValues;
            structValues.push_back(NestedVal::getChildVal(&keys, i)->copy());
            structValues.push_back(NestedVal::getChildVal(&vals, i)->copy());
            children.push_back(std::make_unique<Value>(ListType::getChildType(type).copy(),
                std::move(structValues)));
        }
        return Value(type.copy(), std::move(children));
    }
    case LogicalTypeID::STRUCT: {
        auto dict = py::reinterpret_borrow<py::dict>(val);
        std::vector<std::unique_ptr<Value>> children;
        auto fieldIdx = 0u;
        for (auto field : dict) {
            auto fieldType = StructType::getFieldType(type, fieldIdx++).copy();
            children.push_back(std::make_unique<Value>(
                transformPythonValueFromParameterAs(field.second, fieldType)));
        }
        return Value(type.copy(), std::move(children));
    }
    case LogicalTypeID::POINTER: {
        return Value::createValue(reinterpret_cast<uint8_t*>(val.ptr()));
    }
    default:
        return transformPythonValueAs(val, type);
    }
}

Value PyConnection::transformPythonValue(const py::handle& val) {
    auto type = pyLogicalType(val);
    return transformPythonValueAs(val, type);
}

Value PyConnection::transformPythonValueFromParameter(const py::handle& val) {
    auto type = pyLogicalTypeFromParameter(val);
    return transformPythonValueFromParameterAs(val, type);
}

std::unique_ptr<PyQueryResult> PyConnection::checkAndWrapQueryResult(
    std::unique_ptr<QueryResult>& queryResult) {
    if (!queryResult->isSuccess()) {
        throw std::runtime_error(queryResult->getErrorMessage());
    }
    auto pyQueryResult = std::make_unique<PyQueryResult>();
    pyQueryResult->queryResult = queryResult.release();
    pyQueryResult->isOwned = true;
    return pyQueryResult;
}

void PyConnection::createScalarFunction(const std::string& name, const py::function& udf,
    const py::list& params, const std::string& retval, bool defaultNull, bool catchExceptions) {
    conn->addUDFFunctionSet(name, PyUDF::toFunctionSet(name, udf, params, retval, defaultNull,
                                      catchExceptions, conn->getClientContext()));
}

void PyConnection::removeScalarFunction(const std::string& name) {
    conn->removeUDFFunction(name);
}
