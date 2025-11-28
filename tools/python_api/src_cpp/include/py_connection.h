#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <mutex>

#include "main/storage_driver.h"
#include "py_database.h"
#include "py_prepared_statement.h"
#include "py_query_result.h"
#include "graph/on_disk_graph.h"
#include "graph/neighbor_sampler.h"

using monad::common::LogicalType;
using monad::common::LogicalTypeID;
using monad::common::Value;



class PyConnection {

public:
    static void initialize(py::handle& m);

    explicit PyConnection(PyDatabase* pyDatabase, uint64_t numThreads);

    void close();

    ~PyConnection();

    void setQueryTimeout(uint64_t timeoutInMS);
    void interrupt();

    std::unique_ptr<PyQueryResult> execute(PyPreparedStatement* preparedStatement,
        const py::dict& params);

    std::unique_ptr<PyQueryResult> query(const std::string& statement);

    void setMaxNumThreadForExec(uint64_t numThreads);

    PyPreparedStatement prepare(const std::string& query, const py::dict& parameters);

    uint64_t getNumNodes(const std::string& nodeName);

    uint64_t getNumRels(const std::string& relName);

    void getAllEdgesForTorchGeometric(py::array_t<int64_t>& npArray,
        const std::string& srcTableName, const std::string& relName,
        const std::string& dstTableName, size_t queryBatchSize);

    py::list getGraphRelInfo(const std::string& graphName);

    py::dict neighborSample(const std::string& graphName, const py::dict& seedOffsets,
        const py::sequence& fanouts, bool replace, const std::string& direction,
        const std::optional<int64_t>& randomSeed, py::object timeObj, py::object timeFieldsObj);

    static bool isPandasDataframe(const py::handle& object);
    static bool isPolarsDataframe(const py::handle& object);
    static bool isPyArrowTable(const py::handle& object);

    void createScalarFunction(const std::string& name, const py::function& udf,
        const py::list& params, const std::string& retval, bool defaultNull, bool catchExceptions);
    void removeScalarFunction(const std::string& name);

    static Value transformPythonValue(const py::handle& val);
    static Value transformPythonValueAs(const py::handle& val, const LogicalType& type);
    static Value transformPythonValueFromParameter(const py::handle& val);
    static Value transformPythonValueFromParameterAs(const py::handle& val,
        const LogicalType& type);

private:
    std::unique_ptr<StorageDriver> storageDriver;
    std::unique_ptr<Connection> conn;
    std::unordered_map<std::string, std::shared_ptr<monad::graph::OnDiskGraph>> graphCache;

    std::mutex cacheMutex;


    static std::unique_ptr<PyQueryResult> checkAndWrapQueryResult(
        std::unique_ptr<QueryResult>& queryResult);
};
