package org.gdbtesting.gremlin;

import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.GremlinManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.structure.gremlin.ResultSet;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedElement;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferencePath;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.gdbtesting.Randomly;
import org.gdbtesting.connection.GremlinConnection;
import org.gdbtesting.gremlin.ast.GraphConstant;
import org.gdbtesting.gremlin.query.GraphTraversalGenerator;

import java.io.*;
import java.util.*;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GraphDBExecutor {

    private List<GremlinConnection> connections;
    private GraphGlobalState state;
    private Map<String, String> commands;

    // Map vertex and edge in different databases to a unified ID
    // ==========  Start dividing line for path strategy 1 ==========
    private Map<String, Map<String, String>> vertexIDMap;
    private Map<String, Map<String, String>> edgeIDMap;
    private List<String> queryList;
    private List<String> queryOneList;
    private List<String> queryTwoList;
    private List<List<List<Object>>> resultList;
    private List<List<List<Object>>> resultOneList;
    private List<List<List<Object>>> resultTwoList;
    private List<List<List<String>>> resultNodeOneList;
    private List<List<List<String>>> resultNodeTwoList;
    private List<Map<String, Exception>> errorList;
    private List<Map<String, Exception>> errorOneList;
    private List<Map<String, Exception>> errorTwoList;
    private List<List<List<String>>> successList;
    // ==========  Start dividing line for path strategy 2, 3 ==========
    private List<List<String>> queriesList;


    public GraphDBExecutor(List<GremlinConnection> connections, GraphGlobalState state) {
        this.connections = connections;
        this.state = state;
    }

    public void initiateGraphDBExecutor(int strategyFlag, int mrFlag) {
        // For pure random-based testing
        if (strategyFlag == 0) {
            queryList = new ArrayList<>();
            resultList = new ArrayList<>((int) state.getQueryNum());
            errorList = new ArrayList<>((int) state.getQueryNum());
            for (int i = 0; i < state.getQueryNum(); i++) {
                resultList.add(i, new ArrayList<>(connections.size()));
                errorList.add(i, new HashMap<>());
            }
        } else if (strategyFlag == 1) {
            if (mrFlag == 0) {
                queriesList = new ArrayList<>((int) state.getQueryNum());
                queryOneList = new ArrayList<>();
                resultOneList = new ArrayList<>((int) state.getQueryNum());
                resultNodeOneList = new ArrayList<>((int) state.getQueryNum());
                errorOneList = new ArrayList<>((int) state.getQueryNum());
                for (int i = 0; i < state.getQueryNum(); i++) {
                    queriesList.add(i, new ArrayList<>(10));
                    resultOneList.add(i, new ArrayList<>(connections.size()));
                    resultNodeOneList.add(i, new ArrayList<>(connections.size()));
                    errorOneList.add(i, new HashMap<>());
                }
            } else if (mrFlag == 1) {
                queriesList = new ArrayList<>((int) state.getQueryNum());
                queryTwoList = new ArrayList<>();
                resultTwoList = new ArrayList<>((int) state.getQueryNum());
                resultNodeTwoList = new ArrayList<>((int) state.getQueryNum());
                errorTwoList = new ArrayList<>((int) state.getQueryNum());
                successList = new ArrayList<>((int) state.getQueryNum());
                for (int i = 0; i < state.getQueryNum(); i++){
                    queriesList.add(i, new ArrayList<>(10));
                    resultTwoList.add(i, new ArrayList<>(connections.size()));
                    resultNodeTwoList.add(i, new ArrayList<>(connections.size()));
                    errorTwoList.add(i, new HashMap<>());
                    successList.add(i, new ArrayList<>(connections.size()));
                }
            }
        }
    }

    public void initiateGraphDBExecutor2() {
        queriesList = new ArrayList<>((int) state.getQueryNum());
        for (int i = 0; i < state.getQueryNum(); i++) {
            queriesList.add(i, new ArrayList<>(10));
        }
    }

    public void generateRandomQuery(){
        queryList = new ArrayList<>();
        resultList = new ArrayList<>((int) state.getQueryNum());
        errorList = new ArrayList<>((int) state.getQueryNum());
        GraphTraversalGenerator gtg = new GraphTraversalGenerator(state);
        for(int i = 0; i < state.getQueryNum(); i++){
            String query = gtg.generateRandomlyTraversal();
            queryList.add(query);
            resultList.add(i, new ArrayList<>(connections.size()));
            errorList.add(i, new HashMap<>());
        }
    }

    public void generateRandomQuery_Backup() {
        GraphTraversalGenerator gtg = new GraphTraversalGenerator(state);
        for (int i = 0; i < state.getQueryNum(); i++) {
            String query = gtg.generateRandomlyTraversal();
            queryList.add(query);
        }
    }

    public void generateStrategyQuery(String strategy) {
        GraphTraversalGenerator gtg = new GraphTraversalGenerator(state);
        for (int i = 0; i < state.getQueryNum(); i++) {
            switch (strategy) {
                case "path":
                    int variant = 1;
                    String query = gtg.generateStrategyTraversal(strategy, variant);
                    queryOneList.add(query);
                    break;
                case "node":
                    break;
                case "edge":
                    break;
                default:
                    break;
            }
        }
    }

    public String generateStrategyMRQuery(String strategy, int database, String id) {
        GraphTraversalGenerator gtg = new GraphTraversalGenerator(state);
        switch (strategy) {
            case "path":
                // Strategy 1: Path. Path strategy also has so many variants
                // Variant 1:
                int variant = 1;
                // Use specified nodeId to start traversal
                String query = gtg.generateStrategyMRTraversal(strategy, variant, id);
                return query;
            case "node":
                return "";
            case "edge":
                return "";
        }
        return "";
    }

    public String generateCheckQuery(String strategy, String startId, String endId) {
        GraphTraversalGenerator gtg = new GraphTraversalGenerator(state);
        switch (strategy) {
            case "path":
                String query = gtg.generateCheckTraversal(strategy, startId, endId);
                return query;
            case "node":
                return "";
            case "edge":
                return "";
        }
        return "";
    }

    public String generateStrategyQuerySequence(int queryNum, String strategy, String startId, String endId) {
        GraphTraversalGenerator gtg = new GraphTraversalGenerator(state);
        switch (strategy) {
            case "path":
                if (queryNum == 1) {
                    String query = gtg.generateStrategyTraversal(strategy, 1);
                    return query;
                } else if (queryNum == 2) {
                    String query = gtg.generateStrategyMRTraversal(strategy, 1, startId);
                    return query;
                } else if (queryNum == 3) {
                    String query = gtg.generateCheckTraversal(strategy, startId, endId);
                    return query;
                }
                return "";
            case "node":
                return "";
            case "edge":
                return "";
        }
        return "";
    }

    public String generateStrategyMRQuerySequence(int queryNum, String strategy, int variant, String startId, String endId) {
        GraphTraversalGenerator gtg = new GraphTraversalGenerator(state);
        switch (strategy) {
            case "path":
                switch (variant) {
                    case 1:
                        if (queryNum == 1) {
                            String query = gtg.getVAllIdsExpression();
                            return query;
                        } else if (queryNum == 2 || queryNum == 3 || queryNum == 5) {
                            String query = gtg.getPathCheckExpression(startId, endId);
                            return query;
                        } else if (queryNum == 4) {
                            String query = gtg.getAddEBetweenVerticesExpression(startId, endId, "el0");
                            return query;
                        }
                        return "";
                    case 2:
                        if (queryNum == 1) {
                            String query = gtg.generateStrategyTraversal(strategy, variant);
                            return query;
                        } else if (queryNum == 2) {
                            String query = gtg.getAddVerticesExpression();
                            return query;
                        } else if (queryNum == 3) {
                            String query = gtg.getAddEBetweenVerticesExpression(startId, endId, "el" + String.valueOf(Randomly.getInteger(50, 100)));
                            return query;
                        }
                        return "";
                    case 3:
                        if (queryNum == 1) {
                            String query = gtg.getVAllIdsExpression();
                            return query;
                        } else if (queryNum == 2 || queryNum == 3) {
                            String query = gtg.getPathCheckExpression(startId, endId);
                            return query;
                        }
                        return "";
                    case 4:
                        if (queryNum == 1) {
                            String query = gtg.getVAllIdsExpression();
                            return query;
                        } else if (queryNum == 2 || queryNum == 5) {
                            String query = gtg.getPathCheckExpression(startId, endId);
                            return query;
                        } else if (queryNum == 3) {
                            String query = gtg.getAddVerticesExpression();
                            return query;
                        } else if (queryNum == 4) {
                            String query = gtg.getAddEBetweenVerticesExpression(startId, endId, "el" + String.valueOf(Randomly.getInteger(50, 100)));
                            return query;
                        }
                    case 5:
                        if (queryNum == 1) {
                            String query = gtg.getVAllIdsExpression();
                            return query;
                        } else if (queryNum == 2 || queryNum == 4) {
                            String query = gtg.getOneHopCheckExpression(startId, endId);
                            return query;
                        } else if (queryNum == 3) {
                            String query = gtg.getDropEBetweenVerticesExpression(startId, endId);
                            return query;
                        }
                }
            case "node":
                switch (variant) {
                    case 1:
                        if (queryNum == 1) {
                            String query = gtg.getNodeFilterExpression();
                            return query;
                        } else if (queryNum == 3) {
                            String query = gtg.getNodeSizeExpression();
                            return query;
                        }
                        return "";
                    case 2:
                        if (queryNum == 1) {
                            String query = gtg.getNodeFilterExpression();
                            return query;
                        }
                    case 3:
                        if (queryNum == 1) {
                            String query = gtg.getNodeFilterExpression();
                            return query;
                        }
                    case 4:
                        if (queryNum == 1 || queryNum == 2) {
                            String query = gtg.generateRandomlyNodeTraversal();
                            return query;
                        }
                    case 5:
                        if (queryNum == 1 || queryNum == 2) {
                            String query = gtg.generateRandomlyNodeTraversal();
                            return query;
                        }
                    case 6:
                        if (queryNum == 1 || queryNum == 2) {
                            String query = gtg.generateRandomlyNodeValueTraversal();
                            return query;
                        }
                    case 7:
                        if (queryNum == 1) {
                            String query = gtg.getVAllIdsExpression();
                            return query;
                        } else if (queryNum == 2) {
                            String query = gtg.getKHopNodesExpression(startId, endId);
                            return query;
                        } else if (queryNum == 3) {
                            String query = gtg.getKHopNodesReverseExpression(startId, endId);
                            return query;
                        }
                    case 8:
                        if (queryNum == 1) {
                            String query = gtg.getVAllIdsExpression();
                            return query;
                        } else if (queryNum == 2 || queryNum == 3) {
                            String query = gtg.getVSpouseExpression(startId);
                            return query;
                        }
                    case 9:
                        if (queryNum == 1) {
                            String query = gtg.getVAllIdsExpression();
                            return query;
                        } else if (queryNum == 2) {
                            String query = gtg.getVDescendantExpression(startId);
                            return query;
                        } else if (queryNum == 3) {
                            String query = gtg.getVAncestorExpression(startId);
                            return query;
                        }
                    case 10:
                        if (queryNum == 1) {
                            String query = gtg.getVAllIdsExpression();
                            return query;
                        } else if (queryNum == 2) {
                            String query = gtg.getVAllPathsExpression(startId, endId);
                            return query;
                        }
                    case 11:
                        if (queryNum == 1) {
                            String query = gtg.getRandomlyNodeHasPartitionExpression();
                            return query;
                        }
                    case 12:
                        if (queryNum == 1) {
                            String query = gtg.getRandomlyNodeHasPartitionCountExpression();
                            return query;
                        }
                    case 13:
                        if (queryNum == 1) {
                            String query = gtg.getRandomlyNodeHasPartitionDedupExpression();
                            return query;
                        } else if (queryNum == 2) {
                            String query = gtg.getVAllIdsExpression();
                            return query;
                        } else if (queryNum == 3) {
                            String query = gtg.getDropVerticesExpression(startId);
                            return query;
                        }
                    case 14:
                        if (queryNum == 1) {
                            String query = gtg.getRandomlyNodeHasPartitionExpression();
                            return query;
                        } else if (queryNum == 2) {
                            String query = gtg.getAddMultipleVerticesExpression();
                            return query;
                        } else if (queryNum == 3) {
                            String query = gtg.getAddMultipleEBetweenVerticesExpression(startId);
                            return query;
                        }
                    case 15:
                        if (queryNum == 1) {
                            String query = gtg.getRandomlyNodeHasPartitionCountExpression();
                            return query;
                        } else if (queryNum == 2) {
                            String query = gtg.getAddMultipleVerticesExpression();
                            return query;
                        } else if (queryNum == 3) {
                            String query = gtg.getAddMultipleEBetweenVerticesExpression(startId);
                            return query;
                        }
                    case 16:
                        if (queryNum == 1) {
                            String query = gtg.getVAllIdsExpression();
                            return query;
                        } else if (queryNum == 2) {
                            String query = gtg.getVSpouseHasPartitionExpression(startId);
                            return query;
                        }
                    case 17:
                        if (queryNum == 1) {
                            String query = gtg.getVAllIdsExpression();
                            return query;
                        } else if (queryNum == 2) {
                            String query = gtg.getVDescendantHasPartitionExpression(startId);
                            return query;
                        }
                    case 18:
                        if (queryNum == 1) {
                            String query = gtg.getVAllIdsExpression();
                            return query;
                        } else if (queryNum == 2) {
                            String query = gtg.getKHopNodesHasPartitionExpression(startId, endId);
                            return query;
                        }
                }
                return "";
            case "edge":
                switch (variant) {
                    case 1:
                        if (queryNum == 1) {
                            String query = gtg.getEdgeFilterExpression();
                            return query;
                        } else if (queryNum == 3) {
                            String query = gtg.getEdgeSizeExpression();
                            return query;
                        }
                    case 2:
                        if (queryNum == 1) {
                            String query = gtg.getEdgeFilterExpression();
                            return query;
                        }
                    case 3:
                        if (queryNum == 1) {
                            String query = gtg.getEdgeFilterExpression();
                            return query;
                        }
                    case 4:
                        if (queryNum == 1 || queryNum == 2) {
                            String query = gtg.generateRandomlyEdgeTraversal();
                            return query;
                        }
                    case 5:
                        if (queryNum == 1 || queryNum == 2) {
                            String query = gtg.generateRandomlyEdgeTraversal();
                            return query;
                        }
                }
                return "";
        }
        return "";
    }

    public String generateStrategyMRQuerySequence(int queryNum, String strategy, int variant, String id1, String id2, String id3) {
        GraphTraversalGenerator gtg = new GraphTraversalGenerator(state);
        switch (strategy) {
            case "path":
                switch (variant) {
                    case 3:
                        if (queryNum == 4) {
                            String query = gtg.getPathHopCheckExpression(id1, id2, id3);
                            return query;
                        }
                        return "";
                }
            case "node":
                return "";
            case "edge":
                return "";
        }
        return "";
    }


    /**
     * ==========  Start dividing line for deprecated codes ==========
     */

    public void executeQuery_Random(GremlinConnection connection, int count) throws IOException {
        String cur = System.getProperty("period");
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log_orig/" + connection.getDatabase()+ "-" + cur +".log"));
        for(int i = 0; i < queryList.size(); i++){
            try{
                out.write("========================Query " + i + "=======================\n");
                out.write(queryList.get(i) + "\n");
                long time = System.currentTimeMillis();
                List<Result> results;
                List<Object> list = new ArrayList<>();
                if(connection.getDatabase().equals("HugeGraph")){
                    GremlinManager gremlin = connection.getHugespecial().gremlin();
                    com.baidu.hugegraph.structure.gremlin.ResultSet hugeResult = gremlin.gremlin(queryList.get(i)).execute();
                    Iterator<com.baidu.hugegraph.structure.gremlin.Result> huresult = hugeResult.iterator();
                    Long t = System.currentTimeMillis() - time;
                    System.out.println("query " + i + " in " + t + "ms");
                    huresult.forEachRemaining(result -> {
                        Object object = result.getObject();
                        if (object instanceof com.baidu.hugegraph.structure.graph.Vertex) {
                            try {
                                out.write("v[" + ((com.baidu.hugegraph.structure.graph.Vertex) object).id() + "]");
                                list.add(object);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        } else if (object instanceof com.baidu.hugegraph.structure.graph.Edge) {
                            try {
                                out.write("e[" + ((com.baidu.hugegraph.structure.graph.Edge) object).id() + "]");
                                list.add(object);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        } else if (object instanceof com.baidu.hugegraph.structure.graph.Path) {
                            List<Object> elements = ((com.baidu.hugegraph.structure.graph.Path) object).objects();
                            elements.forEach(element -> {
                                System.out.println(element.getClass());
                                System.out.println(element);
                            });
                        } else {
                            try {
                                out.write("n[" + object + "]");
                                list.add(object);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                        }
                    });
                }
                else{
                    results = connection.getClient().submit(queryList.get(i)).all().get();
                    System.out.println("query " + i + " in " + (System.currentTimeMillis() - time) + "ms");
                    for (Result r : results) {
                        String result = String.valueOf(r);
                        out.write(result);
                        out.newLine();
                        list.add(r);
                    }
                }
                resultList.get(i).add(count, list);
            }catch (Exception e){
                out.write(e.toString());
                resultList.get(i).add(count, null);
                errorList.get(i).put(String.valueOf(count),e);
            }
        }
        out.close();
    }


    public void executeQuery(GremlinConnection connection, int database,
                             List<String> executeQueryList, List<List<List<Object>>> executeResultList, List<Map<String, Exception>> executeErrorList, List<List<List<String>>> executeResultNodeList,
                             int mrFlag) throws IOException {
        BufferedWriter out = null;
        if (mrFlag == 0) {
            out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/dt" + "/" + connection.getDatabase() + "-all" + ".log"));
        } else if (mrFlag == 1) {
            out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/dt" + "/" + connection.getDatabase() + "-all2" + ".log"));
        }
        for (int i = 0; i < executeQueryList.size(); i++) {
            try {
                String executeQuery = executeQueryList.get(i);
                out.append("========================Query " + i + "=======================\n");
                out.append(executeQuery+ "\n");
                long time = System.currentTimeMillis();
                List<Result> results;
                List<Object> resultsList = new ArrayList<>();
                List<String> nodesList = new ArrayList<>();

                if (executeQuery.equals("")) {
                    executeResultList.get(i).add(database, resultsList);
                    executeResultNodeList.get(i).add(database, nodesList);
                    continue;
                }

                // Abandon HugeGraph
                if (connection.getDatabase().equals("HugeGraph")) {
                    executeResultList.get(i).add(database, resultsList);
                    executeResultNodeList.get(i).add(database, nodesList);
                    continue;
                } else {
                    results = connection.getClient().submit(executeQuery).all().get();
                    System.out.println("query " + i + executeQuery + ", in " + (System.currentTimeMillis() - time) + "ms");

                    if (results.size() != 0) {
                        for (Result r : results) {
                            String res = String.valueOf(r.getPath());
                            if (res != null) {
                                List<String> regex = new ArrayList<>();
                                List<String> nodes = new ArrayList<>();
                                regex = Arrays.asList(res.split(", "));
                                regex.forEach((e) -> {
                                    String node = e.replaceAll("[^0-9]", "");
                                    nodes.add(node);
                                });
                                nodesList.add(nodes.toString());
                            }

                            String result = String.valueOf(r);
                            out.append(result);
                            out.newLine();
                            resultsList.add(r);
                        }
                    }
                    executeResultList.get(i).add(database, resultsList);
                    executeResultNodeList.get(i).add(database, nodesList);
                }
            } catch (Exception e) {
                e.printStackTrace();
                executeErrorList.get(i).put(String.valueOf(database), e);
            }
        }
        System.out.println("For debug.");
        out.close();
    }

    public void executeMRQuery(GremlinConnection connection, int database,
                             List<String> executeQueryList, List<List<List<Object>>> executeResultList, List<Map<String, Exception>> executeErrorList, List<List<List<String>>> executeResultNodeList,
                             int mrFlag) throws IOException {
        BufferedWriter out = null;
        if (mrFlag == 0) {
            out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr1" + "/" + connection.getDatabase() + "-all1" + ".log"));
        } else if (mrFlag == 1) {
            out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr1" + "/" + connection.getDatabase() + "-all2" + ".log"));
        }
        for (int i = 0; i < state.getQueryNum(); i++) {
            long time = System.currentTimeMillis();
            List<Result> results;
            List<Object> resultsList = new ArrayList<>();
            List<String> nodesList = new ArrayList<>();
            List<String> successList = new ArrayList<>();
            String executeQuery = "";

            // Generate MR queries
            List<String> nodesId = new ArrayList<>();
            if (resultNodeOneList.get(i) != null && resultNodeOneList.get(i).size() == 3) {
                nodesId = resultNodeOneList.get(i).get(database);
            }
            if (nodesId.size() == 0 || nodesId == null) {
                executeQuery = "";
                executeQueryList.add(executeQuery);
                executeResultList.get(i).add(database, resultsList);
                executeResultNodeList.get(i).add(database, nodesList);
                this.successList.get(i).add(database, nodesList);
                continue;
            } else {
                // Get all traversal id
                for (int j = 0; j < nodesId.size(); j++) {
                    String nodeId = nodesId.get(j);
                    List<String> ids = new ArrayList<>();
                    if (nodeId != null) {
                        List<String> regex = new ArrayList<>();
                        regex = Arrays.asList(nodeId.split(", "));
                        regex.forEach((e) -> {
                            String node = e.replaceAll("[^0-9]", "");
                            ids.add(node);
                        });
                    }
                    String startId = ids.get(0);
                    for (int k = 1; k < ids.size(); k++) {
                        String id = ids.get(k);
                        executeQuery = generateStrategyMRQuery("path", database, id);
                        out.append("========================Query " + i + "=======================\n");
                        out.append(executeQuery + "\n");

                        // Execute query
                        if (connection.getDatabase().equals("HugeGraph")) {
                            System.out.println("HugeGraph pass");
                        } else {
                            try {
                                System.out.println("query " + i + " in " + (System.currentTimeMillis() - time) + "ms");
                                results = connection.getClient().submit(executeQuery).all().get();
                                if (results.size() == 0 || results == null) {
                                    continue;
                                } else {
                                    for (Result r : results) {
                                        // Append node indexes to resultNodeList
                                        String res = String.valueOf(r.getPath());
                                        if (res != null) {
                                            List<String> regex2 = new ArrayList<>();
                                            List<String> nodes2 = new ArrayList<>();
                                            List<String> nodes3 = new ArrayList<>();
                                            regex2 = Arrays.asList(res.split(", "));
                                            regex2.forEach((e) -> {
                                                String node = e.replaceAll("[^0-9]", "");
                                                nodes2.add(node);
                                                nodes3.add(node);
                                            });
                                            nodesList.add(nodes2.toString());
                                            nodes3.set(0, startId);
                                            successList.add(nodes3.toString());
                                        }

                                        String result = String.valueOf(r);
                                        out.append(result);
                                        out.newLine();
                                        resultsList.add(r);
                                    }
                                    executeQueryList.add(executeQuery);
                                    executeResultList.get(i).add(database, resultsList);     // Count: means different databases. List item: means return objects results
                                    executeResultNodeList.get(i).add(database, nodesList);
                                    this.successList.get(i).add(database, successList);

                                    // Break two out loops.
                                    j = nodesId.size();
                                    break;
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (ExecutionException e) {
                                executeResultList.get(i).add(database, null);
                                executeResultNodeList.get(i).add(database, null);
                                this.successList.get(i).add(database, null);
                                executeErrorList.get(i).put(String.valueOf(database), e);
                            }
                        }

                    }
                }
            }
            System.out.println("Finish execute query number: " + i);
        }
        System.out.println("For debug.");
        out.close();
    }

    /**
     * ==========  End dividing line for deprecated codes ==========
     */

    public void executeMRQuery1(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr1" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr1" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 5) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                List<Result> results, results2, results3;
                List<Object> resultsList = new ArrayList<>();
                List<Object> resultsList2 = new ArrayList<>();
                List<String> nodesList = new ArrayList<>();
                List<String> nodesList2 = new ArrayList<>();
                List<String> successList = new ArrayList<>();
                String x1 = "", x2 = "", x3 = "";
                out.append("========================Query sequence " + i + "=======================\n");

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyQuerySequence(queryNum, "path", "", "");
                try {
                    results = connection.getClient().submit(executeQuery).all().get();
                    out.append("query 1:" + "\n");
                    out.append(executeQuery+ "\n");
                    System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    queriesList.get(i).add(executeQuery);
                    queryOneList.add(executeQuery);

                    if (results.size() != 0) {
                        for (Result r : results) {
                            DetachedPath path = (DetachedPath) r.getPath();
                            if (path != null) {
                                List<String> nodes = new ArrayList<>();
                                for (Object p : path) {
                                    DetachedVertex v = (DetachedVertex) p;
                                    nodes.add(v.id().toString());
                                }
                                nodesList.add(nodes.toString());
                            }
                            String rstr = String.valueOf(r);
                            out.append(rstr);
                            out.newLine();
                            resultsList.add(r);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errorOneList.get(i).put(String.valueOf(database), e);

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    continue;
                }

                // Query 2:
                queryNum = 2;
                if (nodesList.size() == 0 || nodesList == null) {
                    executeQuery = "";
                    queriesList.get(i).add(executeQuery);
                    queryTwoList.add(executeQuery);
                    continue;
                } else {
                    for (int j = 0; j < nodesList.size(); j++) {
                        String nodeIdTmp = nodesList.get(j).substring(1);
                        String nodeIdStr = nodeIdTmp.substring(0, nodeIdTmp.length() - 1);
                        List<String> nodeIds = new ArrayList<>();
                        if (nodeIdStr != null) {
                            List<String> regex = new ArrayList<>();
                            regex = Arrays.asList(nodeIdStr.split(", "));
                            regex.forEach((e) -> {
                                nodeIds.add(e);
                            });
                        }
                        String startId = nodeIds.get(0);
                        for (int k = 1; k < nodeIds.size(); k++) {
                            String id = "'" + nodeIds.get(k) + "'";
                            executeQuery = generateStrategyQuerySequence(queryNum, "path", id, "");

                            try {
                                results2 = connection.getClient().submit(executeQuery).all().get();
                                out.append("query 2:" + "\n");
                                out.append(executeQuery + "\n");
                                System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                if (results2.size() == 0 || results2 == null) {
                                    continue;
                                } else {
                                    for (Result r : results2) {
                                        DetachedPath path = (DetachedPath) r.getPath();
                                        if (path != null) {
                                            List<String> nodes2 = new ArrayList<>();
                                            List<String> nodes3 = new ArrayList<>();
                                            for (Object p : path) {
                                                DetachedVertex v = (DetachedVertex) p;
                                                nodes2.add(v.id().toString());
                                                nodes3.add(v.id().toString());
                                            }
                                            nodesList2.add(nodes2.toString());
                                            nodes3.set(0, startId);
                                            successList.add(nodes3.toString());
                                        }

                                        String rstr = String.valueOf(r);
                                        out.append(rstr);
                                        out.newLine();
                                        resultsList2.add(r);
                                    }
                                    queriesList.get(i).add(executeQuery);
                                    queryTwoList.add(executeQuery);

                                    // Break two out loops.
                                    j = nodesList.size();
                                    break;
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                errorTwoList.get(i).put(String.valueOf(database), e);

                                System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                                result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                                continue;
                            }
                        }
                    }
                }

                // Query 3:
                queryNum = 3;
                if (successList.size() == 0 || successList == null) {
                    executeQuery = "";
                    queriesList.get(i).add(executeQuery);
                    continue;
                } else {
                    List<String> executeList = new ArrayList<>();
                    for (String nodeList : successList) {
                        String tmp = nodeList.substring(1);
                        String nodeListTmp = tmp.substring(0, tmp.length() - 1);
                        List<String> regex = new ArrayList<>();
                        List<String> nodes = new ArrayList<>();
                        regex = Arrays.asList(nodeListTmp.split(", "));
                        regex.forEach((e) -> {
                            nodes.add(e);
                        });
                        if (executeList.size() == 0) {
                            executeList.add(nodes.get(0));
                            executeList.addAll(nodes.subList(1, nodes.size()));
                        } else {
                            executeList.addAll(nodes.subList(1, nodes.size()));
                        }
                    }
                    x1 = "'" + executeList.get(0) + "'";
                    for (int k = 1; k < executeList.size(); k++) {
                        x2 = "'" + executeList.get(k) + "'";
                        executeQuery = generateStrategyQuerySequence(queryNum, "path", x1, x2);

                        out.append("query 3:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        queriesList.get(i).add(executeQuery);
                        try {
                            results3 = connection.getClient().submit(executeQuery).all().get();
                            if (results3.size() == 0 || results3 == null) {
                                System.out.println(connection.getDatabase() + " false : " + executeQuery + "\n");
                                result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                result.append(connection.getDatabase() + " false : " + executeQuery + "\n");
                            } else {
                                for (Result r : results3) {
                                    if (r.getInt() == 0) {
                                        System.out.println(connection.getDatabase() + " false : " + executeQuery + "\n");
                                        result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        result.append(connection.getDatabase() + " false : " + executeQuery + "\n");
                                    }
                                    String rstr = String.valueOf(r);
                                    out.append(connection.getDatabase() + ": " + rstr + "\n");
                                    out.newLine();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();

                            System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                            result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                            continue;
                        }
                    }
                }
            }
            out.close();
            result.close();
        } else if (database == 4) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                List<Result> results, results2, results3;
                List<Object> resultsList = new ArrayList<>();
                List<Object> resultsList2 = new ArrayList<>();
                List<String> nodesList = new ArrayList<>();
                List<String> nodesList2 = new ArrayList<>();
                List<String> successList = new ArrayList<>();
                String x1 = "", x2 = "", x3 = "";
                out.append("========================Query sequence " + i + "=======================\n");

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyQuerySequence(queryNum, "path", "", "");
                try {
                    results = connection.getClient().submit(executeQuery).all().get();
                    out.append("query 1:" + "\n");
                    out.append(executeQuery+ "\n");
                    System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    queriesList.get(i).add(executeQuery);
                    queryOneList.add(executeQuery);

                    if (results.size() != 0) {
                        for (Result r : results) {
                            ReferencePath path = (ReferencePath) r.getPath();
                            if (path != null) {
                                List<String> nodes = new ArrayList<>();
                                for (Object p : path) {
                                    ReferenceVertex v = (ReferenceVertex) p;
                                    nodes.add((String) v.id());
                                }
                                nodesList.add(nodes.toString());
                            }
                            String rstr = String.valueOf(r);
                            out.append(rstr);
                            out.newLine();
                            resultsList.add(r);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errorOneList.get(i).put(String.valueOf(database), e);

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    continue;
                }

                // Query 2:
                queryNum = 2;
                if (nodesList.size() == 0 || nodesList == null) {
                    executeQuery = "";
                    queriesList.get(i).add(executeQuery);
                    queryTwoList.add(executeQuery);
                    continue;
                } else {
                    for (int j = 0; j < nodesList.size(); j++) {
                        String nodeIdTmp = nodesList.get(j).substring(1);
                        String nodeIdStr = nodeIdTmp.substring(0, nodeIdTmp.length() - 1);
                        List<String> nodeIds = new ArrayList<>();
                        if (nodeIdStr != null) {
                            List<String> regex = new ArrayList<>();
                            regex = Arrays.asList(nodeIdStr.split(", "));
                            regex.forEach((e) -> {
                                nodeIds.add(e);
                            });
                        }
                        String startId = nodeIds.get(0);
                        for (int k = 1; k < nodeIds.size(); k++) {
                            String id = "'" + nodeIds.get(k) + "'";
                            executeQuery = generateStrategyQuerySequence(queryNum, "path", id, "");

                            try {
                                results2 = connection.getClient().submit(executeQuery).all().get();
                                out.append("query 2:" + "\n");
                                out.append(executeQuery + "\n");
                                System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                if (results2.size() == 0 || results2 == null) {
                                    continue;
                                } else {
                                    for (Result r : results2) {
                                        ReferencePath path = (ReferencePath) r.getPath();
                                        if (path != null) {
                                            List<String> nodes2 = new ArrayList<>();
                                            List<String> nodes3 = new ArrayList<>();
                                            for (Object p : path) {
                                                ReferenceVertex v = (ReferenceVertex) p;
                                                nodes2.add((String) v.id());
                                                nodes3.add((String) v.id());
                                            }
                                            nodesList2.add(nodes2.toString());
                                            nodes3.set(0, startId);
                                            successList.add(nodes3.toString());
                                        }

                                        String rstr = String.valueOf(r);
                                        out.append(rstr);
                                        out.newLine();
                                        resultsList2.add(r);
                                    }
                                    queriesList.get(i).add(executeQuery);
                                    queryTwoList.add(executeQuery);

                                    // Break two out loops.
                                    j = nodesList.size();
                                    break;
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                errorTwoList.get(i).put(String.valueOf(database), e);

                                System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                                result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                                continue;
                            }
                        }
                    }
                }

                // Query 3:
                queryNum = 3;
                if (successList.size() == 0 || successList == null) {
                    executeQuery = "";
                    queriesList.get(i).add(executeQuery);
                    continue;
                } else {
                    List<String> executeList = new ArrayList<>();
                    for (String nodeList : successList) {
                        String tmp = nodeList.substring(1);
                        String nodeListTmp = tmp.substring(0, tmp.length() - 1);
                        List<String> regex = new ArrayList<>();
                        List<String> nodes = new ArrayList<>();
                        regex = Arrays.asList(nodeListTmp.split(", "));
                        regex.forEach((e) -> {
                            nodes.add(e);
                        });
                        if (executeList.size() == 0) {
                            executeList.add(nodes.get(0));
                            executeList.addAll(nodes.subList(1, nodes.size()));
                        } else {
                            executeList.addAll(nodes.subList(1, nodes.size()));
                        }
                    }
                    x1 = "'" + executeList.get(0) + "'";
                    for (int k = 1; k < executeList.size(); k++) {
                        x2 = "'" + executeList.get(k) + "'";
                        executeQuery = generateStrategyQuerySequence(queryNum, "path", x1, x2);

                        out.append("query 3:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        queriesList.get(i).add(executeQuery);
                        try {
                            results3 = connection.getClient().submit(executeQuery).all().get();
                            if (results3.size() == 0 || results3 == null) {
                                System.out.println(connection.getDatabase() + " false : " + executeQuery + "\n");
                                result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                result.append(connection.getDatabase() + " false : " + executeQuery + "\n");
                            } else {
                                for (Result r : results3) {
                                    if (r.getInt() == 0) {
                                        System.out.println(connection.getDatabase() + " false : " + executeQuery + "\n");
                                        result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        result.append(connection.getDatabase() + " false : " + executeQuery + "\n");
                                    }
                                    String rstr = String.valueOf(r);
                                    out.append(connection.getDatabase() + ": " + rstr + "\n");
                                    out.newLine();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();

                            System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                            result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                            continue;
                        }
                    }
                }
            }
            out.close();
            result.close();
        } else if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                List<Result> results, results2, results3;
                List<Object> resultsList = new ArrayList<>();
                List<Object> resultsList2 = new ArrayList<>();
                List<String> nodesList = new ArrayList<>();
                List<String> nodesList2 = new ArrayList<>();
                List<String> successList = new ArrayList<>();
                String x1 = "", x2 = "", x3 = "";
//                resultOneList.get(i).add(database, null);
//                resultTwoList.get(i).add(database, null);
                out.append("========================Query sequence " + i + "=======================\n");

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyQuerySequence(queryNum, "path", "", "");

                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    ResultSet hugeResult = hugeGraph.gremlin(executeQuery).execute();
                    out.append("query 1:" + "\n");
                    out.append(executeQuery+ "\n");
                    System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    queriesList.get(i).add(executeQuery);
                    queryOneList.add(executeQuery);

                    if (hugeResult.data().size() != 0) {
                        hugeResult.data().forEach(r -> {
                            List<String> nodes = new ArrayList<>();
                            HashMap<String, List> rtmp = (HashMap<String, List>) r;
                            rtmp.get("objects").forEach(r2 -> {
                                HashMap<String, List> r2tmp = (HashMap<String, List>) r2;
                                nodes.add(String.valueOf(r2tmp.get("id")));
                            });
                            nodesList.add(nodes.toString());
                            String rpath = "path" + nodes.toString();
                            try {
                                out.append(rpath);
                                out.newLine();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errorOneList.get(i).put(String.valueOf(database), e);

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    continue;
                }
//                resultNodeOneList.get(i).add(database, nodesList);

                // Query 2:
                queryNum = 2;
                if (nodesList.size() == 0 || nodesList == null) {
                    executeQuery = "";
                    queriesList.get(i).add(executeQuery);
                    queryTwoList.add(executeQuery);
//                    resultNodeTwoList.get(i).add(database, nodesList2);
//                    this.successList.get(i).add(database, nodesList2);
                    continue;
                } else {
                    for (int j = 0; j < nodesList.size(); j++) {
                        String nodeIdStr = nodesList.get(j);
                        List<String> nodeIds = new ArrayList<>();
                        if (nodeIdStr != null) {
                            List<String> regex = new ArrayList<>();
                            regex = Arrays.asList(nodeIdStr.split(", "));
                            regex.forEach((e) -> {
                                String node = e.replaceAll("[^0-9]", "");
                                nodeIds.add(node);
                            });
                        }
                        String startId = nodeIds.get(0);
                        for (int k = 1; k < nodeIds.size(); k++) {
                            String id = nodeIds.get(k);
                            executeQuery = generateStrategyQuerySequence(queryNum, "path", id, "");

                            try {
                                GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                                ResultSet hugeResult = hugeGraph.gremlin(executeQuery).execute();
                                out.append("query 2:" + "\n");
                                out.append(executeQuery + "\n");
                                System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");

                                if (hugeResult.data().size() != 0) {
                                    hugeResult.data().forEach(r -> {
                                        List<String> nodes2 = new ArrayList<>();
                                        List<String> nodes3 = new ArrayList<>();
                                        HashMap<String, List> rtmp = (HashMap<String, List>) r;
                                        rtmp.get("objects").forEach(r2 -> {
                                            HashMap<String, List> r2tmp = (HashMap<String, List>) r2;
                                            nodes2.add(String.valueOf(r2tmp.get("id")));
                                            nodes3.add(String.valueOf(r2tmp.get("id")));
                                        });
                                        nodesList2.add(nodes2.toString());
                                        nodes3.set(0, startId);
                                        successList.add(nodes3.toString());
                                        String rpath = "path" + nodes2.toString();
                                        try {
                                            out.append(rpath);
                                            out.newLine();
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                    });
                                    queriesList.get(i).add(executeQuery);
                                    queryTwoList.add(executeQuery);
//                                    resultNodeTwoList.get(i).add(database, nodesList2);
//                                    this.successList.get(i).add(database, successList);

                                    // Break two out loops.
                                    j = nodesList.size();
                                    break;
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
//                                resultNodeTwoList.get(i).add(database, null);
//                                this.successList.get(i).add(database, null);
                                errorTwoList.get(i).put(String.valueOf(database), e);

                                System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                                result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                                continue;
                            }
                        }
                    }
                }

                // Query 3:
                queryNum = 3;
                if (successList.size() == 0 || successList == null) {
                    executeQuery = "";
                    queriesList.get(i).add(executeQuery);
                    continue;
                } else {
                    List<String> executeList = new ArrayList<>();
                    for (String nodeList : successList) {
                        List<String> regex = new ArrayList<>();
                        List<String> nodes = new ArrayList<>();
                        regex = Arrays.asList(nodeList.split(", "));
                        regex.forEach((e) -> {
                            String node = e.replaceAll("[^0-9]", "");
                            nodes.add(node);
                        });
                        if (executeList.size() == 0) {
                            executeList.add(nodes.get(0));
                            executeList.addAll(nodes.subList(1, nodes.size()));
                        } else {
                            executeList.addAll(nodes.subList(1, nodes.size()));
                        }
                    }
                    x1 = executeList.get(0);
                    for (int k = 1; k < executeList.size(); k++) {
                        x2 = executeList.get(k);
                        executeQuery = generateStrategyQuerySequence(queryNum, "path", x1, x2);

                        out.append("query 3:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        queriesList.get(i).add(executeQuery);
                        try {
                            GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                            ResultSet hugeResult = hugeGraph.gremlin(executeQuery).execute();
                            if (hugeResult.data().size() == 0 || hugeResult.data() == null) {
                                System.out.println(connection.getDatabase() + " false : " + executeQuery + "\n");
                                result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                result.append(connection.getDatabase() + " false : " + executeQuery + "\n");
                            } else {
                                if ((int) hugeResult.data().get(0) == 0) {
                                    System.out.println(connection.getDatabase() + " false : " + executeQuery + "\n");
                                    result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                    result.append(connection.getDatabase() + " false : " + executeQuery + "\n");
                                }
                                String res = "size: " + String.valueOf((int) hugeResult.data().get(0));
                                out.append(connection.getDatabase() + ": " + res + "\n");
                                out.newLine();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();

                            System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                            result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                            continue;
                        }
                    }
                }
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                List<Result> results, results2, results3;
                List<Object> resultsList = new ArrayList<>();
                List<Object> resultsList2 = new ArrayList<>();
                List<String> nodesList = new ArrayList<>();
                List<String> nodesList2 = new ArrayList<>();
                List<String> successList = new ArrayList<>();
                String x1 = "", x2 = "", x3 = "";
                out.append("========================Query sequence " + i + "=======================\n");

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyQuerySequence(queryNum, "path", "", "");
                try {
                    results = connection.getClient().submit(executeQuery).all().get();
                    out.append("query 1:" + "\n");
                    out.append(executeQuery+ "\n");
                    System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    queriesList.get(i).add(executeQuery);
                    queryOneList.add(executeQuery);

                    if (results.size() != 0) {
                        for (Result r : results) {
                            String res = String.valueOf(r.getPath());
                            if (res != null) {
                                List<String> regex = new ArrayList<>();
                                List<String> nodes = new ArrayList<>();
                                regex = Arrays.asList(res.split(", "));
                                regex.forEach((e) -> {
                                    String node = e.replaceAll("[^0-9]", "");
                                    nodes.add(node);
                                });
                                nodesList.add(nodes.toString());
                            }
                            String rstr = String.valueOf(r);
                            out.append(rstr);
                            out.newLine();
                            resultsList.add(r);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errorOneList.get(i).put(String.valueOf(database), e);

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    continue;
                }
//                resultOneList.get(i).add(database, resultsList);
//                resultNodeOneList.get(i).add(database, nodesList);

                // Query 2:
                queryNum = 2;
                if (nodesList.size() == 0 || nodesList == null) {
                    executeQuery = "";
                    queriesList.get(i).add(executeQuery);
                    queryTwoList.add(executeQuery);
//                    resultTwoList.get(i).add(database, resultsList2);
//                    resultNodeTwoList.get(i).add(database, nodesList2);
//                    this.successList.get(i).add(database, nodesList2);
                    continue;
                } else {
                    for (int j = 0; j < nodesList.size(); j++) {
                        String nodeIdStr = nodesList.get(j);
                        List<String> nodeIds = new ArrayList<>();
                        if (nodeIdStr != null) {
                            List<String> regex = new ArrayList<>();
                            regex = Arrays.asList(nodeIdStr.split(", "));
                            regex.forEach((e) -> {
                                String node = e.replaceAll("[^0-9]", "");
                                nodeIds.add(node);
                            });
                        }
                        String startId = nodeIds.get(0);
                        for (int k = 1; k < nodeIds.size(); k++) {
                            String id = nodeIds.get(k);
                            executeQuery = generateStrategyQuerySequence(queryNum, "path", id, "");

                            try {
                                results2 = connection.getClient().submit(executeQuery).all().get();
                                out.append("query 2:" + "\n");
                                out.append(executeQuery + "\n");
                                System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                if (results2.size() == 0 || results2 == null) {
                                    continue;
                                } else {
                                    for (Result r : results2) {
                                        String res = String.valueOf(r.getPath());
                                        if (res != null) {
                                            List<String> regex2 = new ArrayList<>();
                                            List<String> nodes2 = new ArrayList<>();
                                            List<String> nodes3 = new ArrayList<>();
                                            regex2 = Arrays.asList(res.split(", "));
                                            regex2.forEach((e) -> {
                                                String node = e.replaceAll("[^0-9]", "");
                                                nodes2.add(node);
                                                nodes3.add(node);
                                            });
                                            nodesList2.add(nodes2.toString());
                                            nodes3.set(0, startId);
                                            successList.add(nodes3.toString());
                                        }

                                        String rstr = String.valueOf(r);
                                        out.append(rstr);
                                        out.newLine();
                                        resultsList2.add(r);
                                    }
                                    queriesList.get(i).add(executeQuery);
                                    queryTwoList.add(executeQuery);
//                                    resultTwoList.get(i).add(database, resultsList2);
//                                    resultNodeTwoList.get(i).add(database, nodesList2);
//                                    this.successList.get(i).add(database, successList);

                                    // Break two out loops.
                                    j = nodesList.size();
                                    break;
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
//                                resultTwoList.get(i).add(database, null);
//                                resultNodeTwoList.get(i).add(database, null);
//                                this.successList.get(i).add(database, null);
                                errorTwoList.get(i).put(String.valueOf(database), e);

                                System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                                result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                                continue;
                            }
                        }
                    }
                }

                // Query 3:
                queryNum = 3;
                if (successList.size() == 0 || successList == null) {
                    executeQuery = "";
                    queriesList.get(i).add(executeQuery);
                    continue;
                } else {
                    List<String> executeList = new ArrayList<>();
                    for (String nodeList : successList) {
                        List<String> regex = new ArrayList<>();
                        List<String> nodes = new ArrayList<>();
                        regex = Arrays.asList(nodeList.split(", "));
                        regex.forEach((e) -> {
                            String node = e.replaceAll("[^0-9]", "");
                            nodes.add(node);
                        });
                        if (executeList.size() == 0) {
                            executeList.add(nodes.get(0));
                            executeList.addAll(nodes.subList(1, nodes.size()));
                        } else {
                            executeList.addAll(nodes.subList(1, nodes.size()));
                        }
                    }
                    x1 = executeList.get(0);
                    for (int k = 1; k < executeList.size(); k++) {
                        x2 = executeList.get(k);
                        executeQuery = generateStrategyQuerySequence(queryNum, "path", x1, x2);

                        out.append("query 3:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        queriesList.get(i).add(executeQuery);
                        try {
                            results3 = connection.getClient().submit(executeQuery).all().get();
                            if (results3.size() == 0 || results3 == null) {
                                System.out.println(connection.getDatabase() + " false : " + executeQuery + "\n");
                                result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                result.append(connection.getDatabase() + " false : " + executeQuery + "\n");
                            } else {
                                for (Result r : results3) {
                                    if (r.getInt() == 0) {
                                        System.out.println(connection.getDatabase() + " false : " + executeQuery + "\n");
                                        result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        result.append(connection.getDatabase() + " false : " + executeQuery + "\n");
                                    }
                                    String rstr = String.valueOf(r);
                                    out.append(connection.getDatabase() + ": " + rstr + "\n");
                                    out.newLine();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();

                            System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                            result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                            continue;
                        }
                    }
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery2(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr2" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr2" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 5) {
            int lIndex = 0, rIndex = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5, results6;
                List<String> VAllIds = new ArrayList<>();
                String x1 = "", x2 = "", x3 = "";
                int variant = 1;

                System.out.println("Print lIndex here: " + lIndex + "\n");
                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(r.getString());
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                if (lIndex >= VAllIds.size()) {
                    continue;
                }
                // Query 2:
                queryNum = 2;
                for (int x = lIndex + 1; x < VAllIds.size(); x++) {
                    for (int y = x + 1; y < VAllIds.size(); y++) {
                        System.out.println("Check queries 2 iterations: " + x + ", " + y);
                        executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + VAllIds.get(x) + "'", "'" + VAllIds.get(y) + "'");
                        try {
                            results2 = connection.getClient().submit(executeQuery).all().get();
                            if (results2.size() == 0 || results2 == null) {
                                continue;
                            } else {
                                for (Result r : results2) {
                                    if (r.getInt() != 0) {
                                        x1 = VAllIds.get(x);
                                        x2 = VAllIds.get(y);
                                        lIndex = x;
                                        rIndex = y;
                                        queriesList.get(i).add(executeQuery);
                                        out.append("query 2:" + "\n");
                                        out.append(executeQuery + "\n");
                                        System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        // Break two out loops.
                                        y = VAllIds.size();
                                        x = VAllIds.size();
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();

                            System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                            result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                        }
                    }
                }

                if (x1 == "" && x2 == "") continue;
                // Query 3:
                queryNum = 3;
                for (int x = 0; x < VAllIds.size(); x++) {
                    if (x == lIndex)
                        continue;
                    System.out.println("Check queries 3&4 iterations: " + x);
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x1 + "'", "'" + VAllIds.get(x) + "'");
                    try {
                        results3 = connection.getClient().submit(executeQuery).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            continue;
                        } else {
                            for (Result r : results3) {
                                if (r.getInt() == 0) {
                                    String executeQuery2 = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x2 + "'", "'" + VAllIds.get(x) + "'");
                                    results4 = connection.getClient().submit(executeQuery2).all().get();

                                    if (results4.size() == 0 || results4 == null) {
                                        continue;
                                    } else {
                                        for (Result r2: results4) {
                                            if (r2.getInt() == 0) {
                                                x3 = VAllIds.get(x);
                                                queriesList.get(i).add(executeQuery);
                                                queriesList.get(i).add(executeQuery2);
                                                out.append("query 3:" + "\n");
                                                out.append(executeQuery + "\n");
                                                System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                                out.append("query 4:" + "\n");
                                                out.append(executeQuery2 + "\n");
                                                System.out.println("Query sequence " + i + ", 4: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                                // Break out loop
                                                x = VAllIds.size();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (x3 == "") continue;
                // Query 4:
                queryNum = 4;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x2 + "'", "'" + x3 + "'");
                try {
                    results5 = connection.getClient().submit(executeQuery).all().get();
                    if (results5.size() == 0 || results5 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        queriesList.get(i).add(executeQuery);
                        out.append("query 5:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 5: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 5: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 5:
                queryNum = 5;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x1 + "'", "'" + x3 + "'");
                try {
                    results6 = connection.getClient().submit(executeQuery).all().get();
                    if (results6.size() == 0 || results6 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results6) {
                            if (r.getInt() == 0) {
                                result.append("Query sequence " + i + ": false\n");
                                System.out.println("Query sequence " + i + ": false\n");
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 6:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 6: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            System.out.println("Result is: " + r.getInt() + "\n");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 6: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 4) {
            int lIndex = 0, rIndex = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5, results6;
                List<String> VAllIds = new ArrayList<>();
                String x1 = "", x2 = "", x3 = "";
                int variant = 1;

                System.out.println("Print lIndex here: " + lIndex + "\n");
                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(r.getString());
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                if (lIndex >= VAllIds.size()) {
                    continue;
                }
                // Query 2:
                queryNum = 2;
                for (int x = lIndex + 1; x < VAllIds.size(); x++) {
                    for (int y = x + 1; y < VAllIds.size(); y++) {
                        System.out.println("Check queries 2 iterations: " + x + ", " + y);
                        executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + VAllIds.get(x) + "'", "'" + VAllIds.get(y) + "'");
                        try {
                            results2 = connection.getClient().submit(executeQuery).all().get();
                            if (results2.size() == 0 || results2 == null) {
                                continue;
                            } else {
                                for (Result r : results2) {
                                    if (r.getInt() != 0) {
                                        x1 = VAllIds.get(x);
                                        x2 = VAllIds.get(y);
                                        lIndex = x;
                                        rIndex = y;
                                        queriesList.get(i).add(executeQuery);
                                        out.append("query 2:" + "\n");
                                        out.append(executeQuery + "\n");
                                        System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        // Break two out loops.
                                        y = VAllIds.size();
                                        x = VAllIds.size();
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();

                            System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                            result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                        }
                    }
                }

                if (x1 == "" && x2 == "") continue;
                // Query 3:
                queryNum = 3;
                for (int x = 0; x < VAllIds.size(); x++) {
                    if (x == lIndex)
                        continue;
                    System.out.println("Check queries 3&4 iterations: " + x);
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x1 + "'", "'" + VAllIds.get(x) + "'");
                    try {
                        results3 = connection.getClient().submit(executeQuery).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            continue;
                        } else {
                            for (Result r : results3) {
                                if (r.getInt() == 0) {
                                    String executeQuery2 = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x2 + "'", "'" + VAllIds.get(x) + "'");
                                    results4 = connection.getClient().submit(executeQuery2).all().get();

                                    if (results4.size() == 0 || results4 == null) {
                                        continue;
                                    } else {
                                        for (Result r2: results4) {
                                            if (r2.getInt() == 0) {
                                                x3 = VAllIds.get(x);
                                                queriesList.get(i).add(executeQuery);
                                                queriesList.get(i).add(executeQuery2);
                                                out.append("query 3:" + "\n");
                                                out.append(executeQuery + "\n");
                                                System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                                out.append("query 4:" + "\n");
                                                out.append(executeQuery2 + "\n");
                                                System.out.println("Query sequence " + i + ", 4: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                                // Break out loop
                                                x = VAllIds.size();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (x3 == "") continue;
                // Query 4:
                queryNum = 4;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x2 + "'", "'" + x3 + "'");
                try {
                    results5 = connection.getClient().submit(executeQuery).all().get();
                    if (results5.size() == 0 || results5 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        queriesList.get(i).add(executeQuery);
                        out.append("query 5:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 5: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 5: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 5:
                queryNum = 5;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x1 + "'", "'" + x3 + "'");
                try {
                    results6 = connection.getClient().submit(executeQuery).all().get();
                    if (results6.size() == 0 || results6 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results6) {
                            if (r.getInt() == 0) {
                                result.append("Query sequence " + i + ": false\n");
                                System.out.println("Query sequence " + i + ": false\n");
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 6:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 6: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            System.out.println("Result is: " + r.getInt() + "\n");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 6: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 0) {
            int lIndex = 0, rIndex = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3, hugeResult4, hugeResult5, hugeResult6;
                List<String> VAllIds = new ArrayList<>();
                String x1 = "", x2 = "", x3 = "";
                int variant = 1;

                System.out.println("Print lIndex here: " + lIndex + "\n");
                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        hugeResult1.data().forEach(r -> {
                            VAllIds.add(String.valueOf(r));
                        });
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                if (lIndex >= VAllIds.size()) {
                    continue;
                }
                // Query 2:
                queryNum = 2;
                for (int x = lIndex + 1; x < VAllIds.size(); x++) {
                    for (int y = x + 1; y < VAllIds.size(); y++) {
                        System.out.println("Check queries 2 iterations: " + x + ", " + y);
                        executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, VAllIds.get(x), VAllIds.get(y));
                        try {
                            GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                            hugeResult2 = hugeGraph.gremlin(executeQuery).execute();
                            if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                                continue;
                            } else {
                                if ((int) hugeResult2.data().get(0) != 0) {
                                    x1 = VAllIds.get(x);
                                    x2 = VAllIds.get(y);
                                    lIndex = x;
                                    rIndex = y;
                                    queriesList.get(i).add(executeQuery);
                                    out.append("query 2:" + "\n");
                                    out.append(executeQuery + "\n");
                                    System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                    // Break two out loops.
                                    y = VAllIds.size();
                                    x = VAllIds.size();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();

                            System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                            result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                        }
                    }
                }

                if (x1 == "" && x2 == "") continue;
                // Query 3:
                queryNum = 3;
                for (int x = 0; x < VAllIds.size(); x++) {
                    if (x == lIndex)
                        continue;
                    System.out.println("Check queries 3&4 iterations: " + x);
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x1, VAllIds.get(x));
                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult3 = hugeGraph.gremlin(executeQuery).execute();
                        if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                            continue;
                        } else {
                            if ((int) hugeResult3.data().get(0) == 0) {
                                String executeQuery2 = generateStrategyMRQuerySequence(queryNum, "path", variant, x2, VAllIds.get(x));
                                hugeResult4 = hugeGraph.gremlin(executeQuery2).execute();

                                if (hugeResult4.data().size() == 0 || hugeResult4.data() == null) {
                                    continue;
                                } else {
                                    if ((int) hugeResult4.data().get(0) == 0) {
                                        x3 = VAllIds.get(x);
                                        queriesList.get(i).add(executeQuery);
                                        queriesList.get(i).add(executeQuery2);
                                        out.append("query 3:" + "\n");
                                        out.append(executeQuery + "\n");
                                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        out.append("query 4:" + "\n");
                                        out.append(executeQuery2 + "\n");
                                        System.out.println("Query sequence " + i + ", 4: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        // Break out loop
                                        x = VAllIds.size();
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (x3 == "") continue;
                // Query 4:
                queryNum = 4;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x2, x3);
                try {
                    String edgeLabel = String.valueOf(Randomly.getInteger(50, 1000));
//                    executeQuery = "g." + "V(" + x2 + ").as(\"" + x2 + "\").V(" + x3 + ").as(\"" + x3 +
//                            "\").addE(\"" + edgeLabel + "\").from(\"" + x2 + "\").to(\"" + x3 + "\")";
//                    GraphManager hugeGraph = connection.getHugespecial().graph();
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult5 = hugeGraph.gremlin(executeQuery).execute();

                    if (hugeResult5.data().size() == 0 || hugeResult5.data() == null) {
                        continue;
                    } else {
                        queriesList.get(i).add(executeQuery);
                        out.append("query 5:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 5: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 5: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 5:
                queryNum = 5;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x1, x3);
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult6 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult6.data().size() == 0 || hugeResult6.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        if ((int) hugeResult6.data().get(0) == 0) {
                            result.append("Query sequence " + i + ": false\n");
                            System.out.println("Query sequence " + i + ": false\n");
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 6:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 6: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        System.out.println("Result is: " + hugeResult6.data().get(0) + "\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 6: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            int lIndex = 0, rIndex = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5, results6;
                List<String> VAllIds = new ArrayList<>();
                String x1 = "", x2 = "", x3 = "";
                int variant = 1;

                System.out.println("Print lIndex here: " + lIndex + "\n");
                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                if (lIndex >= VAllIds.size()) {
                    continue;
                }
                // Query 2:
                queryNum = 2;
                for (int x = lIndex + 1; x < VAllIds.size(); x++) {
                    for (int y = x + 1; y < VAllIds.size(); y++) {
                        System.out.println("Check queries 2 iterations: " + x + ", " + y);
                        executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, VAllIds.get(x), VAllIds.get(y));
                        try {
                            results2 = connection.getClient().submit(executeQuery).all().get();
                            if (results2.size() == 0 || results2 == null) {
                                continue;
                            } else {
                                for (Result r : results2) {
                                    if (r.getInt() != 0) {
                                        x1 = VAllIds.get(x);
                                        x2 = VAllIds.get(y);
                                        lIndex = x;
                                        rIndex = y;
                                        queriesList.get(i).add(executeQuery);
                                        out.append("query 2:" + "\n");
                                        out.append(executeQuery + "\n");
                                        System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        // Break two out loops.
                                        y = VAllIds.size();
                                        x = VAllIds.size();
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();

                            System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                            result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                        }
                    }
                }

                if (x1 == "" && x2 == "") continue;
                // Query 3:
                queryNum = 3;
                for (int x = 0; x < VAllIds.size(); x++) {
                    if (x == lIndex)
                        continue;
                    System.out.println("Check queries 3&4 iterations: " + x);
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x1, VAllIds.get(x));
                    try {
                        results3 = connection.getClient().submit(executeQuery).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            continue;
                        } else {
                            for (Result r : results3) {
                                if (r.getInt() == 0) {
                                    String executeQuery2 = generateStrategyMRQuerySequence(queryNum, "path", variant, x2, VAllIds.get(x));
                                    results4 = connection.getClient().submit(executeQuery2).all().get();

                                    if (results4.size() == 0 || results4 == null) {
                                        continue;
                                    } else {
                                        for (Result r2: results4) {
                                            if (r2.getInt() == 0) {
                                                x3 = VAllIds.get(x);
                                                queriesList.get(i).add(executeQuery);
                                                queriesList.get(i).add(executeQuery2);
                                                out.append("query 3:" + "\n");
                                                out.append(executeQuery + "\n");
                                                System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                                out.append("query 4:" + "\n");
                                                out.append(executeQuery2 + "\n");
                                                System.out.println("Query sequence " + i + ", 4: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                                // Break out loop
                                                x = VAllIds.size();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (x3 == "") continue;
                // Query 4:
                queryNum = 4;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x2, x3);
                try {
                    results5 = connection.getClient().submit(executeQuery).all().get();
                    if (results5.size() == 0 || results5 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        queriesList.get(i).add(executeQuery);
                        out.append("query 5:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 5: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 5: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 5:
                queryNum = 5;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x1, x3);
                try {
                    results6 = connection.getClient().submit(executeQuery).all().get();
                    if (results6.size() == 0 || results6 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results6) {
                            if (r.getInt() == 0) {
                                result.append("Query sequence " + i + ": false\n");
                                System.out.println("Query sequence " + i + ": false\n");
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 6:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 6: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            System.out.println("Result is: " + r.getInt() + "\n");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 6: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == -1) {        // Deprecated, no differential testing for different node ids
            for (int i = 0 ; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5, results6;

                // Query 1:
                executeQuery = queriesList.get(i).get(0);
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        continue;
                    } else {
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Query 2:
                if (queriesList.get(i).get(1) == null) continue;
                else executeQuery = queriesList.get(i).get(1);
                try {
                    results2 = connection.getClient().submit(executeQuery).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        continue;
                    } else {
                        for (Result r : results2) {
                            if (r.getInt() != 0) {
                                out.append("query 2:" + "\n");
                                out.append(executeQuery + "\n");
                                System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Query 3:
                if (queriesList.get(i).get(2) == null) continue;
                else executeQuery = queriesList.get(i).get(2);
                try {
                    results3 = connection.getClient().submit(executeQuery).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        continue;
                    } else {
                        for (Result r : results3) {
                            if (r.getInt() == 0) {
                                String executeQuery2 = queriesList.get(i).get(3);
                                results4 = connection.getClient().submit(executeQuery2).all().get();

                                if (results4.size() == 0 || results4 == null) {
                                    continue;
                                } else {
                                    for (Result r2: results4) {
                                        if (r2.getInt() == 0) {
                                            out.append("query 3:" + "\n");
                                            out.append(executeQuery + "\n");
                                            System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                            out.append("query 4:" + "\n");
                                            out.append(executeQuery2 + "\n");
                                            System.out.println("Query sequence " + i + ", 4: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Query 4:
                if (queriesList.get(i).get(4) == null) continue;
                else executeQuery = queriesList.get(i).get(4);
                try {
                    results5 = connection.getClient().submit(executeQuery).all().get();
                    if (results5.size() == 0 || results5 == null) {
                        continue;
                    } else {
                        out.append("query 5:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 5: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Query 5:
                if (queriesList.get(i).get(5) == null) continue;
                else executeQuery = queriesList.get(i).get(5);
                try {
                    results6 = connection.getClient().submit(executeQuery).all().get();
                    if (results6.size() == 0 || results6 == null) {
                        continue;
                    } else {
                        for (Result r : results6) {
                            if (r.getInt() == 0) {
                                result.append("Query sequence " + i + ": false\n");
                                System.out.println("Query sequence " + i + ": false\n");
                            }
                            out.append("query 6:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 6: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            System.out.println("Result is: " + r.getInt() + "\n");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery3(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr3" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr3" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String repeatQuery = "";
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                int size1 = -1, size2 = -1;
                String id1 = "", id2 = "", id3 = "", id4 = "";
                int variant = 2;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = r.getInt();
                            queriesList.get(i).add(executeQuery);
                            repeatQuery = executeQuery;
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results2) {
                            String tmp = r.getString().substring(2);
                            String res = tmp.substring(0, tmp.length() - 1);
                            id1 = res;
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        for (Result r : results2) {
                            String tmp = r.getString().substring(2);
                            String res = tmp.substring(0, tmp.length() - 1);
                            id2 = res;
                        }
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + id1 + "'", "'" + id2 + "'");
                try {
                    results3 = connection.getClient().submit(executeQuery).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        queriesList.get(i).add(executeQuery);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 4:
                queryNum = 4;
                try {
                    results4 = connection.getClient().submit(repeatQuery).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        queriesList.get(i).add(repeatQuery);
                        continue;
                    } else {
                        for (Result r : results4) {
                            size2 = r.getInt();
                            queriesList.get(i).add(repeatQuery);
                            out.append("query 4:" + "\n");
                            out.append(repeatQuery + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + repeatQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + repeatQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + repeatQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                if (size1 != size2) {
                    result.append("Query sequence " + i + ": false\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size1 + ", " + size2 + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 4) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String repeatQuery = "";
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                int size1 = -1, size2 = -1;
                String id1 = "", id2 = "", id3 = "", id4 = "";
                int variant = 2;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = r.getInt();
                            queriesList.get(i).add(executeQuery);
                            repeatQuery = executeQuery;
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results2) {
                            String tmp = r.getString().substring(2);
                            String res = tmp.substring(0, tmp.length() - 1);
                            id1 = res;
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        for (Result r : results2) {
                            String tmp = r.getString().substring(2);
                            String res = tmp.substring(0, tmp.length() - 1);
                            id2 = res;
                        }
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + id1 + "'", "'" + id2 + "'");
                try {
                    results3 = connection.getClient().submit(executeQuery).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        queriesList.get(i).add(executeQuery);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 4:
                queryNum = 4;
                try {
                    results4 = connection.getClient().submit(repeatQuery).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        queriesList.get(i).add(repeatQuery);
                        continue;
                    } else {
                        for (Result r : results4) {
                            size2 = r.getInt();
                            queriesList.get(i).add(repeatQuery);
                            out.append("query 4:" + "\n");
                            out.append(repeatQuery + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + repeatQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + repeatQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + repeatQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                if (size1 != size2) {
                    result.append("Query sequence " + i + ": false\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size1 + ", " + size2 + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String repeatQuery = "";
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                int size1 = -1, size2 = -1;
                String id1 = "", id2 = "", id3 = "", id4 = "";
                int variant = 2;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = r.getInt();
                            queriesList.get(i).add(executeQuery);
                            repeatQuery = executeQuery;
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results2) {
                            String res = r.getString();
                            id1 = res.replaceAll("[^0-9]", "");
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        for (Result r : results2) {
                            String res = r.getString();
                            id2 = res.replaceAll("[^0-9]", "");
                        }
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, id1, id2);
                try {
                    results3 = connection.getClient().submit(executeQuery).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        queriesList.get(i).add(executeQuery);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 4:
                queryNum = 4;
                try {
                    results4 = connection.getClient().submit(repeatQuery).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        queriesList.get(i).add(repeatQuery);
                        continue;
                    } else {
                        for (Result r : results4) {
                            size2 = r.getInt();
                            queriesList.get(i).add(repeatQuery);
                            out.append("query 4:" + "\n");
                            out.append(repeatQuery + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + repeatQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + repeatQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + repeatQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                if (size1 != size2) {
                    result.append("Query sequence " + i + ": false\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size1 + ", " + size2 + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery4(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr4" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr4" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VAllIds = new ArrayList<>();
                String x1 = "", x2 = "", x3 = "";
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 3;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(r.getString());
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    for (int y = 0; y < VAllIds.size(); y++) {
                        executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + VAllIds.get(x) + "'", "'" + VAllIds.get(y) + "'");
                        try {
                            results2 = connection.getClient().submit(executeQuery).all().get();
                            if (results2.size() == 0 || results2 == null) {
                                executeQuery = "";
                                queriesList.get(i).add(executeQuery);
                                continue;
                            } else {
                                for (Result r : results2) {
                                    if (r.getInt() != 0) {
                                        index = x;
                                        x1 = VAllIds.get(x);
                                        x2 = VAllIds.get(y);
                                        size1 = r.getInt();
                                        queriesList.get(i).add(executeQuery);
                                        out.append("query 2:" + "\n");
                                        out.append(executeQuery + "\n");
                                        System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        x = VAllIds.size();
                                        y = VAllIds.size();
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();

                            System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                            result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                        }
                    }
                }

                if (x2.equals(""))
                    continue;
                // Query 3:
                queryNum = 3;
                for (int z = 0; z < VAllIds.size(); z++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x2 + "'", "'" + VAllIds.get(z) + "'");
                    try {
                        results3 = connection.getClient().submit(executeQuery).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            for (Result r : results3) {
                                if (r.getInt() != 0) {
                                    x3 = VAllIds.get(z);
                                    size2 = r.getInt();
                                    queriesList.get(i).add(executeQuery);
                                    out.append("query 3:" + "\n");
                                    out.append(executeQuery + "\n");
                                    System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                    z = VAllIds.size();
                                } else {
                                    x3 = VAllIds.get((int) Randomly.getInteger(VAllIds.size() - 1));
                                    size2 = r.getInt();
                                    System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (x1.equals("") || x2.equals("") || x3.equals(""))
                    continue;
                // Query 4:
                queryNum = 4;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x1 + "'", "'" + x2 + "'", "'" + x3 + "'");
                try {
                    results4 = connection.getClient().submit(executeQuery).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results4) {
                            size3 = r.getInt();
                            queriesList.get(i).add(executeQuery);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Compare " + String.valueOf(size1) + "*" + String.valueOf(size2) + " with " + String.valueOf(size3));
                if (size1*size2 != size3) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size1 + " * " + size2 + " != " + size3 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size1 + " * " + size2 + " != " + size3 + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 4) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VAllIds = new ArrayList<>();
                String x1 = "", x2 = "", x3 = "";
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 3;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(r.getString());
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    for (int y = 0; y < VAllIds.size(); y++) {
                        executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + VAllIds.get(x) + "'", "'" + VAllIds.get(y) + "'");
                        try {
                            results2 = connection.getClient().submit(executeQuery).all().get();
                            if (results2.size() == 0 || results2 == null) {
                                executeQuery = "";
                                queriesList.get(i).add(executeQuery);
                                continue;
                            } else {
                                for (Result r : results2) {
                                    if (r.getInt() != 0) {
                                        index = x;
                                        x1 = VAllIds.get(x);
                                        x2 = VAllIds.get(y);
                                        size1 = r.getInt();
                                        queriesList.get(i).add(executeQuery);
                                        out.append("query 2:" + "\n");
                                        out.append(executeQuery + "\n");
                                        System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        x = VAllIds.size();
                                        y = VAllIds.size();
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();

                            System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                            result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                        }
                    }
                }

                if (x2.equals(""))
                    continue;
                // Query 3:
                queryNum = 3;
                for (int z = 0; z < VAllIds.size(); z++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x2 + "'", "'" + VAllIds.get(z) + "'");
                    try {
                        results3 = connection.getClient().submit(executeQuery).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            for (Result r : results3) {
                                if (r.getInt() != 0) {
                                    x3 = VAllIds.get(z);
                                    size2 = r.getInt();
                                    queriesList.get(i).add(executeQuery);
                                    out.append("query 3:" + "\n");
                                    out.append(executeQuery + "\n");
                                    System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                    z = VAllIds.size();
                                } else {
                                    x3 = VAllIds.get((int) Randomly.getInteger(VAllIds.size() - 1));
                                    size2 = r.getInt();
                                    System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (x1.equals("") || x2.equals("") || x3.equals(""))
                    continue;
                // Query 4:
                queryNum = 4;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x1 + "'", "'" + x2 + "'", "'" + x3 + "'");
                try {
                    results4 = connection.getClient().submit(executeQuery).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results4) {
                            size3 = r.getInt();
                            queriesList.get(i).add(executeQuery);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Compare " + String.valueOf(size1) + "*" + String.valueOf(size2) + " with " + String.valueOf(size3));
                if (size1*size2 != size3) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size1 + " * " + size2 + " != " + size3 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size1 + " * " + size2 + " != " + size3 + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 3) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3, hugeResult4;
                List<String> VAllIds = new ArrayList<>();
                String x1 = "", x2 = "", x3 = "";
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 3;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        hugeResult1.data().forEach(r -> {
                            VAllIds.add(String.valueOf(r));
                        });
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    for (int y = 0; y < VAllIds.size(); y++) {
                        executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, VAllIds.get(x), VAllIds.get(y));
                        try {
                            GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                            hugeResult2 = hugeGraph.gremlin(executeQuery).execute();
                            if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                                executeQuery = "";
                                queriesList.get(i).add(executeQuery);
                                continue;
                            } else {
                                if ((int) hugeResult2.data().get(0) != 0) {
                                    index = x;
                                    x1 = VAllIds.get(x);
                                    x2 = VAllIds.get(y);
                                    size1 = (int) hugeResult2.data().get(0);
                                    queriesList.get(i).add(executeQuery);
                                    out.append("query 2:" + "\n");
                                    out.append(executeQuery + "\n");
                                    System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                    x = VAllIds.size();
                                    y = VAllIds.size();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();

                            System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                            result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                        }
                    }
                }

                if (x2.equals(""))
                    continue;
                // Query 3:
                queryNum = 3;
                for (int z = 0; z < VAllIds.size(); z++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x2, VAllIds.get(z));
                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult3 = hugeGraph.gremlin(executeQuery).execute();
                        if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            if ((int) hugeResult3.data().get(0) != 0) {
                                x3 = VAllIds.get(z);
                                size2 = (int) hugeResult3.data().get(0);
                                queriesList.get(i).add(executeQuery);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                z = VAllIds.size();
                            } else {
                                x3 = VAllIds.get((int) Randomly.getInteger(VAllIds.size() - 1));
                                size2 = (int) hugeResult3.data().get(0);
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");

                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (x1.equals("") || x2.equals("") || x3.equals(""))
                    continue;
                // Query 4:
                queryNum = 4;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x1, x2, x3);
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult4 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult4.data().size() == 0 || hugeResult4.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        size3 = (int) hugeResult4.data().get(0);
                        queriesList.get(i).add(executeQuery);
                        out.append("query 4:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Compare " + String.valueOf(size1) + "*" + String.valueOf(size2) + " with " + String.valueOf(size3));
                if (size1*size2 != size3) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size1 + " * " + size2 + " != " + size3 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size1 + " * " + size2 + " != " + size3 + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VAllIds = new ArrayList<>();
                String x1 = "", x2 = "", x3 = "";
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 3;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    for (int y = 0; y < VAllIds.size(); y++) {
                        executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, VAllIds.get(x), VAllIds.get(y));
                        try {
                            results2 = connection.getClient().submit(executeQuery).all().get();
                            if (results2.size() == 0 || results2 == null) {
                                executeQuery = "";
                                queriesList.get(i).add(executeQuery);
                                continue;
                            } else {
                                for (Result r : results2) {
                                    if (r.getInt() != 0) {
                                        index = x;
                                        x1 = VAllIds.get(x);
                                        x2 = VAllIds.get(y);
                                        size1 = r.getInt();
                                        queriesList.get(i).add(executeQuery);
                                        out.append("query 2:" + "\n");
                                        out.append(executeQuery + "\n");
                                        System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        x = VAllIds.size();
                                        y = VAllIds.size();
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();

                            System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                            result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                        }
                    }
                }

                if (x2.equals(""))
                    continue;
                // Query 3:
                queryNum = 3;
                for (int z = 0; z < VAllIds.size(); z++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x2, VAllIds.get(z));
                    try {
                        results3 = connection.getClient().submit(executeQuery).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            for (Result r : results3) {
                                if (r.getInt() != 0) {
                                    x3 = VAllIds.get(z);
                                    size2 = r.getInt();
                                    queriesList.get(i).add(executeQuery);
                                    out.append("query 3:" + "\n");
                                    out.append(executeQuery + "\n");
                                    System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                    z = VAllIds.size();
                                } else {
                                    x3 = VAllIds.get((int) Randomly.getInteger(VAllIds.size() - 1));
                                    size2 = r.getInt();
                                    System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (x1.equals("") || x2.equals("") || x3.equals(""))
                    continue;
                // Query 4:
                queryNum = 4;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x1, x2, x3);
                try {
                    results4 = connection.getClient().submit(executeQuery).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results4) {
                            size3 = r.getInt();
                            queriesList.get(i).add(executeQuery);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Compare " + String.valueOf(size1) + "*" + String.valueOf(size2) + " with " + String.valueOf(size3));
                if (size1*size2 != size3) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size1 + " * " + size2 + " != " + size3 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size1 + " * " + size2 + " != " + size3 + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery5(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr5" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr5" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 4) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5;
                List<String> VAllIds = new ArrayList<>();
                String x1 = "", x2 = "", x3 = "";
                int size1 = -1, size2 = -1;
                int variant = 4;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(r.getString());
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    for (int y = 0; y < VAllIds.size(); y++) {
                        executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + VAllIds.get(x) + "'", "'" + VAllIds.get(y) + "'");
                        try {
                            results2 = connection.getClient().submit(executeQuery).all().get();
                            if (results2.size() == 0 || results2 == null) {
                                executeQuery = "";
                                queriesList.get(i).add(executeQuery);
                                continue;
                            } else {
                                for (Result r : results2) {
                                    if (r.getInt() != 0) {
                                        index = x;
                                        x1 = VAllIds.get(x);
                                        x2 = VAllIds.get(y);
                                        size1 = r.getInt();
                                        queriesList.get(i).add(executeQuery);
                                        out.append("query 2:" + "\n");
                                        out.append(executeQuery + "\n");
                                        System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        x = VAllIds.size();
                                        y = VAllIds.size();
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();

                            System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                            result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                        }
                    }
                }

                if (x2.equals(""))
                    continue;
                // Query 3:
                queryNum = 3;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results3 = connection.getClient().submit(executeQuery).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results3) {
                            String tmp = r.getString().substring(2);
                            String res = tmp.substring(0, tmp.length() - 1);
                            x3 = res;
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 4:
                queryNum = 4;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x2 + "'", "'" + x3 + "'");
                try {
                    results4 = connection.getClient().submit(executeQuery).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        queriesList.get(i).add(executeQuery);
                        out.append("query 4:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 5:
                queryNum = 5;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x1 + "'", "'" + x3 + "'");
                try {
                    results5 = connection.getClient().submit(executeQuery).all().get();
                    if (results5.size() == 0 || results5 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results5) {
                            size2 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 5:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 5: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 5: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Compare " + String.valueOf(size1) + " with " + String.valueOf(size2));
                if (size1 != size2) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size1 + " != " + size2 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size1 + " != " + size2 + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 0) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5;
                List<String> VAllIds = new ArrayList<>();
                String x1 = "", x2 = "", x3 = "";
                int size1 = -1, size2 = -1;
                int variant = 4;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(r.getString());
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    for (int y = 0; y < VAllIds.size(); y++) {
                        executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + VAllIds.get(x) + "'", "'" + VAllIds.get(y) + "'");
                        try {
                            results2 = connection.getClient().submit(executeQuery).all().get();
                            if (results2.size() == 0 || results2 == null) {
                                executeQuery = "";
                                queriesList.get(i).add(executeQuery);
                                continue;
                            } else {
                                for (Result r : results2) {
                                    if (r.getInt() != 0) {
                                        index = x;
                                        x1 = VAllIds.get(x);
                                        x2 = VAllIds.get(y);
                                        size1 = r.getInt();
                                        queriesList.get(i).add(executeQuery);
                                        out.append("query 2:" + "\n");
                                        out.append(executeQuery + "\n");
                                        System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        x = VAllIds.size();
                                        y = VAllIds.size();
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();

                            System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                            result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                        }
                    }
                }

                if (x2.equals(""))
                    continue;
                // Query 3:
                queryNum = 3;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results3 = connection.getClient().submit(executeQuery).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results3) {
                            String tmp = r.getString().substring(2);
                            String res = tmp.substring(0, tmp.length() - 1);
                            x3 = res;
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 4:
                queryNum = 4;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x2 + "'", "'" + x3 + "'");
                try {
                    results4 = connection.getClient().submit(executeQuery).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        queriesList.get(i).add(executeQuery);
                        out.append("query 4:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 5:
                queryNum = 5;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "'" + x1 + "'", "'" + x3 + "'");
                try {
                    results5 = connection.getClient().submit(executeQuery).all().get();
                    if (results5.size() == 0 || results5 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results5) {
                            size2 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 5:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 5: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 5: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Compare " + String.valueOf(size1) + " with " + String.valueOf(size2));
                if (size1 != size2) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size1 + " != " + size2 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size1 + " != " + size2 + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5;
                List<String> VAllIds = new ArrayList<>();
                String x1 = "", x2 = "", x3 = "";
                int size1 = -1, size2 = -1;
                int variant = 4;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    for (int y = 0; y < VAllIds.size(); y++) {
                        executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, VAllIds.get(x), VAllIds.get(y));
                        try {
                            results2 = connection.getClient().submit(executeQuery).all().get();
                            if (results2.size() == 0 || results2 == null) {
                                executeQuery = "";
                                queriesList.get(i).add(executeQuery);
                                continue;
                            } else {
                                for (Result r : results2) {
                                    if (r.getInt() != 0) {
                                        index = x;
                                        x1 = VAllIds.get(x);
                                        x2 = VAllIds.get(y);
                                        size1 = r.getInt();
                                        queriesList.get(i).add(executeQuery);
                                        out.append("query 2:" + "\n");
                                        out.append(executeQuery + "\n");
                                        System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        x = VAllIds.size();
                                        y = VAllIds.size();
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();

                            System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                            result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                        }
                    }
                }

                if (x2.equals(""))
                    continue;
                // Query 3:
                queryNum = 3;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results3 = connection.getClient().submit(executeQuery).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results3) {
                            String res = r.getString();
                            x3 = res.replaceAll("[^0-9]", "");
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 4:
                queryNum = 4;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x2, x3);
                try {
                    results4 = connection.getClient().submit(executeQuery).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        queriesList.get(i).add(executeQuery);
                        out.append("query 4:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 5:
                queryNum = 5;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x1, x3);
                try {
                    results5 = connection.getClient().submit(executeQuery).all().get();
                    if (results5.size() == 0 || results5 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results5) {
                            size2 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 5:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 5: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 5: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Compare " + String.valueOf(size1) + " with " + String.valueOf(size2));
                if (size1 != size2) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size1 + " != " + size2 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size1 + " != " + size2 + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery6(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr6" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr6" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 3) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 1;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                StringBuilder executeQuery2 = new StringBuilder(executeQuery);
                executeQuery2.insert(6, "not(");
                executeQuery2.insert(executeQuery2.length() - 7, ")");
                try {
                    results2 = connection.getClient().submit(executeQuery2.toString()).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results2) {
                            size2 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery2.toString());
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2.toString() + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2.toString() + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results3 = connection.getClient().submit(executeQuery).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results3) {
                            size3 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Compare " + String.valueOf(size1) + " + " + String.valueOf(size2) + " with " + String.valueOf(size3));
                if (size1 + size2 != size3) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size1 + " + " + size2 + " != " + size3 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size1 + " + " + size2 + " != " + size3 + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3;
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 1;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        size1 = (int) hugeResult1.data().get(0);
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                StringBuilder executeQuery2 = new StringBuilder(executeQuery);
                executeQuery2.insert(6, "not(");
                executeQuery2.insert(executeQuery2.length() - 7, ")");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult2 = hugeGraph.gremlin(executeQuery2.toString()).execute();
                    if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                        executeQuery2 = new StringBuilder("");
                        queriesList.get(i).add(executeQuery2.toString());
                        continue;
                    } else {
                        size2 = (int) hugeResult2.data().get(0);
                        queriesList.get(i).add(executeQuery2.toString());
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2.toString() + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2.toString() + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult3 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        size3 = (int) hugeResult3.data().get(0);
                        queriesList.get(i).add(executeQuery);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Compare " + String.valueOf(size1) + " + " + String.valueOf(size2) + " with " + String.valueOf(size3));
                if (size1 + size2 != size3) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size1 + " + " + size2 + " != " + size3 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size1 + " + " + size2 + " != " + size3 + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 1;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                StringBuilder executeQuery2 = new StringBuilder(executeQuery);
                executeQuery2.insert(6, "not(");
                executeQuery2.insert(executeQuery2.length() - 7, ")");
                try {
                    results2 = connection.getClient().submit(executeQuery2.toString()).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results2) {
                            size2 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery2.toString());
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2.toString() + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2.toString() + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results3 = connection.getClient().submit(executeQuery).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results3) {
                            size3 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Compare " + String.valueOf(size1) + " + " + String.valueOf(size2) + " with " + String.valueOf(size3));
                if (size1 + size2 != size3) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size1 + " + " + size2 + " != " + size3 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size1 + " + " + size2 + " != " + size3 + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery7(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr7" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr7" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 3) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 2;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                String filter = executeQuery.replace("g.V().", "").replace(".size()", "");
                executeQuery2 = "g.V().not(" + filter + ").and(" + filter + ").size()";
                try {
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        for (Result r : results2) {
                            size2 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Check " + String.valueOf(size1) + " with " + String.valueOf(size2));
                if (size2 != 0) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size2 + " != 0" + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size2 + " != 0" + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3;
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 2;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        size1 = (int) hugeResult1.data().get(0);
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                String filter = executeQuery.replace("g.V().", "").replace(".size()", "");
                executeQuery2 = "g.V().not(" + filter + ").and(" + filter + ").size()";
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult2 = hugeGraph.gremlin(executeQuery2).execute();
                    if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        size2 = (int) hugeResult2.data().get(0);
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Check " + String.valueOf(size1) + " with " + String.valueOf(size2));
                if (size2 != 0) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size2 + " != 0" + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size2 + " != 0" + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 2;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                String filter = executeQuery.replace("g.V().", "").replace(".size()", "");
                executeQuery2 = "g.V().not(" + filter + ").and(" + filter + ").size()";
                try {
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        for (Result r : results2) {
                            size2 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Check " + String.valueOf(size1) + " with " + String.valueOf(size2));
                if (size2 != 0) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size2 + " != 0" + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size2 + " != 0" + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery8(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr8" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr8" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 3) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 3;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                String filter = executeQuery.replace("g.V().", "").replace(".size()", "");
                executeQuery2 = "g.V().not(" + filter + ").or()." + filter + ".size()";
                try {
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        for (Result r : results2) {
                            size2 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Check " + String.valueOf(size1) + " with " + String.valueOf(size2));
                if (size2 != 100) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size2 + " != 100" + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size2 + " != 100" + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3;
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 3;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        size1 = (int) hugeResult1.data().get(0);
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                String filter = executeQuery.replace("g.V().", "").replace(".size()", "");
                executeQuery2 = "g.V().not(" + filter + ").or()." + filter + ".size()";
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult2 = hugeGraph.gremlin(executeQuery2).execute();
                    if (hugeResult2.data().size() == 0 || hugeResult2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        size2 = (int) hugeResult2.data().get(0);
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Check " + String.valueOf(size1) + " with " + String.valueOf(size2));
                if (size2 != 100) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size2 + " != 100" + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size2 + " != 100" + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 3;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                String filter = executeQuery.replace("g.V().", "").replace(".size()", "");
                executeQuery2 = "g.V().not(" + filter + ").or()." + filter + ".size()";
                try {
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        for (Result r : results2) {
                            size2 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Check " + String.valueOf(size1) + " with " + String.valueOf(size2));
                if (size2 != 100) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size2 + " != 100" + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size2 + " != 100" + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery9(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr9" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr9" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3;
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 1;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        size1 = (int) hugeResult1.data().get(0);
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                StringBuilder executeQuery2 = new StringBuilder(executeQuery);
                executeQuery2.insert(6, "not(");
                executeQuery2.insert(executeQuery2.length() - 7, ")");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult2 = hugeGraph.gremlin(executeQuery2.toString()).execute();
                    if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                        executeQuery2 = new StringBuilder("");
                        queriesList.get(i).add(executeQuery2.toString());
                        continue;
                    } else {
                        size2 = (int) hugeResult2.data().get(0);
                        queriesList.get(i).add(executeQuery2.toString());
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2.toString() + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2.toString() + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult3 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        size3 = (int) hugeResult3.data().get(0);
                        queriesList.get(i).add(executeQuery);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }

                    // Check result:
                    System.out.println("Compare " + String.valueOf(size1) + " + " + String.valueOf(size2) + " with " + String.valueOf(size3));
                    if (size1 + size2 != size3) {
                        result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                        result.append("Results are: " + size1 + " + " + size2 + " != " + size3 + "\n");
                        System.out.println("Query sequence " + i + ": false\n");
                        System.out.println("Results are: " + size1 + " + " + size2 + " != " + size3 + "\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 1;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                StringBuilder executeQuery2 = new StringBuilder(executeQuery);
                executeQuery2.insert(6, "not(");
                executeQuery2.insert(executeQuery2.length() - 7, ")");
                try {
                    results2 = connection.getClient().submit(executeQuery2.toString()).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = new StringBuilder("");
                        queriesList.get(i).add(executeQuery2.toString());
                        continue;
                    } else {
                        for (Result r : results2) {
                            size2 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery2.toString());
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2.toString() + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2.toString() + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results3 = connection.getClient().submit(executeQuery).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results3) {
                            size3 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Compare " + String.valueOf(size1) + " + " + String.valueOf(size2) + " with " + String.valueOf(size3));
                if (size1 + size2 != size3) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size1 + " + " + size2 + " != " + size3 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size1 + " + " + size2 + " != " + size3 + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery10(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr10" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr10" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3;
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 2;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        size1 = (int) hugeResult1.data().get(0);
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                String filter = executeQuery.replace("g.E().", "").replace(".size()", "");
                executeQuery2 = "g.E().not(" + filter + ").and(" + filter + ").size()";
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult2 = hugeGraph.gremlin(executeQuery2).execute();
                    if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        size2 = (int) hugeResult2.data().get(0);
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Check " + String.valueOf(size1) + " with " + String.valueOf(size2));
                if (size2 != 0) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size2 + " != 0" + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size2 + " != 0" + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 2;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                String filter = executeQuery.replace("g.E().", "").replace(".size()", "");
                executeQuery2 = "g.E().not(" + filter + ").and(" + filter + ").size()";
                try {
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        for (Result r : results2) {
                            size2 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Check " + String.valueOf(size1) + " with " + String.valueOf(size2));
                if (size2 != 0) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size2 + " != 0" + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size2 + " != 0" + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery11(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr11" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr11" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3;
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 3;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        size1 = (int) hugeResult1.data().get(0);
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                String filter = executeQuery.replace("g.E().", "").replace(".size()", "");
                executeQuery2 = "g.E().not(" + filter + ").or()." + filter + ".size()";
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult2 = hugeGraph.gremlin(executeQuery2).execute();
                    if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        size2 = (int) hugeResult2.data().get(0);
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Check " + String.valueOf(size1) + " with " + String.valueOf(size2));
                if (size2 != 200) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size2 + " != 200" + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size2 + " != 200" + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 3;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                String filter = executeQuery.replace("g.E().", "").replace(".size()", "");
                executeQuery2 = "g.E().not(" + filter + ").or()." + filter + ".size()";
                try {
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        for (Result r : results2) {
                            size2 = r.getInt();
                        }
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                System.out.println("Check " + String.valueOf(size1) + " with " + String.valueOf(size2));
                if (size2 != 200) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + size2 + " != 200" + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + size2 + " != 200" + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery12(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr12" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr12" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            int index = -1;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3, hugeResult4;
                List<String> VAllIds = new ArrayList<>();
                String x1 = "", x2 = "";
                int size1 = -1, size2 = -1;
                int variant = 5;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        hugeResult1.data().forEach(r -> {
                            VAllIds.add(String.valueOf(r));
                        });
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    for (int y = 0; y < VAllIds.size(); y++) {
                        executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, VAllIds.get(x), VAllIds.get(y));
                        try {
                            GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                            hugeResult2 = hugeGraph.gremlin(executeQuery).execute();
                            if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                                executeQuery = "";
                                queriesList.get(i).add(executeQuery);
                                continue;
                            } else {
                                if ((int) hugeResult2.data().get(0) == 1) {
                                    index = x;
                                    x1 = VAllIds.get(x);
                                    x2 = VAllIds.get(y);
                                    size1 = (int) hugeResult2.data().get(0);
                                    System.out.println("Check initial path: " + size1);
                                    queriesList.get(i).add(executeQuery);
                                    out.append("query 2:" + "\n");
                                    out.append(executeQuery + "\n");
                                    System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                    x = VAllIds.size();
                                    y = VAllIds.size();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

                if (x2.equals(""))
                    continue;
                // Query 3:
                queryNum = 3;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x1, x2);
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult3 = hugeGraph.gremlin(executeQuery).execute();
                    queriesList.get(i).add(executeQuery);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery + "\n");
                    System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Query 4:
                queryNum = 4;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x1, x2);
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult4 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult4.data().size() == 0 || hugeResult4.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        size2 = (int) hugeResult4.data().get(0);
                        queriesList.get(i).add(executeQuery);
                        System.out.println("Check final path: " + size2);
                        out.append("query 4:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        if (size2 != 0) {
                            result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                            result.append("Results are: " + size2 + " != 0\n");
                            System.out.println("Query sequence " + i + ": false\n");
                            System.out.println("Results are: " + size2 + " != 0\n");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            int index = -1;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VAllIds = new ArrayList<>();
                String x1 = "", x2 = "";
                int size1 = -1, size2 = -1;
                int variant = 5;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    for (int y = 0; y < VAllIds.size(); y++) {
                        executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, VAllIds.get(x), VAllIds.get(y));
                        try {
                            results2 = connection.getClient().submit(executeQuery).all().get();
                            if (results2.size() == 0 || results2 == null) {
                                executeQuery = "";
                                queriesList.get(i).add(executeQuery);
                                continue;
                            } else {
                                for (Result r : results2) {
                                    if (r.getInt() == 1) {
                                        index = x;
                                        x1 = VAllIds.get(x);
                                        x2 = VAllIds.get(y);
                                        size1 = r.getInt();
                                        System.out.println("Check initial path: " + size1);
                                        queriesList.get(i).add(executeQuery);
                                        out.append("query 2:" + "\n");
                                        out.append(executeQuery + "\n");
                                        System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                                        x = VAllIds.size();
                                        y = VAllIds.size();
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

                if (x2.equals(""))
                    continue;
                // Query 3:
                queryNum = 3;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x1, x2);
                try {
                    results3 = connection.getClient().submit(executeQuery).all().get();
                    queriesList.get(i).add(executeQuery);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery + "\n");
                    System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Query 4:
                queryNum = 4;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, x1, x2);
                try {
                    results4 = connection.getClient().submit(executeQuery).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results4) {
                            size2 = r.getInt();
                            queriesList.get(i).add(executeQuery);
                            System.out.println("Check final path: " + size2);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            if (r.getInt() != 0) {
                                result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                                result.append("Results are: " + size2 + " != 0\n");
                                System.out.println("Query sequence " + i + ": false\n");
                                System.out.println("Results are: " + size2 + " != 0\n");
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery13(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr13" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr13" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            return;
        } else if (database == 1 || database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String repeatQuery = "";
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                int size1 = -1, size2 = -1;
                String id1 = "", id2 = "", id3 = "", id4 = "";
                int variant = 6;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "path", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = r.getInt();
                            queriesList.get(i).add(executeQuery);
                            repeatQuery = executeQuery;
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void executeMRQuery14(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr14" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr14" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 5) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 4;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery + ".dedup()").all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results1) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsOne.add(res);
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery2 + ".dedup()").all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results2) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsTwo.add(res);
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.V()." + executeQuery.substring(6) + ".fold().as('x')" + ".V()." + executeQuery2.substring(6) + ".where(within('x'))";
//                executeQuery3 = "g.V()." + executeQuery.substring(6) + ".fold().as('a')" + ".V()." + executeQuery2.substring(6) + ".fold().as('b')" + ".select('a').where('a',eq('b'))";
//                executeQuery3 = "g.V().match(__.as('a')." + executeQuery.substring(6) + ",__.as('a').filter(" + executeQuery2.substring(6) + ")).select('a')";
//                executeQuery3 = "g.V().and(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    results3 = connection.getClient().submit(executeQuery3 + ".dedup()").all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results3) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsThree.add(res);
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                Collections.sort(VIdsOne);
                Collections.sort(VIdsTwo);
                Collections.sort(VIdsThree);
                List<String> intersection = new ArrayList<>(VIdsOne);
                intersection.retainAll(VIdsTwo);
                Collections.sort(intersection);
                if (!intersection.equals(VIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("Intersection results: " + intersection + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 4) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 4;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery + ".dedup()").all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results1) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsOne.add(res);
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery2 + ".dedup()").all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results2) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsTwo.add(res);
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.V()." + executeQuery.substring(6) + ".fold().as('x')" + ".V()." + executeQuery2.substring(6) + ".where(within('x'))";
//                executeQuery3 = "g.V()." + executeQuery.substring(6) + ".fold().as('a')" + ".V()." + executeQuery2.substring(6) + ".fold().as('b')" + ".select('a').where('a',eq('b'))";
//                executeQuery3 = "g.V().match(__.as('a')." + executeQuery.substring(6) + ",__.as('a').filter(" + executeQuery2.substring(6) + ")).select('a')";
//                executeQuery3 = "g.V().and(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    results3 = connection.getClient().submit(executeQuery3 + ".dedup()").all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results3) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsThree.add(res);
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                Collections.sort(VIdsOne);
                Collections.sort(VIdsTwo);
                Collections.sort(VIdsThree);
                List<String> intersection = new ArrayList<>(VIdsOne);
                intersection.retainAll(VIdsTwo);
                Collections.sort(intersection);
                if (!intersection.equals(VIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("Intersection results: " + intersection + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                }
            }
            out.close();
            result.close();
        }  else if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3, hugeResult4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 4;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery + ".dedup()").execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult1.data().get(0);
                        String rclass = (String) tempResult.get("type");
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("vertex")) {
                            hugeResult1.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsOne.add(String.valueOf(t.get("id")));
                            });
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult2 = hugeGraph.gremlin(executeQuery2 + ".dedup()").execute();
                    if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult2.data().get(0);
                        String rclass = (String) tempResult.get("type");
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("vertex")) {
                            hugeResult2.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsTwo.add(String.valueOf(t.get("id")));
                            });
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.V()." + executeQuery.substring(6) + ".fold().as('x')" + ".V()." + executeQuery2.substring(6) + ".where(within('x'))";
//                executeQuery3 = "g.V()." + executeQuery.substring(6) + ".fold().as('a')" + ".V()." + executeQuery2.substring(6) + ".fold().as('b')" + ".select('a').where('a',eq('b'))";
//                executeQuery3 = "g.V().match(__.as('a')." + executeQuery.substring(6) + ",__.as('a').filter(" + executeQuery2.substring(6) + ")).select('a')";
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult3 = hugeGraph.gremlin(executeQuery3 + ".dedup()").execute();
                    if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult3.data().get(0);
                        String rclass = (String) tempResult.get("type");
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("vertex")) {
                            hugeResult3.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsThree.add(String.valueOf(t.get("id")));
                            });
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                Collections.sort(VIdsOne);
                Collections.sort(VIdsTwo);
                Collections.sort(VIdsThree);
                List<String> intersection = new ArrayList<>(VIdsOne);
                intersection.retainAll(VIdsTwo);
                Collections.sort(intersection);
                if (!intersection.equals(VIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("Intersection results: " + intersection + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 4;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery + ".dedup()").all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results1) {
                                String res = r.getObject().toString();
                                VIdsOne.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery2 + ".dedup()").all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results2) {
                                String res = r.getObject().toString();
                                VIdsTwo.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.V()." + executeQuery.substring(6) + ".fold().as('x')" + ".V()." + executeQuery2.substring(6) + ".where(within('x'))";
//                executeQuery3 = "g.V()." + executeQuery.substring(6) + ".fold().as('a')" + ".V()." + executeQuery2.substring(6) + ".fold().as('b')" + ".select('a').where('a',eq('b'))";
//                executeQuery3 = "g.V().match(__.as('a')." + executeQuery.substring(6) + ",__.as('a').filter(" + executeQuery2.substring(6) + ")).select('a')";
//                executeQuery3 = "g.V().and(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    results3 = connection.getClient().submit(executeQuery3 + ".dedup()").all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results3) {
                                String res = r.getObject().toString();
                                VIdsThree.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                Collections.sort(VIdsOne);
                Collections.sort(VIdsTwo);
                Collections.sort(VIdsThree);
                List<String> intersection = new ArrayList<>(VIdsOne);
                intersection.retainAll(VIdsTwo);
                Collections.sort(intersection);
                if (!intersection.equals(VIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("Intersection results: " + intersection + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsThree results:u " + VIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 4;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery + ".dedup()").all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results1) {
                                String res = r.getObject().toString();
                                VIdsOne.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery2 + ".dedup()").all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results2) {
                                String res = r.getObject().toString();
                                VIdsTwo.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.V()." + executeQuery.substring(6) + ".fold().as('x')" + ".V()." + executeQuery2.substring(6) + ".where(within('x'))";
//                executeQuery3 = "g.V()." + executeQuery.substring(6) + ".fold().as('a')" + ".V()." + executeQuery2.substring(6) + ".fold().as('b')" + ".select('a').where('a',eq('b'))";
//                executeQuery3 = "g.V().match(__.as('a')." + executeQuery.substring(6) + ",__.as('a').filter(" + executeQuery2.substring(6) + ")).select('a')";
//                executeQuery3 = "g.V().and(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    results3 = connection.getClient().submit(executeQuery3 + ".dedup()").all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results3) {
                                String res = r.getObject().toString();
                                VIdsThree.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                Collections.sort(VIdsOne);
                Collections.sort(VIdsTwo);
                Collections.sort(VIdsThree);
                List<String> intersection = new ArrayList<>(VIdsOne);
                intersection.retainAll(VIdsTwo);
                Collections.sort(intersection);
                if (!intersection.equals(VIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("Intersection results: " + intersection + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery15(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr15" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr15" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 5) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 5;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery + ".dedup()").all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results1) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsOne.add(res);
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery2 + ".dedup()").all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results2) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsTwo.add(res);
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.V().union(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    results3 = connection.getClient().submit(executeQuery3 + ".dedup()").all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results3) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsThree.add(res);
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                Collections.sort(VIdsOne);
                Collections.sort(VIdsTwo);
                Collections.sort(VIdsThree);
                List<String> union = new ArrayList<>(VIdsOne);
                union.removeAll(VIdsTwo);
                union.addAll(VIdsTwo);
                Collections.sort(union);
                if (!union.equals(VIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 4) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 5;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery + ".dedup()").all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results1) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsOne.add(res);
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery2 + ".dedup()").all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results2) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsTwo.add(res);
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.V().union(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    results3 = connection.getClient().submit(executeQuery3 + ".dedup()").all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results3) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsThree.add(res);
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                Collections.sort(VIdsOne);
                Collections.sort(VIdsTwo);
                Collections.sort(VIdsThree);
                List<String> union = new ArrayList<>(VIdsOne);
                union.removeAll(VIdsTwo);
                union.addAll(VIdsTwo);
                Collections.sort(union);
                if (!union.equals(VIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3, hugeResult4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 4;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery + ".dedup()").execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult1.data().get(0);
                        String rclass = (String) tempResult.get("type");
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("vertex")) {
                            hugeResult1.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsOne.add(String.valueOf(t.get("id")));
                            });
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult2 = hugeGraph.gremlin(executeQuery2 + ".dedup()").execute();
                    if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult2.data().get(0);
                        String rclass = (String) tempResult.get("type");
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("vertex")) {
                            hugeResult2.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsTwo.add(String.valueOf(t.get("id")));
                            });
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.V().union(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult3 = hugeGraph.gremlin(executeQuery3 + ".dedup()").execute();
                    if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult3.data().get(0);
                        String rclass = (String) tempResult.get("type");
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("vertex")) {
                            hugeResult3.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsThree.add(String.valueOf(t.get("id")));
                            });
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                Collections.sort(VIdsOne);
                Collections.sort(VIdsTwo);
                Collections.sort(VIdsThree);
                List<String> union = new ArrayList<>(VIdsOne);
                union.removeAll(VIdsTwo);
                union.addAll(VIdsTwo);
                Collections.sort(union);
                if (!union.equals(VIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 5;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery + ".dedup()").all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results1) {
                                String res = r.getObject().toString();
                                VIdsOne.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery2 + ".dedup()").all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results2) {
                                String res = r.getObject().toString();
                                VIdsTwo.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.V().union(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    results3 = connection.getClient().submit(executeQuery3 + ".dedup()").all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results3) {
                                String res = r.getObject().toString();
                                VIdsThree.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                Collections.sort(VIdsOne);
                Collections.sort(VIdsTwo);
                Collections.sort(VIdsThree);
                List<String> union = new ArrayList<>(VIdsOne);
                union.removeAll(VIdsTwo);
                union.addAll(VIdsTwo);
                Collections.sort(union);
                if (!union.equals(VIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 5;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery + ".dedup()").all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results1) {
                                String res = r.getObject().toString();
                                VIdsOne.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery2 + ".dedup()").all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results2) {
                                String res = r.getObject().toString();
                                VIdsTwo.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.V().union(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    results3 = connection.getClient().submit(executeQuery3 + ".dedup()").all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results3) {
                                String res = r.getObject().toString();
                                VIdsThree.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                Collections.sort(VIdsOne);
                Collections.sort(VIdsTwo);
                Collections.sort(VIdsThree);
                List<String> union = new ArrayList<>(VIdsOne);
                union.removeAll(VIdsTwo);
                union.addAll(VIdsTwo);
                Collections.sort(union);
                if (!union.equals(VIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery16(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr16" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr16" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> EIdsOne = new ArrayList<>();
                List<String> EIdsTwo = new ArrayList<>();
                List<String> EIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 4;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery + ".dedup()").all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge}")) {
                            for (Result r : results1) {
                                DetachedEdge t = (DetachedEdge) r.getObject();
                                EIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery2 + ".dedup()").all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge}")) {
                            for (Result r : results2) {
                                DetachedEdge t = (DetachedEdge) r.getObject();
                                EIdsTwo.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.E().match(__.as('a')." + executeQuery.substring(6) + ",__.as('a').filter(" + executeQuery2.substring(6) + ")).select('a')";
                executeQuery3 = "g.E()." + executeQuery.substring(6) + ".fold().as('x')" + ".E()." + executeQuery2.substring(6) + ".where(within('x'))";
//                executeQuery3 = "g.E()." + executeQuery.substring(6) + ".fold().as('a')" + ".E()." + executeQuery2.substring(6) + ".fold().as('b')" + ".select('a').where('a',eq('b'))";
//                executeQuery3 = "g.E().and(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    results3 = connection.getClient().submit(executeQuery3 + ".dedup()").all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge}")) {
                            for (Result r : results3) {
                                DetachedEdge t = (DetachedEdge) r.getObject();
                                EIdsThree.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                EIdsOne.sort(Comparator.naturalOrder());
                EIdsTwo.sort(Comparator.naturalOrder());
                EIdsThree.sort(Comparator.naturalOrder());
                List<String> intersection = new ArrayList<>(EIdsOne);
                intersection.retainAll(EIdsTwo);
                Collections.sort(intersection);
                if (!intersection.equals(EIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("EIdsOne results: " + EIdsOne + "\n");
                    result.append("EIdsTwo results: " + EIdsTwo + "\n");
                    result.append("EIdsThree results: " + EIdsThree + "\n");
                    result.append("Intersection results: " + intersection + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 3) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3, hugeResult4;
                List<String> EIdsOne = new ArrayList<>();
                List<String> EIdsTwo = new ArrayList<>();
                List<String> EIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 4;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery + ".dedup()").execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        if (!hugeResult1.data().get(0).getClass().toString().equals("class java.util.LinkedHashMap")) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult1.data().get(0);
                            String rclass = (String) tempResult.get("type");
                            System.out.println("rclass 1: " + rclass);
                            if (rclass.equals("edge")) {
                                hugeResult1.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                    EIdsOne.add(String.valueOf(t.get("id")));
                                });
                                queriesList.get(i).add(executeQuery);
                                out.append("query 1:" + "\n");
                                out.append(executeQuery + "\n");
                                System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                executeQuery = "";
                                queriesList.get(i).add(executeQuery);
                                continue;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult2 = hugeGraph.gremlin(executeQuery2 + ".dedup()").execute();
                    if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        if (!hugeResult2.data().get(0).getClass().toString().equals("class java.util.LinkedHashMap")) {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        } else {
                            HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult2.data().get(0);
                            String rclass = (String) tempResult.get("type");
                            System.out.println("rclass 2: " + rclass);
                            if (rclass.equals("edge")) {
                                hugeResult2.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                    EIdsTwo.add(String.valueOf(t.get("id")));
                                });
                                queriesList.get(i).add(executeQuery2);
                                out.append("query 2:" + "\n");
                                out.append(executeQuery2 + "\n");
                                System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                executeQuery2 = "";
                                queriesList.get(i).add(executeQuery2);
                                continue;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
//                executeQuery3 = "g.E().match(__.as('a')." + executeQuery.substring(6) + ",__.as('a').filter(" + executeQuery2.substring(6) + ")).select('a')";
                executeQuery3 = "g.E()." + executeQuery.substring(6) + ".fold().as('x')" + ".E()." + executeQuery2.substring(6) + ".where(within('x'))";
//                executeQuery3 = "g.E()." + executeQuery.substring(6) + ".fold().as('a')" + ".E()." + executeQuery2.substring(6) + ".fold().as('b')" + ".select('a').where('a',eq('b'))";
//                executeQuery3 = "g.E().and(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult3 = hugeGraph.gremlin(executeQuery3 + ".dedup()").execute();
                    if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        if (!hugeResult3.data().get(0).getClass().toString().equals("class java.util.LinkedHashMap")) {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        } else {
                            HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult3.data().get(0);
                            String rclass = (String) tempResult.get("type");
                            System.out.println("rclass 3: " + rclass);
                            if (rclass.equals("edge")) {
                                hugeResult3.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                    EIdsThree.add(String.valueOf(t.get("id")));
                                });
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                executeQuery3 = "";
                                queriesList.get(i).add(executeQuery3);
                                continue;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                EIdsOne.sort(Comparator.naturalOrder());
                EIdsTwo.sort(Comparator.naturalOrder());
                EIdsThree.sort(Comparator.naturalOrder());
                List<String> intersection = new ArrayList<>(EIdsOne);
                intersection.retainAll(EIdsTwo);
                Collections.sort(intersection);
                if (!intersection.equals(EIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("EIdsOne results: " + EIdsOne + "\n");
                    result.append("EIdsTwo results: " + EIdsTwo + "\n");
                    result.append("EIdsThree results: " + EIdsThree + "\n");
                    result.append("Intersection results: " + intersection + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> EIdsOne = new ArrayList<>();
                List<String> EIdsTwo = new ArrayList<>();
                List<String> EIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 4;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery + ".dedup()").all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge}")) {
                            for (Result r : results1) {
                                DetachedEdge t = (DetachedEdge) r.getObject();
                                EIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery2 + ".dedup()").all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge}")) {
                            for (Result r : results2) {
                                DetachedEdge t = (DetachedEdge) r.getObject();
                                EIdsTwo.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.E().match(__.as('a')." + executeQuery.substring(6) + ",__.as('a').filter(" + executeQuery2.substring(6) + ")).select('a')";
                executeQuery3 = "g.E()." + executeQuery.substring(6) + ".fold().as('x')" + ".E()." + executeQuery2.substring(6) + ".where(within('x'))";
//                executeQuery3 = "g.E()." + executeQuery.substring(6) + ".fold().as('a')" + ".E()." + executeQuery2.substring(6) + ".fold().as('b')" + ".select('a').where('a',eq('b'))";
//                executeQuery3 = "g.E().and(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    results3 = connection.getClient().submit(executeQuery3 + ".dedup()").all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge}")) {
                            for (Result r : results3) {
                                DetachedEdge t = (DetachedEdge) r.getObject();
                                EIdsThree.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                EIdsOne.sort(Comparator.naturalOrder());
                EIdsTwo.sort(Comparator.naturalOrder());
                EIdsThree.sort(Comparator.naturalOrder());
                List<String> intersection = new ArrayList<>(EIdsOne);
                intersection.retainAll(EIdsTwo);
                Collections.sort(intersection);
                if (!intersection.equals(EIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("EIdsOne results: " + EIdsOne + "\n");
                    result.append("EIdsTwo results: " + EIdsTwo + "\n");
                    result.append("EIdsThree results: " + EIdsThree + "\n");
                    result.append("Intersection results: " + intersection + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> EIdsOne = new ArrayList<>();
                List<String> EIdsTwo = new ArrayList<>();
                List<String> EIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 4;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery + ".dedup()").all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge}")) {
                            for (Result r : results1) {
                                ReferenceEdge t = (ReferenceEdge) r.getObject();
                                EIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery2 + ".dedup()").all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge}")) {
                            for (Result r : results2) {
                                ReferenceEdge t = (ReferenceEdge) r.getObject();
                                EIdsTwo.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.E().match(__.as('a')." + executeQuery.substring(6) + ",__.as('a').filter(" + executeQuery2.substring(6) + ")).select('a')";
                executeQuery3 = "g.E()." + executeQuery.substring(6) + ".fold().as('x')" + ".E()." + executeQuery2.substring(6) + ".where(within('x'))";
//                executeQuery3 = "g.E()." + executeQuery.substring(6) + ".fold().as('a')" + ".E()." + executeQuery2.substring(6) + ".fold().as('b')" + ".select('a').where('a',eq('b'))";
//                executeQuery3 = "g.E().and(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    results3 = connection.getClient().submit(executeQuery3 + ".dedup()").all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge}")) {
                            for (Result r : results3) {
                                ReferenceEdge t = (ReferenceEdge) r.getObject();
                                EIdsThree.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                EIdsOne.sort(Comparator.naturalOrder());
                EIdsTwo.sort(Comparator.naturalOrder());
                EIdsThree.sort(Comparator.naturalOrder());
                List<String> intersection = new ArrayList<>(EIdsOne);
                intersection.retainAll(EIdsTwo);
                Collections.sort(intersection);
                if (!intersection.equals(EIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("EIdsOne results: " + EIdsOne + "\n");
                    result.append("EIdsTwo results: " + EIdsTwo + "\n");
                    result.append("EIdsThree results: " + EIdsThree + "\n");
                    result.append("Intersection results: " + intersection + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery17(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr17" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr17" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 3) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> EIdsOne = new ArrayList<>();
                List<String> EIdsTwo = new ArrayList<>();
                List<String> EIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 5;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery + ".dedup()").all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge}")) {
                            for (Result r : results1) {
                                DetachedEdge t = (DetachedEdge) r.getObject();
                                EIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery2 + ".dedup()").all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge}")) {
                            for (Result r : results2) {
                                DetachedEdge t = (DetachedEdge) r.getObject();
                                EIdsTwo.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.E().union(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    results3 = connection.getClient().submit(executeQuery3 + ".dedup()").all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge}")) {
                            for (Result r : results3) {
                                DetachedEdge t = (DetachedEdge) r.getObject();
                                EIdsThree.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                EIdsOne.sort(Comparator.naturalOrder());
                EIdsTwo.sort(Comparator.naturalOrder());
                EIdsThree.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(EIdsOne);
                union.removeAll(EIdsTwo);
                union.addAll(EIdsTwo);
                Collections.sort(union);
                if (!union.equals(EIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("EIdsOne results: " + EIdsOne + "\n");
                    result.append("EIdsTwo results: " + EIdsTwo + "\n");
                    result.append("EIdsThree results: " + EIdsThree + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 4) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3, hugeResult4;
                List<String> EIdsOne = new ArrayList<>();
                List<String> EIdsTwo = new ArrayList<>();
                List<String> EIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 5;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery + ".dedup()").execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        if (!hugeResult1.data().get(0).getClass().toString().equals("class java.util.LinkedHashMap")) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult1.data().get(0);
                            String rclass = (String) tempResult.get("type");
                            System.out.println("rclass 1: " + rclass);
                            if (rclass.equals("edge")) {
                                hugeResult1.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                    EIdsOne.add(String.valueOf(t.get("id")));
                                });
                                queriesList.get(i).add(executeQuery);
                                out.append("query 1:" + "\n");
                                out.append(executeQuery + "\n");
                                System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                executeQuery = "";
                                queriesList.get(i).add(executeQuery);
                                continue;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult2 = hugeGraph.gremlin(executeQuery2 + ".dedup()").execute();
                    if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        if (!hugeResult2.data().get(0).getClass().toString().equals("class java.util.LinkedHashMap")) {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        } else {
                            HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult2.data().get(0);
                            String rclass = (String) tempResult.get("type");
                            System.out.println("rclass 2: " + rclass);
                            if (rclass.equals("edge")) {
                                hugeResult2.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                    EIdsTwo.add(String.valueOf(t.get("id")));
                                });
                                queriesList.get(i).add(executeQuery2);
                                out.append("query 2:" + "\n");
                                out.append(executeQuery2 + "\n");
                                System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                executeQuery2 = "";
                                queriesList.get(i).add(executeQuery2);
                                continue;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.E().union(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult3 = hugeGraph.gremlin(executeQuery3 + ".dedup()").execute();
                    if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        if (!hugeResult3.data().get(0).getClass().toString().equals("class java.util.LinkedHashMap")) {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        } else {
                            HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult3.data().get(0);
                            String rclass = (String) tempResult.get("type");
                            System.out.println("rclass 3: " + rclass);
                            if (rclass.equals("edge")) {
                                hugeResult3.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                    EIdsThree.add(String.valueOf(t.get("id")));
                                });
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                executeQuery3 = "";
                                queriesList.get(i).add(executeQuery3);
                                continue;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                EIdsOne.sort(Comparator.naturalOrder());
                EIdsTwo.sort(Comparator.naturalOrder());
                EIdsThree.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(EIdsOne);
                union.removeAll(EIdsTwo);
                union.addAll(EIdsTwo);
                Collections.sort(union);
                if (!union.equals(EIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("EIdsOne results: " + EIdsOne + "\n");
                    result.append("EIdsTwo results: " + EIdsTwo + "\n");
                    result.append("EIdsThree results: " + EIdsThree + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> EIdsOne = new ArrayList<>();
                List<String> EIdsTwo = new ArrayList<>();
                List<String> EIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 5;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery + ".dedup()").all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge}")) {
                            for (Result r : results1) {
                                DetachedEdge t = (DetachedEdge) r.getObject();
                                EIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery2 + ".dedup()").all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge}")) {
                            for (Result r : results2) {
                                DetachedEdge t = (DetachedEdge) r.getObject();
                                EIdsTwo.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.E().union(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    results3 = connection.getClient().submit(executeQuery3 + ".dedup()").all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge}")) {
                            for (Result r : results3) {
                                DetachedEdge t = (DetachedEdge) r.getObject();
                                EIdsThree.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                EIdsOne.sort(Comparator.naturalOrder());
                EIdsTwo.sort(Comparator.naturalOrder());
                EIdsThree.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(EIdsOne);
                union.removeAll(EIdsTwo);
                union.addAll(EIdsTwo);
                Collections.sort(union);
                if (!union.equals(EIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("EIdsOne results: " + EIdsOne + "\n");
                    result.append("EIdsTwo results: " + EIdsTwo + "\n");
                    result.append("EIdsThree results: " + EIdsThree + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> EIdsOne = new ArrayList<>();
                List<String> EIdsTwo = new ArrayList<>();
                List<String> EIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 5;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery + ".dedup()").all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge}")) {
                            for (Result r : results1) {
                                ReferenceEdge t = (ReferenceEdge) r.getObject();
                                EIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "edge", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery2 + ".dedup()").all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge}")) {
                            for (Result r : results2) {
                                ReferenceEdge t = (ReferenceEdge) r.getObject();
                                EIdsTwo.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.E().union(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    results3 = connection.getClient().submit(executeQuery3 + ".dedup()").all().get();
                    results3 = connection.getClient().submit(executeQuery3 + ".dedup()").all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge}")) {
                            for (Result r : results3) {
                                ReferenceEdge t = (ReferenceEdge) r.getObject();
                                EIdsThree.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                EIdsOne.sort(Comparator.naturalOrder());
                EIdsTwo.sort(Comparator.naturalOrder());
                EIdsThree.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(EIdsOne);
                union.removeAll(EIdsTwo);
                union.addAll(EIdsTwo);
                Collections.sort(union);
                if (!union.equals(EIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("EIdsOne results: " + EIdsOne + "\n");
                    result.append("EIdsTwo results: " + EIdsTwo + "\n");
                    result.append("EIdsThree results: " + EIdsThree + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("EIdsThree results: " + EIdsThree + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery18(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr18" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr18" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            return;
        } else if (database == 1) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                int size1 = -1, size2 = -1, size3 = -1;
                int variant = 6;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        for (Result r : results1) {
                            String res = r.getObject().toString();
                            VIdsOne.add(res);
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        for (Result r : results2) {
                            String res = r.getObject().toString();
                            VIdsTwo.add(res);
                        }
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 3:
                queryNum = 3;
                executeQuery3 = "g.V().and(__." + executeQuery.substring(6) + ",__." + executeQuery2.substring(6) + ")";
                try {
                    results3 = connection.getClient().submit(executeQuery3).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        for (Result r : results3) {
                            String res = r.getObject().toString();
                            VIdsThree.add(res);
                        }
                        queriesList.get(i).add(executeQuery3);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery3 + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                Collections.sort(VIdsOne);
                Collections.sort(VIdsTwo);
                Collections.sort(VIdsThree);
                List<String> intersection = new ArrayList<>(VIdsOne);
                intersection.retainAll(VIdsTwo);
                Collections.sort(intersection);
                if (!intersection.equals(VIdsThree)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("Intersection results: " + intersection + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsThree results: " + VIdsThree + "\n");
                    System.out.println("Intersection results: " + intersection + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 2) {
            return;
        }
    }

    public void executeMRQuery20(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr20" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr20" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 3) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                String x1 = "", x2 = "";
                int variant = 8;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(r.getString());
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "'" + VAllIds.get(x) + "'", "");
                    try {
                        results2 = connection.getClient().submit(executeQuery).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            for (Result r : results2) {
                                DetachedVertex t = (DetachedVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            index = x;
                            x1 = VAllIds.get(x);
                            VIdsOne.remove(x1);
                            queriesList.get(i).add(executeQuery);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (VIdsOne.size() == 0)
                    continue;
                // Query 3:
                queryNum = 3;
                for (String id : VIdsOne) {
                    executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, "'" + id + "'", "");
                    try {
                        results3 = connection.getClient().submit(executeQuery2).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            executeQuery2 = "";
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                            result.append("Results are: " + x1 + ", " + x2 + "\n");
                            System.out.println("Query sequence " + i + ": false\n");
                            System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            continue;
                        } else {
                            for (Result r : results3) {
                                DetachedVertex t = (DetachedVertex) r.getObject();
                                VIdsTwo.add(t.id().toString());
                            }
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");

                            // Check result:
                            if (!VIdsTwo.contains(x1)) {
                                result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                                result.append("Results are: " + x1 + ", " + x2 + "\n");
                                System.out.println("Query sequence " + i + ": false\n");
                                System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }
            }
            out.close();
            result.close();
        } else if (database == 4) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                String x1 = "", x2 = "";
                int variant = 8;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(r.getString());
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "'" + VAllIds.get(x) + "'", "");
                    try {
                        results2 = connection.getClient().submit(executeQuery).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            for (Result r : results2) {
                                ReferenceVertex t = (ReferenceVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            index = x;
                            x1 = VAllIds.get(x);
                            VIdsOne.remove(x1);
                            queriesList.get(i).add(executeQuery);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (VIdsOne.size() == 0)
                    continue;
                // Query 3:
                queryNum = 3;
                for (String id : VIdsOne) {
                    executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, "'" + id + "'", "");
                    try {
                        results3 = connection.getClient().submit(executeQuery2).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            executeQuery2 = "";
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                            result.append("Results are: " + x1 + ", " + x2 + "\n");
                            System.out.println("Query sequence " + i + ": false\n");
                            System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            continue;
                        } else {
                            for (Result r : results3) {
                                ReferenceVertex t = (ReferenceVertex) r.getObject();
                                VIdsTwo.add(t.id().toString());
                            }
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");

                            // Check result:
                            if (!VIdsTwo.contains(x1)) {
                                result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                                result.append("Results are: " + x1 + ", " + x2 + "\n");
                                System.out.println("Query sequence " + i + ": false\n");
                                System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }
            }
            out.close();
            result.close();
        } else if (database == 0) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                String x1 = "", x2 = "";
                int variant = 8;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        hugeResult1.data().forEach(r -> {
                            VAllIds.add(String.valueOf(r));
                        });
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), "");
                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult2 = hugeGraph.gremlin(executeQuery).execute();
                        if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            hugeResult2.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsOne.add(String.valueOf(t.get("id")));
                            });
                            index = x;
                            x1 = VAllIds.get(x);
                            VIdsOne.remove(x1);
                            queriesList.get(i).add(executeQuery);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (VIdsOne.size() == 0)
                    continue;
                // Query 3:
                queryNum = 3;
                for (String id : VIdsOne) {
                    executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, id, "");
                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult3 = hugeGraph.gremlin(executeQuery2).execute();
                        if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                            executeQuery2 = "";
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                            result.append("Results are: " + x1 + ", " + x2 + "\n");
                            System.out.println("Query sequence " + i + ": false\n");
                            System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            continue;
                        } else {
                            hugeResult3.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsTwo.add(String.valueOf(t.get("id")));
                            });
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");

                            // Check result:
                            if (!VIdsTwo.contains(x1)) {
                                result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                                result.append("Results are: " + x1 + ", " + x2 + "\n");
                                System.out.println("Query sequence " + i + ": false\n");
                                System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 1: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }
            }
            out.close();
            result.close();
        } else if (database == 1) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                String x1 = "", x2 = "";
                int variant = 8;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), "");
                    try {
                        results2 = connection.getClient().submit(executeQuery).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            for (Result r : results2) {
                                DetachedVertex t = (DetachedVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            index = x;
                            x1 = VAllIds.get(x);
                            VIdsOne.remove(x1);
                            queriesList.get(i).add(executeQuery);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (VIdsOne.size() == 0)
                    continue;
                // Query 3:
                queryNum = 3;
                for (String id : VIdsOne) {
                    executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, id, "");
                    try {
                        results3 = connection.getClient().submit(executeQuery2).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            executeQuery2 = "";
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                            result.append("Results are: " + x1 + ", " + x2 + "\n");
                            System.out.println("Query sequence " + i + ": false\n");
                            System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            continue;
                        } else {
                            for (Result r : results3) {
                                DetachedVertex t = (DetachedVertex) r.getObject();
                                VIdsTwo.add(t.id().toString());
                            }
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");

                            // Check result:
                            if (!VIdsTwo.contains(x1)) {
                                result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                                result.append("Results are: " + x1 + ", " + x2 + "\n");
                                System.out.println("Query sequence " + i + ": false\n");
                                System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }
            }
            out.close();
            result.close();
        } else if (database == 2) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                String x1 = "", x2 = "";
                int variant = 8;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), "");
                    try {
                        results2 = connection.getClient().submit(executeQuery).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            for (Result r : results2) {
                                ReferenceVertex t = (ReferenceVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            index = x;
                            x1 = VAllIds.get(x);
                            VIdsOne.remove(x1);
                            queriesList.get(i).add(executeQuery);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (VIdsOne.size() == 0)
                    continue;
                // Query 3:
                queryNum = 3;
                for (String id : VIdsOne) {
                    executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, id, "");
                    try {
                        results3 = connection.getClient().submit(executeQuery2).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            executeQuery2 = "";
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                            result.append("Results are: " + x1 + ", " + x2 + "\n");
                            System.out.println("Query sequence " + i + ": false\n");
                            System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            continue;
                        } else {
                            for (Result r : results3) {
                                ReferenceVertex t = (ReferenceVertex) r.getObject();
                                VIdsTwo.add(t.id().toString());
                            }
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");

                            // Check result:
                            if (!VIdsTwo.contains(x1)) {
                                result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                                result.append("Results are: " + x1 + ", " + x2 + "\n");
                                System.out.println("Query sequence " + i + ": false\n");
                                System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery21(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr21" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr21" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 5) {
            return;
        } else if (database == 4) {
            return;
        } else if (database == 0) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsAncestor = new ArrayList<>();
                List<String> VIdsDescendant = new ArrayList<>();
                String x1 = "", x2 = "";
                int variant = 9;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        hugeResult1.data().forEach(r -> {
                            VAllIds.add(String.valueOf(r));
                        });
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), "");
                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult2 = hugeGraph.gremlin(executeQuery).execute();
                        if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            hugeResult2.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                ArrayList<HashMap> t2 = (ArrayList<HashMap>) t.get("objects");
                                t2.forEach(r2 -> {
                                    if (!VIdsDescendant.contains(String.valueOf(r2.get("id")))) {
                                        VIdsDescendant.add(String.valueOf(r2.get("id")));
                                    }
                                });
                            });
                            index = x;
                            x1 = VAllIds.get(x);
                            queriesList.get(i).add(executeQuery);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (VIdsDescendant.size() == 0)
                    continue;
                // Query 3:
                queryNum = 3;
                for (String id : VIdsDescendant) {
                    boolean flag = true;
                    for (char c: id.toCharArray()) {
                        if (!Character.isDigit(c)) {
                            flag = false;
                            break;
                        }
                    }
                    if (flag == false)
                        continue;
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, id, "");
                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult3 = hugeGraph.gremlin(executeQuery).execute();
                        if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            hugeResult3.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                ArrayList<HashMap> t2 = (ArrayList<HashMap>) t.get("objects");
                                t2.forEach(r2 -> {
                                    if (!VIdsAncestor.contains(String.valueOf(r2.get("id")))) {
                                        VIdsAncestor.add(String.valueOf(r2.get("id")));
                                    }
                                });
                            });
                            x2 = id;
                            queriesList.get(i).add(executeQuery);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                // Check result
                if (!VIdsAncestor.contains(x1)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + x1 + ", " + x2 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsAncestor = new ArrayList<>();
                List<String> VIdsDescendant = new ArrayList<>();
                String x1 = "", x2 = "";
                int variant = 9;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), "");
                    try {
                        results2 = connection.getClient().submit(executeQuery).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            for (Result r : results2) {
                                String t = r.getObject().toString();
                                String p = "v.\\d+.";
                                Pattern pattern = Pattern.compile(p);
                                Matcher matcher = pattern.matcher(t);
                                while (matcher.find()) {
                                    if (!VIdsDescendant.contains(matcher.group())) {
                                        VIdsDescendant.add(matcher.group().replaceAll("[^0-9]", ""));
                                    }
                                }
                            }
                            index = x;
                            x1 = VAllIds.get(x);
                            queriesList.get(i).add(executeQuery);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (VIdsDescendant.size() == 0)
                    continue;
                // Query 3:
                queryNum = 3;
                for (String id : VIdsDescendant) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, id, "");
                    try {
                        results3 = connection.getClient().submit(executeQuery).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            for (Result r : results3) {
                                String t = r.getObject().toString();
                                String p = "v.\\d+.";
                                Pattern pattern = Pattern.compile(p);
                                Matcher matcher = pattern.matcher(t);
                                while (matcher.find()) {
                                    if (!VIdsAncestor.contains(matcher.group())) {
                                        VIdsAncestor.add(matcher.group().replaceAll("[^0-9]", ""));
                                    }
                                }
                            }
                            x2 = id;
                            queriesList.get(i).add(executeQuery);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                // Check result
                if (!VIdsAncestor.contains(x1)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Results are: " + x1 + ", " + x2 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery22(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr22" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr22" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 5) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                String x1 = "", x2 = "";
                int variant = 7;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(r.getString());
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                String k = String.valueOf(Randomly.getInteger(6));
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "'" + VAllIds.get(x) + "'", k);
                    try {
                        results2 = connection.getClient().submit(executeQuery).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            for (Result r : results2) {
                                DetachedVertex t = (DetachedVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            index = x;
                            x1 = VAllIds.get(x);
                            queriesList.get(i).add(executeQuery);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (VIdsOne.size() == 0)
                    continue;
                // Query 3:
                queryNum = 3;
                for (String id: VIdsOne) {
                    executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, "'" + id + "'", k);
                    try {
                        results3 = connection.getClient().submit(executeQuery2).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            executeQuery2 = "";
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                            result.append("Results are: " + x1 + ", " + x2 + "\n");
                            System.out.println("Query sequence " + i + ": false\n");
                            System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            continue;
                        } else {
                            for (Result r : results3) {
                                DetachedVertex t = (DetachedVertex) r.getObject();
                                VIdsTwo.add(t.id().toString());
                            }
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");

                            // Check result:
                            if (!VIdsTwo.contains(x1)) {
                                result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                                result.append("Results are: " + x1 + ", " + x2 + "\n");
                                System.out.println("Query sequence " + i + ": false\n");
                                System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }
            }
            out.close();
            result.close();
        } else if (database == 4) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                String x1 = "", x2 = "";
                int variant = 7;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(r.getString());
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                String k = String.valueOf(Randomly.getInteger(6));
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "'" + VAllIds.get(x) + "'", k);
                    try {
                        results2 = connection.getClient().submit(executeQuery).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            for (Result r : results2) {
                                ReferenceVertex t = (ReferenceVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            index = x;
                            x1 = VAllIds.get(x);
                            queriesList.get(i).add(executeQuery);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (VIdsOne.size() == 0)
                    continue;
                // Query 3:
                queryNum = 3;
                for (String id: VIdsOne) {
                    executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, "'" + id + "'", k);
                    try {
                        results3 = connection.getClient().submit(executeQuery2).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            executeQuery2 = "";
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                            result.append("Results are: " + x1 + ", " + x2 + "\n");
                            System.out.println("Query sequence " + i + ": false\n");
                            System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            continue;
                        } else {
                            for (Result r : results3) {
                                ReferenceVertex t = (ReferenceVertex) r.getObject();
                                VIdsTwo.add(t.id().toString());
                            }
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");

                            // Check result:
                            if (!VIdsTwo.contains(x1)) {
                                result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                                result.append("Results are: " + x1 + ", " + x2 + "\n");
                                System.out.println("Query sequence " + i + ": false\n");
                                System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }
            }
            out.close();
            result.close();
        } else if (database == 0) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                String x1 = "", x2 = "";
                int variant = 7;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        hugeResult1.data().forEach(r -> {
                            VAllIds.add(String.valueOf(r));
                        });
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                String k = String.valueOf(Randomly.getInteger(6));
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), k);
                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult2 = hugeGraph.gremlin(executeQuery).execute();
                        if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            hugeResult2.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsOne.add(String.valueOf(t.get("id")));
                            });
                            index = x;
                            x1 = VAllIds.get(x);
                            queriesList.get(i).add(executeQuery);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (VIdsOne.size() == 0)
                    continue;
                // Query 3:
                queryNum = 3;
                for (String id: VIdsOne) {
                    executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, id, k);
                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult3 = hugeGraph.gremlin(executeQuery2).execute();
                        if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                            executeQuery2 = "";
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                            result.append("Results are: " + x1 + ", " + x2 + "\n");
                            System.out.println("Query sequence " + i + ": false\n");
                            System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            continue;
                        } else {
                            hugeResult3.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsTwo.add(String.valueOf(t.get("id")));
                            });
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");

                            // Check result:
                            if (!VIdsTwo.contains(x1)) {
                                result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                                result.append("Results are: " + x1 + ", " + x2 + "\n");
                                System.out.println("Query sequence " + i + ": false\n");
                                System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }
            }
            out.close();
            result.close();
        } else if (database == 1) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                String x1 = "", x2 = "";
                int variant = 7;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                String k = String.valueOf(Randomly.getInteger(6));
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), k);
                    try {
                        results2 = connection.getClient().submit(executeQuery).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            for (Result r : results2) {
                                DetachedVertex t = (DetachedVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            index = x;
                            x1 = VAllIds.get(x);
                            queriesList.get(i).add(executeQuery);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (VIdsOne.size() == 0)
                    continue;
                // Query 3:
                queryNum = 3;
                for (String id: VIdsOne) {
                    executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, id, k);
                    try {
                        results3 = connection.getClient().submit(executeQuery2).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            executeQuery2 = "";
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                            result.append("Results are: " + x1 + ", " + x2 + "\n");
                            System.out.println("Query sequence " + i + ": false\n");
                            System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            continue;
                        } else {
                            for (Result r : results3) {
                                DetachedVertex t = (DetachedVertex) r.getObject();
                                VIdsTwo.add(t.id().toString());
                            }
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");

                            // Check result:
                            if (!VIdsTwo.contains(x1)) {
                                result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                                result.append("Results are: " + x1 + ", " + x2 + "\n");
                                System.out.println("Query sequence " + i + ": false\n");
                                System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }
            }
            out.close();
            result.close();
        } else if (database == 2) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                String x1 = "", x2 = "";
                int variant = 7;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                String k = String.valueOf(Randomly.getInteger(6));
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), k);
                    try {
                        results2 = connection.getClient().submit(executeQuery).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        } else {
                            for (Result r : results2) {
                                ReferenceVertex t = (ReferenceVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            index = x;
                            x1 = VAllIds.get(x);
                            queriesList.get(i).add(executeQuery);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (VIdsOne.size() == 0)
                    continue;
                // Query 3:
                queryNum = 3;
                for (String id: VIdsOne) {
                    executeQuery2 = generateStrategyMRQuerySequence(queryNum, "node", variant, id, k);
                    try {
                        results3 = connection.getClient().submit(executeQuery2).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            executeQuery2 = "";
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                            result.append("Results are: " + x1 + ", " + x2 + "\n");
                            System.out.println("Query sequence " + i + ": false\n");
                            System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            continue;
                        } else {
                            for (Result r : results3) {
                                ReferenceVertex t = (ReferenceVertex) r.getObject();
                                VIdsTwo.add(t.id().toString());
                            }
                            x2 = id;
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");

                            // Check result:
                            if (!VIdsTwo.contains(x1)) {
                                result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                                result.append("Results are: " + x1 + ", " + x2 + "\n");
                                System.out.println("Query sequence " + i + ": false\n");
                                System.out.println("Results are: " + x1 + ", " + x2 + "\n");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery23(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr23" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr23" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3;
                List<String> VAllIds = new ArrayList<>();
                List<String> PathRes = new ArrayList<>();
                String x1 = "", x2 = "";
                int variant = 10;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        hugeResult1.data().forEach(r -> {
                            VAllIds.add(String.valueOf(r));
                        });
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                if (1 == 1)
                    continue;
                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    for (int y = 0; y < VAllIds.size(); y++) {
                        if (y == x)
                            continue;
                        executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), VAllIds.get(y));
                        try {
                            GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                            hugeResult2 = hugeGraph.gremlin(executeQuery).execute();
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                                executeQuery = "";
                                queriesList.get(i).add(executeQuery);
                                continue;
                            } else {
                                hugeResult2.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                });
                                queriesList.get(i).add(executeQuery);
                                out.append("query 2:" + "\n");
                                out.append(executeQuery + "\n");
                                System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

                // Manually check result
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                String executeQuery = "", executeQuery2 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3;
                List<String> VAllIds = new ArrayList<>();
                List<String> PathRes = new ArrayList<>();
                String x1 = "", x2 = "";
                int variant = 10;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Query 2:
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    for (int y = 0; y < VAllIds.size(); y++) {
                        if (y == x)
                            continue;
                        executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), VAllIds.get(y));
                        try {
                            results2 = connection.getClient().submit(executeQuery).all().get();
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            if (results2.size() == 0 || results2 == null) {
                                executeQuery = "";
                                queriesList.get(i).add(executeQuery);
                                continue;
                            } else {
                                for (Result r : results2) {
                                    PathRes.add(r.getObject().toString());
                                }
                                queriesList.get(i).add(executeQuery);
                                out.append("query 2:" + "\n");
                                out.append(executeQuery + "\n");
                                System.out.println("Query sequence " + i + ", 2: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

                // Manually check result
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery24(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr24" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr24" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 5) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                int variant = 11;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results1) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsOne.add(res);
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results2) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsTwo.add(res);
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery2);
                    out.append("query 2:" + "\n");
                    out.append(executeQuery2 + "\n");
                    System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results3 = connection.getClient().submit(executeQuery3).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results3) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsThree.add(res);
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results4 = connection.getClient().submit(executeQuery4).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        executeQuery4 = "";
                        queriesList.get(i).add(executeQuery4);
                        continue;
                    } else {
                        String rclass = results4.get(0).toString().split(" ")[1];
                        System.out.println("rclass 4: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results4) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsFour.add(res);
                            }
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery4 = "";
                            queriesList.get(i).add(executeQuery4);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery4);
                    out.append("query 4:" + "\n");
                    out.append(executeQuery4 + "\n");
                    System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 4) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                int variant = 11;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results1) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsOne.add(res);
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results2) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsTwo.add(res);
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery2);
                    out.append("query 2:" + "\n");
                    out.append(executeQuery2 + "\n");
                    System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results3 = connection.getClient().submit(executeQuery3).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results3) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsThree.add(res);
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results4 = connection.getClient().submit(executeQuery4).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        executeQuery4 = "";
                        queriesList.get(i).add(executeQuery4);
                        continue;
                    } else {
                        String rclass = results4.get(0).toString().split(" ")[1];
                        System.out.println("rclass 4: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results4) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsFour.add(res);
                            }
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery4 = "";
                            queriesList.get(i).add(executeQuery4);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery4);
                    out.append("query 4:" + "\n");
                    out.append(executeQuery4 + "\n");
                    System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        }
        else if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3, hugeResult4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                int variant = 11;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult1.data().get(0);
                        String rclass = (String) tempResult.get("type");
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("vertex")) {
                            hugeResult1.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsOne.add(String.valueOf(t.get("id")));
                            });
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult2 = hugeGraph.gremlin(executeQuery2).execute();
                    if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult2.data().get(0);
                        String rclass = (String) tempResult.get("type");
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("vertex")) {
                            hugeResult2.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsTwo.add(String.valueOf(t.get("id")));
                            });
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery2);
                    out.append("query 2:" + "\n");
                    out.append(executeQuery2 + "\n");
                    System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult3 = hugeGraph.gremlin(executeQuery3).execute();
                    if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult3.data().get(0);
                        String rclass = (String) tempResult.get("type");
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("vertex")) {
                            hugeResult3.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsThree.add(String.valueOf(t.get("id")));
                            });
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult4 = hugeGraph.gremlin(executeQuery4).execute();
                    if (hugeResult4.data().size() == 0 || hugeResult4.data() == null) {
                        executeQuery4 = "";
                        queriesList.get(i).add(executeQuery4);
                        continue;
                    } else {
                        HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult4.data().get(0);
                        String rclass = (String) tempResult.get("type");
                        System.out.println("rclass 4: " + rclass);
                        if (rclass.equals("vertex")) {
                            hugeResult4.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsFour.add(String.valueOf(t.get("id")));
                            });
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery4 = "";
                            queriesList.get(i).add(executeQuery4);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery4);
                    out.append("query 4:" + "\n");
                    out.append(executeQuery4 + "\n");
                    System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                int variant = 11;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results1) {
                                String res = r.getObject().toString();
                                VIdsOne.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results2) {
                                String res = r.getObject().toString();
                                VIdsTwo.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery2);
                    out.append("query 2:" + "\n");
                    out.append(executeQuery2 + "\n");
                    System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results3 = connection.getClient().submit(executeQuery3).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results3) {
                                String res = r.getObject().toString();
                                VIdsThree.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results4 = connection.getClient().submit(executeQuery4).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        executeQuery4 = "";
                        queriesList.get(i).add(executeQuery4);
                        continue;
                    } else {
                        String rclass = results4.get(0).toString().split(" ")[1];
                        System.out.println("rclass 4: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results4) {
                                String res = r.getObject().toString();
                                VIdsFour.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery4 = "";
                            queriesList.get(i).add(executeQuery4);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery4);
                    out.append("query 4:" + "\n");
                    out.append(executeQuery4 + "\n");
                    System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append("\n");
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                int variant = 11;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results1) {
                                String res = r.getObject().toString();
                                VIdsOne.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        String rclass = results2.get(0).toString().split(" ")[1];
                        System.out.println("rclass 2: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results2) {
                                String res = r.getObject().toString();
                                VIdsTwo.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery2);
                    out.append("query 2:" + "\n");
                    out.append(executeQuery2 + "\n");
                    System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results3 = connection.getClient().submit(executeQuery3).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        String rclass = results3.get(0).toString().split(" ")[1];
                        System.out.println("rclass 3: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results3) {
                                String res = r.getObject().toString();
                                VIdsThree.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery3 = "";
                            queriesList.get(i).add(executeQuery3);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results4 = connection.getClient().submit(executeQuery4).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        executeQuery4 = "";
                        queriesList.get(i).add(executeQuery4);
                        continue;
                    } else {
                        String rclass = results4.get(0).toString().split(" ")[1];
                        System.out.println("rclass 4: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results4) {
                                String res = r.getObject().toString();
                                VIdsFour.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery4 = "";
                            queriesList.get(i).add(executeQuery4);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery4);
                    out.append("query 4:" + "\n");
                    out.append(executeQuery4 + "\n");
                    System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeMRQuery25(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr25" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr25" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 5) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                int size1 = 0, size2 = 0, size3 = 0, size4 = 0;
                int variant = 12;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = (int) r.getLong();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        for (Result r : results2) {
                            size2 = (int) r.getLong();
                        }
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery2);
                    out.append("query 2:" + "\n");
                    out.append(executeQuery2 + "\n");
                    System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results3 = connection.getClient().submit(executeQuery3).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        for (Result r : results3) {
                            size3 = (int) r.getLong();
                        }
                        queriesList.get(i).add(executeQuery3);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery3 + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results4 = connection.getClient().submit(executeQuery4).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        executeQuery4 = "";
                        queriesList.get(i).add(executeQuery4);
                        continue;
                    } else {
                        for (Result r : results4) {
                            size4 = (int) r.getLong();
                        }
                        queriesList.get(i).add(executeQuery4);
                        out.append("query 4:" + "\n");
                        out.append(executeQuery4 + "\n");
                        System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery4);
                    out.append("query 4:" + "\n");
                    out.append(executeQuery4 + "\n");
                    System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                int union = size2 + size3 + size4;
                if (union != size1) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Size1 results: " + size1 + "\n");
                    result.append("Size2 results: " + size2 + "\n");
                    result.append("Size3 results: " + size3 + "\n");
                    result.append("Size4 results: " + size4 + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Size1 results: " + size1 + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("Size1 results: " + size1 + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        }
        else if (database == 4) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                int size1 = 0, size2 = 0, size3 = 0, size4 = 0;
                int variant = 12;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = (int) r.getLong();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        for (Result r : results2) {
                            size2 = (int) r.getLong();
                        }
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery2);
                    out.append("query 2:" + "\n");
                    out.append(executeQuery2 + "\n");
                    System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results3 = connection.getClient().submit(executeQuery3).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        for (Result r : results3) {
                            size3 = (int) r.getLong();
                        }
                        queriesList.get(i).add(executeQuery3);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery3 + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results4 = connection.getClient().submit(executeQuery4).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        executeQuery4 = "";
                        queriesList.get(i).add(executeQuery4);
                        continue;
                    } else {
                        for (Result r : results4) {
                            size4 = (int) r.getLong();
                        }
                        queriesList.get(i).add(executeQuery4);
                        out.append("query 4:" + "\n");
                        out.append(executeQuery4 + "\n");
                        System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery4);
                    out.append("query 4:" + "\n");
                    out.append(executeQuery4 + "\n");
                    System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                int union = size2 + size3 + size4;
                if (union != size1) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Size1 results: " + size1 + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Size1 results: " + size1 + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("Size1 results: " + size1 + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3, hugeResult4;
                int size1 = 0, size2 = 0, size3 = 0, size4 = 0;
                int variant = 12;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        size1 = (int) hugeResult1.data().get(0);
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }


                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult2 = hugeGraph.gremlin(executeQuery2).execute();
                    if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        size2 = (int) hugeResult2.data().get(0);
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery2);
                    out.append("query 2:" + "\n");
                    out.append(executeQuery2 + "\n");
                    System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult3 = hugeGraph.gremlin(executeQuery3).execute();
                    if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        size3 = (int) hugeResult3.data().get(0);
                        queriesList.get(i).add(executeQuery3);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery3 + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult4 = hugeGraph.gremlin(executeQuery4).execute();
                    if (hugeResult4.data().size() == 0 || hugeResult4.data() == null) {
                        executeQuery4 = "";
                        queriesList.get(i).add(executeQuery4);
                        continue;
                    } else {
                        size4 = (int) hugeResult4.data().get(0);
                        queriesList.get(i).add(executeQuery4);
                        out.append("query 4:" + "\n");
                        out.append(executeQuery4 + "\n");
                        System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery4);
                    out.append("query 4:" + "\n");
                    out.append(executeQuery4 + "\n");
                    System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                int union = size2 + size3 + size4;
                if (union != size1) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Size1 results: " + size1 + "\n");
                    result.append("Size2 results: " + size2 + "\n");
                    result.append("Size3 results: " + size3 + "\n");
                    result.append("Size4 results: " + size4 + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Size1 results: " + size1 + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("Size1 results: " + size1 + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4;
                int size1 = 0, size2 = 0, size3 = 0, size4 = 0;
                int variant = 12;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = (int) r.getLong();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results2 = connection.getClient().submit(executeQuery2).all().get();
                    if (results2.size() == 0 || results2 == null) {
                        executeQuery2 = "";
                        queriesList.get(i).add(executeQuery2);
                        continue;
                    } else {
                        for (Result r : results2) {
                            size2 = (int) r.getLong();
                        }
                        queriesList.get(i).add(executeQuery2);
                        out.append("query 2:" + "\n");
                        out.append(executeQuery2 + "\n");
                        System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery2);
                    out.append("query 2:" + "\n");
                    out.append(executeQuery2 + "\n");
                    System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results3 = connection.getClient().submit(executeQuery3).all().get();
                    if (results3.size() == 0 || results3 == null) {
                        executeQuery3 = "";
                        queriesList.get(i).add(executeQuery3);
                        continue;
                    } else {
                        for (Result r : results3) {
                            size3 = (int) r.getLong();
                        }
                        queriesList.get(i).add(executeQuery3);
                        out.append("query 3:" + "\n");
                        out.append(executeQuery3 + "\n");
                        System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery3);
                    out.append("query 3:" + "\n");
                    out.append(executeQuery3 + "\n");
                    System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results4 = connection.getClient().submit(executeQuery4).all().get();
                    if (results4.size() == 0 || results4 == null) {
                        executeQuery4 = "";
                        queriesList.get(i).add(executeQuery4);
                        continue;
                    } else {
                        for (Result r : results4) {
                            size4 = (int) r.getLong();
                        }
                        queriesList.get(i).add(executeQuery4);
                        out.append("query 4:" + "\n");
                        out.append(executeQuery4 + "\n");
                        System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    queriesList.get(i).add(executeQuery4);
                    out.append("query 4:" + "\n");
                    out.append(executeQuery4 + "\n");
                    System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                int union = size2 + size3 + size4;
                if (union != size1) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("Size1 results: " + size1 + "\n");
                    result.append("Size2 results: " + size2 + "\n");
                    result.append("Size3 results: " + size3 + "\n");
                    result.append("Size4 results: " + size4 + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("Size1 results: " + size1 + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("Size1 results: " + size1 + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeQuery26(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr26" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr26" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "", executeQuery6 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3, hugeResult4, hugeResult5, hugeResult6;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsOne_2 = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsDel = new ArrayList<>();
                int variant = 13;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult1.data().get(0);
                        String rclass = (String) tempResult.get("type");
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("vertex")) {
                            hugeResult1.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsOne.add(String.valueOf(t.get("id")));
                            });
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Delete all the other nodes that not in query 1
                // Query 2:
                queryNum = 2;
                executeQuery5 = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult5 = hugeGraph.gremlin(executeQuery5).execute();
                    if (hugeResult5.data().size() == 0 || hugeResult5.data() == null) {
                        executeQuery5 = "";
                        queriesList.get(i).add(executeQuery5);
                        continue;
                    } else {
                        hugeResult5.data().forEach(r -> {
                            HashMap<String, Object> t = (HashMap<String, Object>) r;
                            VAllIds.add(String.valueOf(t.get("id")));
                        });
                        queriesList.get(i).add(executeQuery5);
                        out.append("query 5:" + "\n");
                        out.append(executeQuery5 + "\n");
                        System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }
                for (String id: VAllIds) {
                    if (!VIdsOne.contains(id))
                        VIdsDel.add(id);
                }
                String delIds = String.join(",", VIdsDel);
                if (delIds.equals(""))
                    continue;

                // Query 3:
                queryNum = 3;
                executeQuery6 = generateStrategyMRQuerySequence(queryNum, "node", variant, delIds, "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult6 = hugeGraph.gremlin(executeQuery6).execute();
                    queriesList.get(i).add(executeQuery6);
                    out.append("query 6:" + "\n");
                    out.append(executeQuery6 + "\n");
                    System.out.println("Query sequence " + i + ", 6: " + executeQuery6 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                } catch (Exception e ){
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery6 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery6 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult1.data().get(0);
                        String rclass = (String) tempResult.get("type");
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("vertex")) {
                            hugeResult1.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsOne.add(String.valueOf(t.get("id")));
                            });
                            queriesList.get(i).add(executeQuery);
                            out.append("query 7:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 7: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsOne_2.sort(Comparator.naturalOrder());
                if (!VIdsOne.equals(VIdsOne_2)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsOne_2 results: " + VIdsOne_2 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("VIdsOne_2 results: " + VIdsOne_2 + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "", executeQuery6 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5, results6;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsOne_2 = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsDel = new ArrayList<>();
                int variant = 13;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results1) {
                                String res = r.getObject().toString();
                                VIdsOne.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Delete all the other nodes that not in query 1
                // Query 2:
                queryNum = 2;
                executeQuery5 = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results5 = connection.getClient().submit(executeQuery5).all().get();
                    if (results5.size() == 0 || results5 == null) {
                        executeQuery5 = "";
                        queriesList.get(i).add(executeQuery5);
                        continue;
                    } else {
                        for (Result r : results5) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery5);
                        out.append("query 5:" + "\n");
                        out.append(executeQuery5 + "\n");
                        System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }
                for (String id: VAllIds) {
                    if (!VIdsOne.contains(id))
                        VIdsDel.add(id);
                }
                String delIds = String.join(",", VIdsDel);
                if (delIds.equals(""))
                    continue;

                // Query 3:
                queryNum = 3;
                executeQuery6 = generateStrategyMRQuerySequence(queryNum, "node", variant, delIds, "");
                try {
                    results6 = connection.getClient().submit(executeQuery6).all().get();
                    queriesList.get(i).add(executeQuery6);
                    out.append("query 6:" + "\n");
                    out.append(executeQuery6 + "\n");
                    System.out.println("Query sequence " + i + ", 6: " + executeQuery6 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                } catch (Exception e ){
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery6 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery6 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results1) {
                                String res = r.getObject().toString();
                                VIdsOne_2.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 7:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 7: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsOne_2.sort(Comparator.naturalOrder());
                if (!VIdsOne.equals(VIdsOne_2)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsOne_2 results: " + VIdsOne_2 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("VIdsOne_2 results: " + VIdsOne_2 + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "", executeQuery6 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5, results6;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsOne_2 = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsDel = new ArrayList<>();
                int variant = 13;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results1) {
                                String res = r.getObject().toString();
                                VIdsOne.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Delete all the other nodes that not in query 1
                // Query 2:
                queryNum = 2;
                executeQuery5 = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results5 = connection.getClient().submit(executeQuery5).all().get();
                    if (results5.size() == 0 || results5 == null) {
                        executeQuery5 = "";
                        queriesList.get(i).add(executeQuery5);
                        continue;
                    } else {
                        for (Result r : results5) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery5);
                        out.append("query 5:" + "\n");
                        out.append(executeQuery5 + "\n");
                        System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                    result.append("Query sequence " + i + ", 2: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }
                for (String id: VAllIds) {
                    if (!VIdsOne.contains(id))
                        VIdsDel.add(id);
                }
                String delIds = String.join(",", VIdsDel);
                if (delIds.equals(""))
                    continue;

                // Query 3:
                queryNum = 3;
                executeQuery6 = generateStrategyMRQuerySequence(queryNum, "node", variant, delIds, "");
                try {
                    results6 = connection.getClient().submit(executeQuery6).all().get();
                    queriesList.get(i).add(executeQuery6);
                    out.append("query 6:" + "\n");
                    out.append(executeQuery6 + "\n");
                    System.out.println("Query sequence " + i + ", 6: " + executeQuery6 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                } catch (Exception e ){
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery6 + "\n");
                    result.append("Query sequence " + i + ", 3: " + executeQuery6 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results1) {
                                String res = r.getObject().toString();
                                VIdsOne_2.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 7:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 7: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsOne_2.sort(Comparator.naturalOrder());
                if (!VIdsOne.equals(VIdsOne_2)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsOne_2 results: " + VIdsOne_2 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("VIdsOne_2 results: " + VIdsOne_2 + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeQuery27(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr27" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr27" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
            }
            out.close();
            result.close();
        } else if (database == 1) {
            for (int i = 0; i < state.getQueryNum(); i++) {
            }
            out.close();
            result.close();
        } else if (database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
            }
            out.close();
            result.close();
        }
    }

    public void executeQuery28(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr28" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr28" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                List<String> addVerticesQueries = null;
                List<String> addEdgesQueries = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "", executeQuery6 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5, results6;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsOne_2 = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                List<String> VAddIds = new ArrayList<>();
                int variant = 14;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results1) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsOne.add(res);
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                addVerticesQueries = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                for (String query : addVerticesQueries) {
                    try {
                        results5 = connection.getClient().submit(query).all().get();
                        if (results5.size() == 0 || results5 == null) {
                            query = "";
                            queriesList.get(i).add(query);
                            continue;
                        } else {
                            for (Result r : results5) {
                                String tmp = r.getString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VAddIds.add(res);
                            }
                            queriesList.get(i).add(query);
                            out.append("query 2:" + "\n");
                            out.append(query + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + query + "\n");
                        result.append("Query sequence " + i + ", 2: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                // Query 3:
                queryNum = 3;
                String addIds = String.join("&", VAddIds);
                addEdgesQueries = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, addIds, "").split("&"));
                for (String query : addEdgesQueries) {
                    try {
                        results6 = connection.getClient().submit(query).all().get();
                        if (results6.size() == 0 || results6 == null) {
                            query = "";
                            queriesList.get(i).add(query);
                            continue;
                        } else {
                            queriesList.get(i).add(query);
                            out.append("query 3:" + "\n");
                            out.append(query + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + query + "\n");
                        result.append("Query sequence " + i + ", 3: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (executeQuery.equals("g.V()"))
                    continue;
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results1) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsOne_2.add(res);
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsOne_2.sort(Comparator.naturalOrder());
                if (!VIdsOne.equals(VIdsOne_2)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsOne_2 results: " + VIdsOne_2 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("VIdsOne_2 results: " + VIdsOne_2 + "\n");
                }
            }
            out.close();
            result.close();
        }
        else if (database == 3) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                List<String> addVerticesQueries = null;
                List<String> addEdgesQueries = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "", executeQuery6 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5, results6;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsOne_2 = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                List<String> VAddIds = new ArrayList<>();
                int variant = 14;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results1) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsOne.add(res);
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                addVerticesQueries = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                for (String query : addVerticesQueries) {
                    try {
                        results5 = connection.getClient().submit(query).all().get();
                        if (results5.size() == 0 || results5 == null) {
                            query = "";
                            queriesList.get(i).add(query);
                            continue;
                        } else {
                            for (Result r : results5) {
                                String tmp = r.getString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VAddIds.add(res);
                            }
                            queriesList.get(i).add(query);
                            out.append("query 2:" + "\n");
                            out.append(query + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + query + "\n");
                        result.append("Query sequence " + i + ", 2: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                // Query 3:
                queryNum = 3;
                String addIds = String.join("&", VAddIds);
                addEdgesQueries = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, addIds, "").split("&"));
                for (String query : addEdgesQueries) {
                    try {
                        results6 = connection.getClient().submit(query).all().get();
                        if (results6.size() == 0 || results6 == null) {
                            query = "";
                            queriesList.get(i).add(query);
                            continue;
                        } else {
                            queriesList.get(i).add(query);
                            out.append("query 3:" + "\n");
                            out.append(query + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + query + "\n");
                        result.append("Query sequence " + i + ", 3: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (executeQuery.equals("g.V()"))
                    continue;
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results1) {
                                String tmp = r.getObject().toString().substring(2);
                                String res = tmp.substring(0, tmp.length() - 1);
                                VIdsOne_2.add(res);
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsOne_2.sort(Comparator.naturalOrder());
                if (!VIdsOne.equals(VIdsOne_2)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsOne_2 results: " + VIdsOne_2 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("VIdsOne_2 results: " + VIdsOne_2 + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                List<String> addVerticesQueries = null;
                List<String> addEdgesQueries = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "", executeQuery6 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5, results6;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsOne_2 = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                List<String> VAddIds = new ArrayList<>();
                int variant = 14;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results1) {
                                String res = r.getObject().toString();
                                VIdsOne.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                addVerticesQueries = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                for (String query : addVerticesQueries) {
                    try {
                        results5 = connection.getClient().submit(query).all().get();
                        if (results5.size() == 0 || results5 == null) {
                            query = "";
                            queriesList.get(i).add(query);
                            continue;
                        } else {
                            for (Result r : results5) {
                                String res = r.getString();
                                VAddIds.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(query);
                            out.append("query 2:" + "\n");
                            out.append(query + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + query + "\n");
                        result.append("Query sequence " + i + ", 2: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                // Query 3:
                queryNum = 3;
                String addIds = String.join("&", VAddIds);
                addEdgesQueries = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, addIds, "").split("&"));
                for (String query : addEdgesQueries) {
                    try {
                        results6 = connection.getClient().submit(query).all().get();
                        if (results6.size() == 0 || results6 == null) {
                            query = "";
                            queriesList.get(i).add(query);
                            continue;
                        } else {
                            queriesList.get(i).add(query);
                            out.append("query 3:" + "\n");
                            out.append(query + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + query + "\n");
                        result.append("Query sequence " + i + ", 3: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (executeQuery.equals("g.V()"))
                    continue;
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                            for (Result r : results1) {
                                String res = r.getObject().toString();
                                VIdsOne_2.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsOne_2.sort(Comparator.naturalOrder());
                if (!VIdsOne.equals(VIdsOne_2)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsOne_2 results: " + VIdsOne_2 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("VIdsOne_2 results: " + VIdsOne_2 + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                List<String> addVerticesQueries = null;
                List<String> addEdgesQueries = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "", executeQuery6 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5, results6;
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsOne_2 = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                List<String> VAddIds = new ArrayList<>();
                int variant = 14;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results1) {
                                String res = r.getObject().toString();
                                VIdsOne.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 1:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                addVerticesQueries = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                for (String query : addVerticesQueries) {
                    try {
                        results5 = connection.getClient().submit(query).all().get();
                        if (results5.size() == 0 || results5 == null) {
                            query = "";
                            queriesList.get(i).add(query);
                            continue;
                        } else {
                            for (Result r : results5) {
                                String res = r.getString();
                                VAddIds.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(query);
                            out.append("query 2:" + "\n");
                            out.append(query + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + query + "\n");
                        result.append("Query sequence " + i + ", 2: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                // Query 3:
                queryNum = 3;
                String addIds = String.join("&", VAddIds);
                addEdgesQueries = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, addIds, "").split("&"));
                for (String query : addEdgesQueries) {
                    try {
                        results6 = connection.getClient().submit(query).all().get();
                        if (results6.size() == 0 || results6 == null) {
                            query = "";
                            queriesList.get(i).add(query);
                            continue;
                        } else {
                            queriesList.get(i).add(query);
                            out.append("query 3:" + "\n");
                            out.append(query + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + query + "\n");
                        result.append("Query sequence " + i + ", 3: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (executeQuery.equals("g.V()"))
                    continue;
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        String rclass = results1.get(0).toString().split(" ")[1];
                        System.out.println("rclass 1: " + rclass);
                        if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                            for (Result r : results1) {
                                String res = r.getObject().toString();
                                VIdsOne_2.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(executeQuery);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            executeQuery = "";
                            queriesList.get(i).add(executeQuery);
                            continue;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsOne_2.sort(Comparator.naturalOrder());
                if (!VIdsOne.equals(VIdsOne_2)) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsOne_2 results: " + VIdsOne_2 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("VIdsOne_2 results: " + VIdsOne_2 + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeQuery29(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr29" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr29" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
            }
            out.close();
            result.close();
        } else if (database == 1 || database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                List<String> addVerticesQueries = null;
                List<String> addEdgesQueries = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "", executeQuery6 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5, results6;
                List<String> VAddIds = new ArrayList<>();
                int size1 = 0, size2 = 0, size3 = 0, size4 = 0, size5 = 0;
                int variant = 15;

                // Query 1:
                int queryNum = 1;
                connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                executeQuery = connectedQuery.get(0);
                executeQuery2 = connectedQuery.get(1);
                executeQuery3 = connectedQuery.get(2);
                executeQuery4 = connectedQuery.get(3);

                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size1 = (int) r.getLong();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2:
                queryNum = 2;
                addVerticesQueries = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "", "").split("&"));
                for (String query : addVerticesQueries) {
                    try {
                        results5 = connection.getClient().submit(query).all().get();
                        if (results5.size() == 0 || results5 == null) {
                            query = "";
                            queriesList.get(i).add(query);
                            continue;
                        } else {
                            for (Result r : results5) {
                                String res = r.getString();
                                VAddIds.add(res.replaceAll("[^0-9]", ""));
                            }
                            queriesList.get(i).add(query);
                            out.append("query 2:" + "\n");
                            out.append(query + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + query + "\n");
                        result.append("Query sequence " + i + ", 2: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                // Query 3:
                queryNum = 3;
                String addIds = String.join("&", VAddIds);
                addEdgesQueries = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, addIds, "").split("&"));
                for (String query : addEdgesQueries) {
                    try {
                        results6 = connection.getClient().submit(query).all().get();
                        if (results6.size() == 0 || results6 == null) {
                            query = "";
                            queriesList.get(i).add(query);
                            continue;
                        } else {
                            queriesList.get(i).add(query);
                            out.append("query 3:" + "\n");
                            out.append(query + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + query + "\n");
                        result.append("Query sequence " + i + ", 3: " + query + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }
                }

                if (executeQuery.equals("g.V()"))
                    continue;
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            size5 = (int) r.getLong();
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 4: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Check result:
                // Check result
                System.out.println("Check result");
                if (size1 != size5) {
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("size1 result: " + size1 + "\n");
                    result.append("size5 result: " + size5 + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("size1 results: " + size1 + "\n");
                    System.out.println("size5 results: " + size5 + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("size1 results: " + size1 + "\n");
                    System.out.println("size5 results: " + size5 + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeQuery30(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr30" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr30" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 5) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                String x1 = "";
                int variant = 16;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(r.getString());
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2: Get spouse
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "'" + VAllIds.get(x) + "'", "").split("&"));
                    executeQuery2 = connectedQuery.get(0);
                    executeQuery3 = connectedQuery.get(1);
                    executeQuery4 = connectedQuery.get(2);
                    executeQuery5 = connectedQuery.get(3);
                    try {
                        results2 = connection.getClient().submit(executeQuery2).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        } else {
                            for (Result r : results2) {
                                DetachedVertex t = (DetachedVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        System.out.println("Query 2 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results3 = connection.getClient().submit(executeQuery3).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results3.get(0).toString().split(" ")[1];
                            System.out.println("rclass 3: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                                for (Result r : results3) {
                                    String tmp = r.getObject().toString().substring(2);
                                    String res = tmp.substring(0, tmp.length() - 1);
                                    VIdsTwo.add(res);
                                }
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 3 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results4 = connection.getClient().submit(executeQuery4).all().get();
                        if (results4.size() == 0 || results4 == null) {
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results4.get(0).toString().split(" ")[1];
                            System.out.println("rclass 4: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                                for (Result r : results4) {
                                    String tmp = r.getObject().toString().substring(2);
                                    String res = tmp.substring(0, tmp.length() - 1);
                                    VIdsThree.add(res);
                                }
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 4 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery4 + "\n");
                        result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results5 = connection.getClient().submit(executeQuery5).all().get();
                        if (results5.size() == 0 || results5 == null) {
                            queriesList.get(i).add(executeQuery5);
                            out.append("query 5:" + "\n");
                            out.append(executeQuery5 + "\n");
                            System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results5.get(0).toString().split(" ")[1];
                            System.out.println("rclass 5: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                                for (Result r : results5) {
                                    String tmp = r.getObject().toString().substring(2);
                                    String res = tmp.substring(0, tmp.length() - 1);
                                    VIdsFour.add(res);
                                }
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 5 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                        result.append("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    index = x;
                    x1 = VAllIds.get(x);
                    break;
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append("\n");
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 4) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                String x1 = "";
                int variant = 16;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(r.getString());
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2: Get spouse
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "'" + VAllIds.get(x) + "'", "").split("&"));
                    executeQuery2 = connectedQuery.get(0);
                    executeQuery3 = connectedQuery.get(1);
                    executeQuery4 = connectedQuery.get(2);
                    executeQuery5 = connectedQuery.get(3);
                    try {
                        results2 = connection.getClient().submit(executeQuery2).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        } else {
                            for (Result r : results2) {
                                ReferenceVertex t = (ReferenceVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        System.out.println("Query 2 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results3 = connection.getClient().submit(executeQuery3).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results3.get(0).toString().split(" ")[1];
                            System.out.println("rclass 3: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                                for (Result r : results3) {
                                    String tmp = r.getObject().toString().substring(2);
                                    String res = tmp.substring(0, tmp.length() - 1);
                                    VIdsTwo.add(res);
                                }
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 3 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results4 = connection.getClient().submit(executeQuery4).all().get();
                        if (results4.size() == 0 || results4 == null) {
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results4.get(0).toString().split(" ")[1];
                            System.out.println("rclass 4: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                                for (Result r : results4) {
                                    String tmp = r.getObject().toString().substring(2);
                                    String res = tmp.substring(0, tmp.length() - 1);
                                    VIdsThree.add(res);
                                }
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 4 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery4 + "\n");
                        result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results5 = connection.getClient().submit(executeQuery5).all().get();
                        if (results5.size() == 0 || results5 == null) {
                            queriesList.get(i).add(executeQuery5);
                            out.append("query 5:" + "\n");
                            out.append(executeQuery5 + "\n");
                            System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results5.get(0).toString().split(" ")[1];
                            System.out.println("rclass 5: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                                for (Result r : results5) {
                                    String tmp = r.getObject().toString().substring(2);
                                    String res = tmp.substring(0, tmp.length() - 1);
                                    VIdsFour.add(res);
                                }
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 5 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                        result.append("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    index = x;
                    x1 = VAllIds.get(x);
                    break;
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append("\n");
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 0) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3, hugeResult4, hugeResult5;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                String x1 = "";
                int variant = 16;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        hugeResult1.data().forEach(r -> {
                            VAllIds.add(String.valueOf(r));
                        });
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2: Get spouse
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), "").split("&"));
                    executeQuery2 = connectedQuery.get(0);
                    executeQuery3 = connectedQuery.get(1);
                    executeQuery4 = connectedQuery.get(2);
                    executeQuery5 = connectedQuery.get(3);
                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult2 = hugeGraph.gremlin(executeQuery2).execute();
                        if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        } else {
                            hugeResult2.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsOne.add(String.valueOf(t.get("id")));
                            });
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        System.out.println("Query 2 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult3 = hugeGraph.gremlin(executeQuery3).execute();
                        if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult3.data().get(0);
                            String rclass = (String) tempResult.get("type");
                            System.out.println("rclass 3: " + rclass);
                            if (rclass.equals("vertex")) {
                                hugeResult3.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                    VIdsTwo.add(String.valueOf(t.get("id")));
                                });
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 3 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult4 = hugeGraph.gremlin(executeQuery4).execute();
                        if (hugeResult4.data().size() == 0 || hugeResult4.data() == null) {
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult4.data().get(0);
                            String rclass = (String) tempResult.get("type");
                            System.out.println("rclass 4: " + rclass);
                            if (rclass.equals("vertex")) {
                                hugeResult4.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                    VIdsThree.add(String.valueOf(t.get("id")));
                                });
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 4 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery4 + "\n");
                        result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult5 = hugeGraph.gremlin(executeQuery5).execute();
                        if (hugeResult5.data().size() == 0 || hugeResult5.data() == null) {
                            queriesList.get(i).add(executeQuery5);
                            out.append("query 5:" + "\n");
                            out.append(executeQuery5 + "\n");
                            System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult5.data().get(0);
                            String rclass = (String) tempResult.get("type");
                            System.out.println("rclass 5: " + rclass);
                            if (rclass.equals("vertex")) {
                                hugeResult5.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                    VIdsFour.add(String.valueOf(t.get("id")));
                                });
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 5 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                        result.append("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    index = x;
                    x1 = VAllIds.get(x);
                    break;
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append("\n");
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                String x1 = "";
                int variant = 16;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2: Get spouse
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), "").split("&"));
                    executeQuery2 = connectedQuery.get(0);
                    executeQuery3 = connectedQuery.get(1);
                    executeQuery4 = connectedQuery.get(2);
                    executeQuery5 = connectedQuery.get(3);
                    try {
                        results2 = connection.getClient().submit(executeQuery2).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        } else {
                            for (Result r : results2) {
                                DetachedVertex t = (DetachedVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        System.out.println("Query 2 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results3 = connection.getClient().submit(executeQuery3).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results3.get(0).toString().split(" ")[1];
                            System.out.println("rclass 3: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                                for (Result r : results3) {
                                    String res = r.getObject().toString();
                                    VIdsTwo.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 3 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results4 = connection.getClient().submit(executeQuery4).all().get();
                        if (results4.size() == 0 || results4 == null) {
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results4.get(0).toString().split(" ")[1];
                            System.out.println("rclass 4: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                                for (Result r : results4) {
                                    String res = r.getObject().toString();
                                    VIdsThree.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 4 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery4 + "\n");
                        result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                         results5 = connection.getClient().submit(executeQuery5).all().get();
                        if (results5.size() == 0 || results5 == null) {
                            queriesList.get(i).add(executeQuery5);
                            out.append("query 5:" + "\n");
                            out.append(executeQuery5 + "\n");
                            System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results5.get(0).toString().split(" ")[1];
                            System.out.println("rclass 5: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                                for (Result r : results5) {
                                    String res = r.getObject().toString();
                                    VIdsFour.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 5 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                        result.append("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    index = x;
                    x1 = VAllIds.get(x);
                    break;
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append("\n");
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 2) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                String x1 = "";
                int variant = 16;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2: Get spouse
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), "").split("&"));
                    executeQuery2 = connectedQuery.get(0);
                    executeQuery3 = connectedQuery.get(1);
                    executeQuery4 = connectedQuery.get(2);
                    executeQuery5 = connectedQuery.get(3);
                    try {
                        results2 = connection.getClient().submit(executeQuery2).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        } else {
                            for (Result r : results2) {
                                ReferenceVertex t = (ReferenceVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        System.out.println("Query 2 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results3 = connection.getClient().submit(executeQuery3).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results3.get(0).toString().split(" ")[1];
                            System.out.println("rclass 3: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                                for (Result r : results3) {
                                    String res = r.getObject().toString();
                                    VIdsTwo.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 3 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results4 = connection.getClient().submit(executeQuery4).all().get();
                        if (results4.size() == 0 || results4 == null) {
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results4.get(0).toString().split(" ")[1];
                            System.out.println("rclass 4: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                                for (Result r : results4) {
                                    String res = r.getObject().toString();
                                    VIdsThree.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 4 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery4 + "\n");
                        result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results5 = connection.getClient().submit(executeQuery5).all().get();
                        if (results5.size() == 0 || results5 == null) {
                            queriesList.get(i).add(executeQuery5);
                            out.append("query 5:" + "\n");
                            out.append(executeQuery5 + "\n");
                            System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results5.get(0).toString().split(" ")[1];
                            System.out.println("rclass 5: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                                for (Result r : results5) {
                                    String res = r.getObject().toString();
                                    VIdsFour.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 5 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                        result.append("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    index = x;
                    x1 = VAllIds.get(x);
                    break;
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append("\n");
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeQuery31(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr31" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr31" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3, hugeResult4, hugeResult5;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                String x1 = "";
                int variant = 17;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        hugeResult1.data().forEach(r -> {
                            VAllIds.add(String.valueOf(r));
                        });
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2: Get descendant
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), "").split("&"));
                    executeQuery2 = connectedQuery.get(0);
                    executeQuery3 = connectedQuery.get(1);
                    executeQuery4 = connectedQuery.get(2);
                    executeQuery5 = connectedQuery.get(3);
                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult2 = hugeGraph.gremlin(executeQuery2).execute();
                        if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        } else {
                            hugeResult2.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsOne.add(String.valueOf(t.get("id")));
                            });
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        System.out.println("Query 2 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult3 = hugeGraph.gremlin(executeQuery3).execute();
                        if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult3.data().get(0);
                            String rclass = (String) tempResult.get("type");
                            System.out.println("rclass 3: " + rclass);
                            if (rclass.equals("vertex")) {
                                hugeResult3.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                    VIdsTwo.add(String.valueOf(t.get("id")));
                                });
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 3 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult4 = hugeGraph.gremlin(executeQuery4).execute();
                        if (hugeResult4.data().size() == 0 || hugeResult4.data() == null) {
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult4.data().get(0);
                            String rclass = (String) tempResult.get("type");
                            System.out.println("rclass 4: " + rclass);
                            if (rclass.equals("vertex")) {
                                hugeResult4.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                    VIdsThree.add(String.valueOf(t.get("id")));
                                });
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 4 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery4 + "\n");
                        result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult5 = hugeGraph.gremlin(executeQuery5).execute();
                        if (hugeResult5.data().size() == 0 || hugeResult5.data() == null) {
                            queriesList.get(i).add(executeQuery5);
                            out.append("query 5:" + "\n");
                            out.append(executeQuery5 + "\n");
                            System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult5.data().get(0);
                            String rclass = (String) tempResult.get("type");
                            System.out.println("rclass 5: " + rclass);
                            if (rclass.equals("vertex")) {
                                hugeResult5.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                    VIdsFour.add(String.valueOf(t.get("id")));
                                });
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 5 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                        result.append("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    index = x;
                    x1 = VAllIds.get(x);
                    break;
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append("\n");
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                String x1 = "";
                int variant = 17;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2: Get descendant
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), "").split("&"));
                    executeQuery2 = connectedQuery.get(0);
                    executeQuery3 = connectedQuery.get(1);
                    executeQuery4 = connectedQuery.get(2);
                    executeQuery5 = connectedQuery.get(3);
                    try {
                        results2 = connection.getClient().submit(executeQuery2).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        } else {
                            for (Result r : results2) {
                                DetachedVertex t = (DetachedVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        System.out.println("Query 2 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results3 = connection.getClient().submit(executeQuery3).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results3.get(0).toString().split(" ")[1];
                            System.out.println("rclass 3: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                                for (Result r : results3) {
                                    String res = r.getObject().toString();
                                    VIdsTwo.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 3 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results4 = connection.getClient().submit(executeQuery4).all().get();
                        if (results4.size() == 0 || results4 == null) {
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results4.get(0).toString().split(" ")[1];
                            System.out.println("rclass 4: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                                for (Result r : results4) {
                                    String res = r.getObject().toString();
                                    VIdsThree.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 4 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery4 + "\n");
                        result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results5 = connection.getClient().submit(executeQuery5).all().get();
                        if (results5.size() == 0 || results5 == null) {
                            queriesList.get(i).add(executeQuery5);
                            out.append("query 5:" + "\n");
                            out.append(executeQuery5 + "\n");
                            System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results5.get(0).toString().split(" ")[1];
                            System.out.println("rclass 5: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                                for (Result r : results5) {
                                    String res = r.getObject().toString();
                                    VIdsFour.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 5 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                        result.append("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    index = x;
                    x1 = VAllIds.get(x);
                    break;
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append("\n");
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 2) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                String x1 = "";
                int variant = 17;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2: Get descendant
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), "").split("&"));
                    executeQuery2 = connectedQuery.get(0);
                    executeQuery3 = connectedQuery.get(1);
                    executeQuery4 = connectedQuery.get(2);
                    executeQuery5 = connectedQuery.get(3);
                    try {
                        results2 = connection.getClient().submit(executeQuery2).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        } else {
                            for (Result r : results2) {
                                ReferenceVertex t = (ReferenceVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        System.out.println("Query 2 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results3 = connection.getClient().submit(executeQuery3).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results3.get(0).toString().split(" ")[1];
                            System.out.println("rclass 3: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                                for (Result r : results3) {
                                    String res = r.getObject().toString();
                                    VIdsTwo.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 3 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results4 = connection.getClient().submit(executeQuery4).all().get();
                        if (results4.size() == 0 || results4 == null) {
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results4.get(0).toString().split(" ")[1];
                            System.out.println("rclass 4: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                                for (Result r : results4) {
                                    String res = r.getObject().toString();
                                    VIdsThree.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 4 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery4 + "\n");
                        result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results5 = connection.getClient().submit(executeQuery5).all().get();
                        if (results5.size() == 0 || results5 == null) {
                            queriesList.get(i).add(executeQuery5);
                            out.append("query 5:" + "\n");
                            out.append(executeQuery5 + "\n");
                            System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results5.get(0).toString().split(" ")[1];
                            System.out.println("rclass 5: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                                for (Result r : results5) {
                                    String res = r.getObject().toString();
                                    VIdsFour.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 5 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                        result.append("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    index = x;
                    x1 = VAllIds.get(x);
                    break;
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append("\n");
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeQuery32(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr32" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr32" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 5) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                String x1 = "";
                int variant = 18;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(r.getString());
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2: Get descendant
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    String k = String.valueOf(Randomly.getInteger(6));
                    connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "'" + VAllIds.get(x) + "'", k).split("&"));
                    executeQuery2 = connectedQuery.get(0);
                    executeQuery3 = connectedQuery.get(1);
                    executeQuery4 = connectedQuery.get(2);
                    executeQuery5 = connectedQuery.get(3);
                    try {
                        results2 = connection.getClient().submit(executeQuery2).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        } else {
                            for (Result r : results2) {
                                DetachedVertex t = (DetachedVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        System.out.println("Query 2 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results3 = connection.getClient().submit(executeQuery3).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results3.get(0).toString().split(" ")[1];
                            System.out.println("rclass 3: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                                for (Result r : results3) {
                                    String tmp = r.getObject().toString().substring(2);
                                    String res = tmp.substring(0, tmp.length() - 1);
                                    VIdsTwo.add(res);
                                }
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 3 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results4 = connection.getClient().submit(executeQuery4).all().get();
                        if (results4.size() == 0 || results4 == null) {
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results4.get(0).toString().split(" ")[1];
                            System.out.println("rclass 4: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                                for (Result r : results4) {
                                    String tmp = r.getObject().toString().substring(2);
                                    String res = tmp.substring(0, tmp.length() - 1);
                                    VIdsThree.add(res);
                                }
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 4 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery4 + "\n");
                        result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results5 = connection.getClient().submit(executeQuery5).all().get();
                        if (results5.size() == 0 || results5 == null) {
                            queriesList.get(i).add(executeQuery5);
                            out.append("query 5:" + "\n");
                            out.append(executeQuery5 + "\n");
                            System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results5.get(0).toString().split(" ")[1];
                            System.out.println("rclass 5: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                                for (Result r : results5) {
                                    String tmp = r.getObject().toString().substring(2);
                                    String res = tmp.substring(0, tmp.length() - 1);
                                    VIdsFour.add(res);
                                }
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 5 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                        result.append("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    index = x;
                    x1 = VAllIds.get(x);
                    break;
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append("\n");
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 4) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                String x1 = "";
                int variant = 18;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(r.getString());
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2: Get descendant
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    String k = String.valueOf(Randomly.getInteger(6));
                    connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, "'" + VAllIds.get(x) + "'", k).split("&"));
                    executeQuery2 = connectedQuery.get(0);
                    executeQuery3 = connectedQuery.get(1);
                    executeQuery4 = connectedQuery.get(2);
                    executeQuery5 = connectedQuery.get(3);
                    try {
                        results2 = connection.getClient().submit(executeQuery2).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        } else {
                            for (Result r : results2) {
                                ReferenceVertex t = (ReferenceVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        System.out.println("Query 2 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results3 = connection.getClient().submit(executeQuery3).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results3.get(0).toString().split(" ")[1];
                            System.out.println("rclass 3: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                                for (Result r : results3) {
                                    String tmp = r.getObject().toString().substring(2);
                                    String res = tmp.substring(0, tmp.length() - 1);
                                    VIdsTwo.add(res);
                                }
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 3 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results4 = connection.getClient().submit(executeQuery4).all().get();
                        if (results4.size() == 0 || results4 == null) {
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results4.get(0).toString().split(" ")[1];
                            System.out.println("rclass 4: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                                for (Result r : results4) {
                                    String tmp = r.getObject().toString().substring(2);
                                    String res = tmp.substring(0, tmp.length() - 1);
                                    VIdsThree.add(res);
                                }
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 4 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery4 + "\n");
                        result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results5 = connection.getClient().submit(executeQuery5).all().get();
                        if (results5.size() == 0 || results5 == null) {
                            queriesList.get(i).add(executeQuery5);
                            out.append("query 5:" + "\n");
                            out.append(executeQuery5 + "\n");
                            System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results5.get(0).toString().split(" ")[1];
                            System.out.println("rclass 5: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                                for (Result r : results5) {
                                    String tmp = r.getObject().toString().substring(2);
                                    String res = tmp.substring(0, tmp.length() - 1);
                                    VIdsFour.add(res);
                                }
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 5 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                        result.append("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    index = x;
                    x1 = VAllIds.get(x);
                    break;
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append("\n");
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 0) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                ResultSet hugeResult1, hugeResult2, hugeResult3, hugeResult4, hugeResult5;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                String x1 = "";
                int variant = 18;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                    hugeResult1 = hugeGraph.gremlin(executeQuery).execute();
                    if (hugeResult1.data().size() == 0 || hugeResult1.data() == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        hugeResult1.data().forEach(r -> {
                            VAllIds.add(String.valueOf(r));
                        });
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2: Get descendant
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    String k = String.valueOf(Randomly.getInteger(6));
                    connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), k).split("&"));
                    executeQuery2 = connectedQuery.get(0);
                    executeQuery3 = connectedQuery.get(1);
                    executeQuery4 = connectedQuery.get(2);
                    executeQuery5 = connectedQuery.get(3);
                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult2 = hugeGraph.gremlin(executeQuery2).execute();
                        if (hugeResult2.data().size() == 0 || hugeResult2.data() == null) {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        } else {
                            hugeResult2.data().forEach(r -> {
                                HashMap<String, Object> t = (HashMap<String, Object>) r;
                                VIdsOne.add(String.valueOf(t.get("id")));
                            });
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        System.out.println("Query 2 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult3 = hugeGraph.gremlin(executeQuery3).execute();
                        if (hugeResult3.data().size() == 0 || hugeResult3.data() == null) {
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult3.data().get(0);
                            String rclass = (String) tempResult.get("type");
                            System.out.println("rclass 3: " + rclass);
                            if (rclass.equals("vertex")) {
                                hugeResult3.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                    VIdsTwo.add(String.valueOf(t.get("id")));
                                });
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 3 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult4 = hugeGraph.gremlin(executeQuery4).execute();
                        if (hugeResult4.data().size() == 0 || hugeResult4.data() == null) {
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult4.data().get(0);
                            String rclass = (String) tempResult.get("type");
                            System.out.println("rclass 4: " + rclass);
                            if (rclass.equals("vertex")) {
                                hugeResult4.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                    VIdsThree.add(String.valueOf(t.get("id")));
                                });
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 4 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery4 + "\n");
                        result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        GremlinManager hugeGraph = connection.getHugespecial().gremlin();
                        hugeResult5 = hugeGraph.gremlin(executeQuery5).execute();
                        if (hugeResult5.data().size() == 0 || hugeResult5.data() == null) {
                            queriesList.get(i).add(executeQuery5);
                            out.append("query 5:" + "\n");
                            out.append(executeQuery5 + "\n");
                            System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            HashMap<String, Object> tempResult = (HashMap<String, Object>) hugeResult5.data().get(0);
                            String rclass = (String) tempResult.get("type");
                            System.out.println("rclass 5: " + rclass);
                            if (rclass.equals("vertex")) {
                                hugeResult5.data().forEach(r -> {
                                    HashMap<String, Object> t = (HashMap<String, Object>) r;
                                    VIdsFour.add(String.valueOf(t.get("id")));
                                });
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 5 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                        result.append("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    index = x;
                    x1 = VAllIds.get(x);
                    break;
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append("\n");
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 1) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                String x1 = "";
                int variant = 18;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2: Get descendant
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    String k = String.valueOf(Randomly.getInteger(6));
                    connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), k).split("&"));
                    executeQuery2 = connectedQuery.get(0);
                    executeQuery3 = connectedQuery.get(1);
                    executeQuery4 = connectedQuery.get(2);
                    executeQuery5 = connectedQuery.get(3);
                    try {
                        results2 = connection.getClient().submit(executeQuery2).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        } else {
                            for (Result r : results2) {
                                DetachedVertex t = (DetachedVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        System.out.println("Query 2 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results3 = connection.getClient().submit(executeQuery3).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results3.get(0).toString().split(" ")[1];
                            System.out.println("rclass 3: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                                for (Result r : results3) {
                                    String res = r.getObject().toString();
                                    VIdsTwo.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 3 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results4 = connection.getClient().submit(executeQuery4).all().get();
                        if (results4.size() == 0 || results4 == null) {
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results4.get(0).toString().split(" ")[1];
                            System.out.println("rclass 4: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                                for (Result r : results4) {
                                    String res = r.getObject().toString();
                                    VIdsThree.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 4 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery4 + "\n");
                        result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results5 = connection.getClient().submit(executeQuery5).all().get();
                        if (results5.size() == 0 || results5 == null) {
                            queriesList.get(i).add(executeQuery5);
                            out.append("query 5:" + "\n");
                            out.append(executeQuery5 + "\n");
                            System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results5.get(0).toString().split(" ")[1];
                            System.out.println("rclass 5: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex}")) {
                                for (Result r : results5) {
                                    String res = r.getObject().toString();
                                    VIdsFour.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 5 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                        result.append("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    index = x;
                    x1 = VAllIds.get(x);
                    break;
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append("\n");
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        } else if (database == 2) {
            int index = 0;
            for (int i = 0; i < state.getQueryNum(); i++) {
                long time = System.currentTimeMillis();
                List<String> connectedQuery = null;
                String executeQuery = "", executeQuery2 = "", executeQuery3 = "", executeQuery4 = "", executeQuery5 = "";
                out.append("========================Query sequence " + i + "=======================\n");
                List<Result> results1, results2, results3, results4, results5;
                List<String> VAllIds = new ArrayList<>();
                List<String> VIdsOne = new ArrayList<>();
                List<String> VIdsTwo = new ArrayList<>();
                List<String> VIdsThree = new ArrayList<>();
                List<String> VIdsFour = new ArrayList<>();
                String x1 = "";
                int variant = 18;

                // Query 1:
                int queryNum = 1;
                executeQuery = generateStrategyMRQuerySequence(queryNum, "node", variant, "", "");
                try {
                    results1 = connection.getClient().submit(executeQuery).all().get();
                    if (results1.size() == 0 || results1 == null) {
                        executeQuery = "";
                        queriesList.get(i).add(executeQuery);
                        continue;
                    } else {
                        for (Result r : results1) {
                            VAllIds.add(String.valueOf(r.getLong()));
                        }
                        queriesList.get(i).add(executeQuery);
                        out.append("query 1:" + "\n");
                        out.append(executeQuery + "\n");
                        System.out.println("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();

                    System.out.println(connection.getDatabase() + " exception : " + executeQuery + "\n");
                    result.append("Query sequence " + i + ", 1: " + executeQuery + " in " + (System.currentTimeMillis() - time) + "ms\n");
                    result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                }

                // Query 2: Get descendant
                queryNum = 2;
                for (int x = index + 1; x < VAllIds.size(); x++) {
                    String k = String.valueOf(Randomly.getInteger(6));
                    connectedQuery = Arrays.asList(generateStrategyMRQuerySequence(queryNum, "node", variant, VAllIds.get(x), k).split("&"));
                    executeQuery2 = connectedQuery.get(0);
                    executeQuery3 = connectedQuery.get(1);
                    executeQuery4 = connectedQuery.get(2);
                    executeQuery5 = connectedQuery.get(3);
                    try {
                        results2 = connection.getClient().submit(executeQuery2).all().get();
                        if (results2.size() == 0 || results2 == null) {
                            executeQuery2 = "";
                            queriesList.get(i).add(executeQuery2);
                            continue;
                        } else {
                            for (Result r : results2) {
                                ReferenceVertex t = (ReferenceVertex) r.getObject();
                                VIdsOne.add(t.id().toString());
                            }
                            queriesList.get(i).add(executeQuery2);
                            out.append("query 2:" + "\n");
                            out.append(executeQuery2 + "\n");
                            System.out.println("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        }
                    } catch (Exception e) {
                        System.out.println("Query 2 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery2 + "\n");
                        result.append("Query sequence " + i + ", 2: " + executeQuery2 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results3 = connection.getClient().submit(executeQuery3).all().get();
                        if (results3.size() == 0 || results3 == null) {
                            queriesList.get(i).add(executeQuery3);
                            out.append("query 3:" + "\n");
                            out.append(executeQuery3 + "\n");
                            System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results3.get(0).toString().split(" ")[1];
                            System.out.println("rclass 3: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                                for (Result r : results3) {
                                    String res = r.getObject().toString();
                                    VIdsTwo.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery3);
                                out.append("query 3:" + "\n");
                                out.append(executeQuery3 + "\n");
                                System.out.println("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 3 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery3 + "\n");
                        result.append("Query sequence " + i + ", 3: " + executeQuery3 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results4 = connection.getClient().submit(executeQuery4).all().get();
                        if (results4.size() == 0 || results4 == null) {
                            queriesList.get(i).add(executeQuery4);
                            out.append("query 4:" + "\n");
                            out.append(executeQuery4 + "\n");
                            System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results4.get(0).toString().split(" ")[1];
                            System.out.println("rclass 4: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                                for (Result r : results4) {
                                    String res = r.getObject().toString();
                                    VIdsThree.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery4);
                                out.append("query 4:" + "\n");
                                out.append(executeQuery4 + "\n");
                                System.out.println("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 4 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery4 + "\n");
                        result.append("Query sequence " + i + ", 4: " + executeQuery4 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    try {
                        results5 = connection.getClient().submit(executeQuery5).all().get();
                        if (results5.size() == 0 || results5 == null) {
                            queriesList.get(i).add(executeQuery5);
                            out.append("query 5:" + "\n");
                            out.append(executeQuery5 + "\n");
                            System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        } else {
                            String rclass = results5.get(0).toString().split(" ")[1];
                            System.out.println("rclass 5: " + rclass);
                            if (rclass.equals("class=org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex}")) {
                                for (Result r : results5) {
                                    String res = r.getObject().toString();
                                    VIdsFour.add(res.replaceAll("[^0-9]", ""));
                                }
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            } else {
                                queriesList.get(i).add(executeQuery5);
                                out.append("query 5:" + "\n");
                                out.append(executeQuery5 + "\n");
                                System.out.println("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Query 5 exception");
                        e.printStackTrace();

                        System.out.println(connection.getDatabase() + " exception : " + executeQuery5 + "\n");
                        result.append("Query sequence " + i + ", 5: " + executeQuery5 + " in " + (System.currentTimeMillis() - time) + "ms\n");
                        result.append(connection.getDatabase() + " exception :\n" + e.toString() + "\n");
                    }

                    index = x;
                    x1 = VAllIds.get(x);
                    break;
                }

                // Check result
                System.out.println("Check result");
                VIdsOne.sort(Comparator.naturalOrder());
                VIdsTwo.sort(Comparator.naturalOrder());
                VIdsThree.sort(Comparator.naturalOrder());
                VIdsFour.sort(Comparator.naturalOrder());
                List<String> union = new ArrayList<>(VIdsTwo);
                union.removeAll(VIdsThree);
                union.addAll(VIdsThree);
                union.removeAll(VIdsFour);
                union.addAll(VIdsFour);
                Collections.sort(union);
                if (!union.equals(VIdsOne)) {
                    result.append("\n");
                    result.append(connection.getDatabase() + " query sequence " + i + ": false\n");
                    result.append("VIdsOne results: " + VIdsOne + "\n");
                    result.append("VIdsTwo results: " + VIdsTwo + "\n");
                    result.append("VIdsThree results: " + VIdsThree + "\n");
                    result.append("VIdsFour results: " + VIdsFour + "\n");
                    result.append("Union results: " + union + "\n");
                    System.out.println("Query sequence " + i + ": false\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                } else {
                    System.out.println("Query sequence " + i + ": true\n");
                    System.out.println("VIdsOne results: " + VIdsOne + "\n");
                    System.out.println("Union results: " + union + "\n");
                }
            }
            out.close();
            result.close();
        }
    }

    public void executeQuery33(GremlinConnection connection, int database) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr33" + "/" + connection.getDatabase() + "-all" + ".log"));
        BufferedWriter result = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr33" + "/" + connection.getDatabase() + "-result" + ".log"));

        if (database == 0) {
            for (int i = 0; i < state.getQueryNum(); i++) {
            }
            out.close();
            result.close();
        } else if (database == 1) {
            for (int i = 0; i < state.getQueryNum(); i++) {
            }
            out.close();
            result.close();
        } else if (database == 2) {
            for (int i = 0; i < state.getQueryNum(); i++) {
            }
            out.close();
            result.close();
        }
    }

    public void setupGraph_Random(List<GraphData.VertexObject> addV, List<GraphData.EdgeObject> addE) throws IOException {
        vertexIDMap = new HashMap<>();
        edgeIDMap = new HashMap<>();
        int count = 0;
        // for each graph db
        for(GremlinConnection connection: connections){
            // setup database
            long start = System.currentTimeMillis();
            setupGraphDatabase(connection, addV, addE);
            System.out.println("setup " + connection.getDatabase() + " in " + (System.currentTimeMillis() - start) +"ms");
            // query database
            start = System.currentTimeMillis();
            executeQuery_Random(connection, count);
            count++;
            System.out.println("query " + connection.getDatabase() + " in " + (System.currentTimeMillis() - start) +"ms");
        }
        // record db map
        recordDBMap();
    }

    /**
     *
     * @param strategyFlag: Random-based or strategy-based test cases generation
     * @param mrFlag: Whether metamorphic relation is enabled
     */
    public void setupGraph(List<GraphData.VertexObject> addV, List<GraphData.EdgeObject> addE, int strategyFlag, int mrFlag) throws IOException {
        // Manual and Random ID map
        vertexIDMap = new HashMap<>();      // <gdb id, manual id>
        edgeIDMap = new HashMap<>();
        int database = 0;
        // for each graph db
        for (GremlinConnection connection : connections) {
            // setup database
            long start = System.currentTimeMillis();
            setupGraphDatabase(connection, addV, addE);
            System.out.println("setup " + connection.getDatabase() + " in " + (System.currentTimeMillis() - start) + "ms");

            // query database
            start = System.currentTimeMillis();
            if (strategyFlag == 0) {
                executeQuery(connection, database, queryList, resultList, errorList, resultNodeOneList, mrFlag);
            } else if (strategyFlag == 1) {
                switch (mrFlag) {
//                    case 0:
//                        executeQuery(connection, database, queryOneList, resultOneList, errorOneList, resultNodeOneList, mrFlag);
//                        break;
                    case 1:
//                        executeMRQuery(connection, database, queryTwoList, resultTwoList, errorTwoList, resultNodeTwoList, mrFlag);
                        executeMRQuery1(connection, database);
                        break;
                    case 2:
                        executeMRQuery2(connection, database);
                        break;
                    case 3:
                        executeMRQuery3(connection, database);
                        break;
                    case 4:
                        executeMRQuery4(connection, database);
                        break;
                    case 5:
                        executeMRQuery5(connection, database);
                        break;
                    case 6:
                        executeMRQuery6(connection, database);
                        break;
                    case 7:
                        executeMRQuery7(connection, database);
                        break;
                    case 8:
                        executeMRQuery8(connection, database);
                        break;
                    case 9:
                        executeMRQuery9(connection, database);
                        break;
                    case 10:
                        executeMRQuery10(connection, database);
                        break;
                    case 11:
                        executeMRQuery11(connection, database);
                        break;
                    case 12:
                        executeMRQuery12(connection, database);
                        break;
                    case 13:
                        executeMRQuery13(connection, database);
                        break;
                    case 14:
                        executeMRQuery14(connection, database);
                        break;
                    case 15:
                        executeMRQuery15(connection, database);
                        break;
                    case 16:
                        executeMRQuery16(connection, database);
                        break;
                    case 17:
                        executeMRQuery17(connection, database);
                        break;
                    case 18:
                    case 19:
                        executeMRQuery18(connection, database);
                        break;
                    case 20:
                        executeMRQuery20(connection, database);
                        break;
                    case 21:
                        executeMRQuery21(connection, database);
                        break;
                    case 22:
                        executeMRQuery22(connection, database);
                        break;
                    case 23:
                        executeMRQuery23(connection, database);
                        break;

                    case 24:
                        executeMRQuery24(connection, database);
                        break;
                    case 25:
                        executeMRQuery25(connection, database);
                        break;
                    case 26:
                        executeQuery26(connection, database);
                        break;
                    case 28:
                        executeQuery28(connection, database);
                        break;
                    case 29:
                        executeQuery29(connection, database);
                        break;
                    case 30:
                        executeQuery30(connection, database);
                        break;
                    case 31:
                        executeQuery31(connection, database);
                        break;
                    case 32:
                        executeQuery32(connection, database);
                        break;
                }
            }

            database++;
            System.out.println("query " + connection.getDatabase() + " in " + (System.currentTimeMillis() - start) + "ms");
        }
        // record db map
        recordDBMap();
    }

    public void setupMRGraph(int strategyFlag, int mrFlag) throws IOException {
        int database = 0;
        // for each graph db
        for (GremlinConnection connection : connections) {
            // query database
            long start = System.currentTimeMillis();
            if (strategyFlag == 0) {
                executeQuery(connection, database, queryList, resultList, errorList, resultNodeOneList, mrFlag);
            } else if (strategyFlag == 1) {
                switch (mrFlag) {
                    case 0:
                        executeQuery(connection, database, queryOneList, resultOneList, errorOneList, resultNodeOneList, mrFlag);
                        break;
                    case 1:
                        executeMRQuery(connection, database, queryTwoList, resultTwoList, errorTwoList, resultNodeTwoList, mrFlag);
                        break;
                }
            }

            database++;
            System.out.println("query " + connection.getDatabase() + " in " + (System.currentTimeMillis() - start) + "ms");
        }
    }

    // Set up graph data into graph database
    public void setupGraphDatabase(GremlinConnection connection, List<GraphData.VertexObject> addV, List<GraphData.EdgeObject> addE) throws IOException {
        String cur = System.getProperty("period");
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log" + "/" + connection.getDatabase() + "-graphdata.txt"));
        BufferedWriter create = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log" + "/" + connection.getDatabase() + "-create.txt"));
        Map<String, String> vIDMap = new HashMap<>();       // <gdb id, manual id>
        Map<Integer, String> tempMap = new HashMap<>();     // <manual id, gdb id>
        Map<String, String> eIDMap = new HashMap<>();

        if (connection.getDatabase().equals("HugeGraph")) {
            HugeClient hc = connection.getHugespecial();
            GraphManager graph = hc.graph();
            Map<String, com.baidu.hugegraph.structure.graph.Vertex> verticesMap = new HashMap<>();
            out.write("Vertex:");
            create.write("Vertex:");
            for (GraphData.VertexObject v : addV) {
                try {
                    String Label = v.getLabel();
                    com.baidu.hugegraph.structure.graph.Vertex add = new com.baidu.hugegraph.structure.graph.Vertex(Label);
                    Map<String, GraphConstant> map = v.getProperites();
                    out.newLine();
                    create.newLine();
                    out.write("Label: " + add.label());
                    out.newLine();
                    out.write("Properties: ");
                    out.newLine();
                    String addVQuery = "";
                    addVQuery = "g.addV('" + Label + "')";
                    for (String key : map.keySet()) {
                        Object value = getTransValue(map.get(key));
                        out.write("Key: " + key + " Value: " + value);
                        out.newLine();
                        add.property(key, value);
                        addVQuery += ".property('" + key + "','" + value + "')";
                    }
                    add = graph.addVertex(add);
                    addVQuery += ".property(T.id," + add.id().toString() + ")";
                    create.write(addVQuery);
                    vIDMap.put(add.id().toString(), String.valueOf(v.getId()));
                    tempMap.put(v.getId(), add.id().toString());
                    out.write("ID: " + add.id().toString());
                    out.newLine();
                    verticesMap.put(add.id().toString(), add);
                } catch (Exception e) {
                    out.write(e.toString());
                    out.newLine();
                    e.printStackTrace();
                    break;
                }
            }
            out.newLine();
            create.newLine();
            out.write("Edge:");
            create.write("Edge:");
            out.newLine();
            create.newLine();
            try {
                for (GraphData.EdgeObject e : addE) {
                    com.baidu.hugegraph.structure.graph.Vertex outVertex = verticesMap.get(tempMap.get(e.getOutVertex().getId()));
                    com.baidu.hugegraph.structure.graph.Vertex inVertex = verticesMap.get(tempMap.get(e.getInVertex().getId()));
                    com.baidu.hugegraph.structure.graph.Edge addEdge = new com.baidu.hugegraph.structure.graph.Edge(e.getLabel()).source(outVertex).target(inVertex);
                    Map<String, GraphConstant> map = e.getProperites();
                    out.write("Label: " + addEdge.label());
                    out.newLine();
                    out.write("Out: " + e.getOutVertex().getId());
                    out.newLine();
                    out.write("In: " + e.getInVertex().getId());
                    out.newLine();
                    out.write("Properties: ");
                    out.newLine();
                    for (String key : map.keySet()) {
                        out.write("Key: " + key + "  Value: " + getTransValue(map.get(key)));
                        out.newLine();
                        addEdge.property(key, getTransValue(map.get(key)));
                    }
                    addEdge = graph.addEdge(addEdge);
                    String inId = tempMap.get(e.getInVertex().getId());
                    String outId = tempMap.get(e.getOutVertex().getId());
                    String addEQuery = "g.V(" + inId + ").as('" + inId + "').V(" + outId + ").as('" + outId + "').addE('" + addEdge.label() + "').from('" + inId + "').to('" + outId + "')";
                    create.write(addEQuery);
                    eIDMap.put(addEdge.id(), String.valueOf(e.getId()));
                    out.write("ID: " + addEdge.id());
                    out.newLine();
                    create.newLine();

                    int inEdgeCount = 0, outEdgeCount = 0;
                    if (state.getInEdgeDegree(outId) == 0) {
                        inEdgeCount = 1;
                        state.setInEdgeDegree(outId, inEdgeCount);
                    } else {
                        inEdgeCount = state.getInEdgeDegree(outId) + 1;
                        state.setInEdgeDegree(outId, inEdgeCount);
                    }
                    if (state.getOutEdgeDegree(inId) == 0) {
                        outEdgeCount = 1;
                        state.setOutEdgeDegree(inId, outEdgeCount);
                    } else {
                        outEdgeCount = state.getOutEdgeDegree(inId) + 1;
                        state.setOutEdgeDegree(inId, outEdgeCount);
                    }
                }
            } catch (Exception e) {
                out.write(e.toString());
                out.newLine();
                e.printStackTrace();
            }
        } else {
            GraphTraversalSource g = connection.getG();
            // reset
            g.E().drop().iterate();
            g.V().drop().iterate();
            // add vertices
            out.write("Vertex:");
            create.write("Vertex:");
            out.newLine();
            create.newLine();
            for (GraphData.VertexObject v : addV) {
                try {
                    Vertex vv = generateVertex(g, v, out, create);
                    vIDMap.put(vv.id().toString(), String.valueOf(v.getId()));      // <gdb id, manual id>
                    tempMap.put(v.getId(), vv.id().toString());                     // <manual id, gdb id>
                } catch (Exception e) {
                    out.write(e.toString());
                    out.newLine();
                    e.printStackTrace();
                    break;
                }
            }
            // add edges
            out.newLine();
            out.write("Edge:");
            out.newLine();
            create.newLine();
            create.write("Edge:");
            create.newLine();
            try {
                for (GraphData.EdgeObject e : addE) {
                    Vertex outVertex = g.V(tempMap.get(e.getOutVertex().getId())).next();
                    Vertex inVertex = g.V(tempMap.get(e.getInVertex().getId())).next();
                    Edge edge = g.addE(e.getLabel()).from(outVertex).to(inVertex).next();
                    String inId = tempMap.get(e.getInVertex().getId());
                    String outId = tempMap.get(e.getOutVertex().getId());
                    String addEQuery = "g.V(" + inId + ").as('" + inId + "').V(" + outId + ").as('" + outId + "').addE('" + e.getLabel() + "').from('" + inId + "').to('" + outId + "')";
                    create.write(addEQuery);
                    out.newLine();
                    out.write("ID: " + edge.id());
                    out.newLine();
                    out.write("Label: " + edge.label());
                    out.newLine();
                    out.write("Out: " + outVertex.id());
                    out.newLine();
                    out.write("In: " + inVertex.id());
                    out.newLine();
                    out.write("Properties: ");
                    out.newLine();
                    Map<String, GraphConstant> map = e.getProperites();
                    for (String key : map.keySet()) {
                        out.write("Key: " + key + "  Value: " + getTransValue(map.get(key)));
                        out.newLine();
                        g.E(edge.id()).property(key, getTransValue(map.get(key))).iterate();
                    }
                    out.newLine();
                    create.newLine();
                    eIDMap.put(edge.id().toString(), String.valueOf(e.getId()));

                    int inEdgeCount = 0, outEdgeCount = 0;
                    if (state.getInEdgeDegree(outId) == 0) {
                        inEdgeCount = 1;
                        state.setInEdgeDegree(outId, inEdgeCount);
                    } else {
                        inEdgeCount = state.getInEdgeDegree(outId) + 1;
                        state.setInEdgeDegree(outId, inEdgeCount);
                    }
                    if (state.getOutEdgeDegree(inId) == 0) {
                        outEdgeCount = 1;
                        state.setOutEdgeDegree(inId, outEdgeCount);
                    } else {
                        outEdgeCount = state.getOutEdgeDegree(inId) + 1;
                        state.setOutEdgeDegree(inId, outEdgeCount);
                    }
                }
            } catch (Exception e) {
                out.write(e.toString());
                out.newLine();
                e.printStackTrace();
            }
        }

        out.close();
        create.close();
        vertexIDMap.put(connection.getDatabase(), vIDMap);
        edgeIDMap.put(connection.getDatabase(), eIDMap);

        if (connection.getDatabase().equals("Neo4j")) {
            createNeo4jGraphIndex(connection);
        } else if (connection.getDatabase().equals("JanusGraph")) {
            createJanusGraphIndex(connection);
        } else if (connection.getDatabase().equals("HugeGraph")) {
            createHugeGraphIndex(connection);
        }
    }

    public void createNeo4jGraphIndex(GremlinConnection connection) {
        for (GraphSchema.GraphVertexIndex index : state.getSchema().getVertexIndices()) {
            String vl = index.getVl().getLabelName();
            List<String> vpList = new ArrayList<>();
            for (GraphSchema.GraphVertexProperty vp : index.getVpList()) {
                vpList.add(vp.getVertexPropertyName());
            }
            StringBuilder vps = new StringBuilder();
            for (String s : vpList) {
                vps.append(s).append(",");
            }
            StringBuilder sb = new StringBuilder();
            String graph = "graph = g.getGraph(); ";
            String cypher = "graph.cypher('CREATE INDEX ON :" + vl + "(" + vps.toString().subSequence(0, vps.length() - 1) + ")');";
            String commit = "graph.tx().commit();";
            try {
                connection.getClient().submit(sb.append(graph).append(cypher).append(commit).toString()).all().get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    public void createJanusGraphIndex(GremlinConnection connection) {
        try {
            // Cluster cluster = Cluster.open("/mnt/g/gdbtesting/target/classes/conf/remote.yaml");
            // Cluster cluster = Cluster.open("/opt/database/gdbtesting/conf/remote.yaml");

            Client client = connection.getClient();
            for (GraphSchema.GraphVertexIndex index : state.getSchema().getVertexIndices()) {
                String vl = index.getVl().getLabelName();
                List<String> vpList = new ArrayList<>();
                for (GraphSchema.GraphVertexProperty vp : index.getVpList()) {
                    vpList.add(vp.getVertexPropertyName());
                }

                StringBuilder sb = new StringBuilder();
                String graph = "graph = g.getGraph(); ";
                String mgmt = "mgmt = graph.openManagement();";
                StringBuilder property = new StringBuilder();
                for (String s : vpList) {
                    property.append(".addKey(mgmt.getPropertyKey('").append(s).append("'))");
                }
                String build = "mgmt.buildIndex('" + index.getIndexName() + "', Vertex.class)" + property.toString() + ".buildCompositeIndex();";
                String commit = "mgmt.commit(); mgmt.awaitGraphIndexStatus(graph, '" + index.getIndexName() + "').call();";
                System.out.println(sb.append(graph).append(mgmt).append(build).append(commit).toString());
                client.submit(sb.toString());

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createHugeGraphIndex(GremlinConnection connection) {
        return;
    }

    public void recordDBMap() throws IOException {
        for (GremlinConnection connection : connections) {
            String cur = System.getProperty("period");
            BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/" + "map" + ".log"));
            Map<String, String> vMap = vertexIDMap.get(connection.getDatabase());
            out.write("Vertex");
            out.newLine();
            for (String id : vMap.keySet()) {
                out.write(vMap.get(id) + "->" + id);
                out.newLine();
            }
            Map<String, String> eMap = edgeIDMap.get(connection.getDatabase());
            out.write("Edge");
            out.newLine();
            for (String id : eMap.keySet()) {
                out.write(eMap.get(id) + "->" + id);
                out.newLine();
            }
            out.close();
        }
    }

    public void checkResult() throws IOException {
        String cur = System.getProperty("period");
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log_orig" + "/check-res" + ".log"));
        BufferedWriter resultOut = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log_orig" + "/result" + ".log"));
        // for each query
        for (int i = 0; i < resultList.size(); i++) {
            List<List<Object>> list = resultList.get(i);
            Map<String, Exception> elist = errorList.get(i);
            List<String> compare = new ArrayList<>();
            out.write("========================Query " + i + "=======================");
            out.newLine();
            // for each graph db
            for (int j = 0; j < list.size(); j++) {
                List<Object> elements = list.get(j);
                Exception errors = elist.get(String.valueOf(j));
                StringBuilder sb = new StringBuilder();
                if (elements == null || elements.size() == 0) {
                    if (errors == null)
                        sb.append("null");
                    else
                        sb.append(errors);
                } else {
                    List<String> idList = new ArrayList<>();
                    String view = elements.get(0).toString();
                    if (elements.get(0).toString().contains("e[")) {
                        for (Object e : elements) {
                            idList.add(edgeIDMap.get(connections.get(j).getDatabase()).get(((Result) e).getElement().id().toString()));
                        }
                        if (idList != null && idList.size() > 0) {
                            Collections.sort(idList);
                        }
                        sb.append("e:").append(idList.toString());
                    } else if (elements.get(0).toString().contains("v[")) {
                        for (Object e : elements) {
                            idList.add(vertexIDMap.get(connections.get(j).getDatabase()).get(((Result) e).getElement().id().toString()));
                        }
                        if (idList != null && idList.size() > 0) {
                            Collections.sort(idList);
                        }
                        sb.append("v:").append(idList.toString());
                    } else if (elements.get(0).toString().contains("java.lang.")) {
                        for (Object e : elements) {
                            String value = e.toString();
                            idList.add(value.substring(value.indexOf("object=") + 7, value.indexOf(" class=")));
                        }
                        if (idList != null && idList.size() > 0) {
                            Collections.sort(idList);
                        }
                        sb.append("n:").append(idList.toString());
                    } else {
                        if (elements.get(0) instanceof com.baidu.hugegraph.structure.graph.Vertex) {
                            for (Object e : elements) {
                                idList.add(vertexIDMap.get(connections.get(j).getDatabase()).get(((com.baidu.hugegraph.structure.graph.Vertex) e).id().toString()));
                            }
                            if (idList != null && idList.size() > 0) {
                                Collections.sort(idList);
                            }
                            sb.append("v:").append(idList.toString());
                        } else if (elements.get(0) instanceof com.baidu.hugegraph.structure.graph.Edge) {
                            for (Object e : elements) {
                                idList.add(edgeIDMap.get(connections.get(j).getDatabase()).get(((com.baidu.hugegraph.structure.graph.Edge) e).id().toString()));
                            }
                            if (idList != null && idList.size() > 0) {
                                Collections.sort(idList);
                            }
                            sb.append("e:").append(idList.toString());
                        } else {
                            for (Object e : elements) {
                                String value = e.toString();
                                idList.add(value);
                            }
                            if (idList != null && idList.size() > 0) {
                                Collections.sort(idList);
                            }
                            sb.append("n:").append(idList.toString());
                        }
                    }
                }
                compare.add(sb.toString());
                out.write("db" + j + ": " + sb.toString());
                out.newLine();
            }
            if (!compareResult(compare)) {
                resultOut.write("query" + i + ": false");
                resultOut.newLine();
            }
        }
        out.close();
        resultOut.close();
    }

    public void checkMRResult() throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr1" + "/check-res" + ".log"));
        BufferedWriter resultOut = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log/mr1" + "/result" + ".log"));

        for (int i = 0; i < successList.size(); i++) {
            List<List<String>> list = successList.get(i);
            if (list.size() == 0 || list == null) {
                continue;
            }

            // for each graph db
            for (int j = 1; j < list.size(); j++) {
                List<String> executeList = new ArrayList<>();

                // Clean data
                List<String> nodesList = list.get(j);
                if (nodesList == null) {
                    continue;
                }
                if (nodesList != null) {
                    for (String nodeList : nodesList) {
                        List<String> regex = new ArrayList<>();
                        List<String> nodes = new ArrayList<>();
                        regex = Arrays.asList(nodeList.split(", "));
                        regex.forEach((e) -> {
                            String node = e.replaceAll("[^0-9]", "");
                            nodes.add(node);
                        });
                        if (executeList.size() == 0) {
                            executeList.add(nodes.get(0));
                        } else {
                            executeList.addAll(nodes.subList(1, nodes.size()));
                        }
                    }
                }

                if (executeList.size() == 0)
                    continue;

                String executeQuery = "";
                String startId = executeList.get(0);
                String endId = "";
                for (int k = 1; k < executeList.size(); k++) {
                    endId = executeList.get(k);
                    executeQuery = generateCheckQuery("path", startId, endId);
                    out.append("========================Query " + i + "=======================\n");
                    out.append("Query 1: " + queryOneList.get(i) + "\n");
//                    out.append("Query 2: " + queryTwoList.get(i) + "\n");
                    out.append("Query 3: " + executeQuery + "\n");
                    System.out.println("Query: " + executeQuery + "\n");

                    // execute query
                    for (GremlinConnection connection : connections) {
                        if (connection.getDatabase().equals("HugeGraph")) {
                            continue;
                        }
                        List<Result> results;
                        // query database
                        long time = System.currentTimeMillis();
                        System.out.println("query " + i + " in " + (System.currentTimeMillis() - time) + "ms");
                        try {
                            results = connection.getClient().submit(executeQuery).all().get();
                            if (results.size() == 0 || results == null) {
                                resultOut.append("========================Query " + i + "=======================\n");
                                resultOut.append(connection.getDatabase() + " false : " + executeQuery + "\n");
                            } else {
                                for (Result r : results) {
                                    if (r.getInt() == 0) {
                                        resultOut.append("========================Query " + i + "=======================\n");
                                        resultOut.append(connection.getDatabase() + " false : " + executeQuery + "\n");
                                    }
                                    String result = String.valueOf(r);
                                    out.append(connection.getDatabase() + ": " + result + "\n");
                                    out.newLine();
                                }
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    public boolean compareResult(List<String> compare) {
        if (compare == null || compare.size() == 0) {
            return true;
        }
        String begin = compare.get(0);
        for (int i = 1; i < compare.size(); i++) {
            if (!compare.get(i).equals(begin)) {
                return false;
            }
        }
        return true;
    }

    public Vertex generateVertex(GraphTraversalSource g, GraphData.VertexObject v, BufferedWriter out, BufferedWriter create) throws IOException {
        Vertex vertex = g.addV(v.getLabel()).next();
        Map<String, GraphConstant> map = v.getProperites();
        out.newLine();
        out.write("ID: " + vertex.id().toString());
        out.newLine();
        out.write("Label: " + v.getLabel());
        out.newLine();
        out.write("Properties: ");
        out.newLine();
        String addVQuery = "";
        addVQuery = "g.addV('" + v.getLabel() + "')";
        for (String key : map.keySet()) {
            Object value = getTransValue(map.get(key));
            out.write("Key: " + key + " Value: " + value);
            out.newLine();
            g.V(vertex).property(key, value).iterate();
            addVQuery += ".property('" + key + "','" + value + "')";
        }
        addVQuery += ".property(T.id," + vertex.id().toString() + ")";
        create.write(addVQuery);
        create.newLine();
        return vertex;
    }

    public Object getTransValue(GraphConstant value) {
        Object type = value.getType();
        if (Integer.class.equals(type)) {
            return Integer.valueOf(value.toString());
        } else if (String.class.equals(type)) {
            return value.toString();
        } else if (Double.class.equals(type)) {
            return Double.valueOf(value.toString());
        } else if (Boolean.class.equals(type)) {
            return Boolean.valueOf(value.toString());
        } else if (Float.class.equals(type)) {
            return Float.valueOf(value.toString());
        } else if (Long.class.equals(type)) {
            return Long.valueOf(value.toString());
        }
        throw new AssertionError();
    }
}

