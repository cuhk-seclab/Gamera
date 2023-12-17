package org.gdbtesting.gremlin;

import org.gdbtesting.GraphDB;
import org.gdbtesting.Randomly;
import org.gdbtesting.connection.GremlinConnection;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Used for database connection and run.sh configuration
 */
public class GraphGlobalState<C extends GremlinConnection> {

    private final int generateDepth;
    private GraphDB dbType;
    private String dbVersion;
    private GraphSchema schema;
    private static Randomly r;
    private C connection;

    private int vertexLabelNum = 20;
    private int edgeLabelNum = 20;
    private int propertyMaxNum = 20;
    private long verticesMaxNum = 100;
    private long edgesMaxNum = 100;

    private long stepMaxNum = 100;
    private int repeatTimes = 1;

    private long queryNum = 100;

    public HashMap<String, Integer> inEdgeDegree = new HashMap<>();

    public HashMap<String, Integer> outEdgeDegree = new HashMap<>();

    public GraphGlobalState(int generateDepth) {
        this.generateDepth = generateDepth;
    }

    public int getGenerateDepth() {
        return generateDepth;
    }

    public GraphDB getDbType() {
        return dbType;
    }

    public void setDbType(GraphDB dbType) {
        this.dbType = dbType;
    }

    public String getDbVersion() {
        return dbVersion;
    }

    public void setDbVersion(String dbVersion) {
        this.dbVersion = dbVersion;
    }

    public GraphSchema getSchema() {
        if (schema == null) {
            try {
                updateSchema();
            } catch (Exception e) {
                throw new AssertionError();
            }
        }
        return schema;
    }

    public void setSchema(GraphSchema schema) {
        this.schema = schema;
    }

    public void updateSchema() throws Exception {
        setSchema(readSchema());
    }

    // TODO: Update the schema rules
    public GraphSchema readSchema() {
        return null;
    }

    public static Randomly getRandomly() {
        return r;
    }

    public void setRandomly(Randomly r) {
        this.r = r;
    }

    public C getConnection() {
        return connection;
    }

    public void setConnection(C connection) {
        this.connection = connection;
    }

    public int getVertexLabelNum() {
        return vertexLabelNum;
    }

    public void setVertexLabelNum(int vertexLabelNum) {
        this.vertexLabelNum = vertexLabelNum;
    }

    public int getEdgeLabelNum() {
        return edgeLabelNum;
    }

    public void setEdgeLabelNum(int edgeLabelNum) {
        this.edgeLabelNum = edgeLabelNum;
    }

    public int getPropertyMaxNum() {
        return propertyMaxNum;
    }

    public void setPropertyMaxNum(int propertyMaxNum) {
        this.propertyMaxNum = propertyMaxNum;
    }

    public long getVerticesMaxNum() {
        return verticesMaxNum;
    }

    public void setVerticesMaxNum(long verticesMaxNum) {
        this.verticesMaxNum = verticesMaxNum;
    }

    public long getEdgesMaxNum() {
        return edgesMaxNum;
    }

    public void setEdgesMaxNum(long edgesMaxNum) {
        this.edgesMaxNum = edgesMaxNum;
    }

    public long getStepMaxNum() {
        return stepMaxNum;
    }

    public void setStepMaxNum(long stepMaxNum) {
        this.stepMaxNum = stepMaxNum;
    }

    public int getRepeatTimes() {
        return this.repeatTimes;
    }

    public void setRepeatTimes(int repeatTimes) {
        this.repeatTimes = repeatTimes;
    }

    public long getQueryNum() {
        return queryNum;
    }

    public void setQueryNum(long queryNum) {
        this.queryNum = queryNum;
    }

    // Other properties
    // TODO: remove remoteFile
    protected String remoteFile;

    public String getRemoteFile() {
        return remoteFile;
    }

    public void setRemoteFile(String remoteFile) {
        this.remoteFile = remoteFile;
    }

    public static volatile AtomicLong vertexLabelIndex = new AtomicLong(1);
    public static volatile AtomicLong vertexPropertyIndex = new AtomicLong(1);
    public static volatile AtomicLong edgeLabelIndex = new AtomicLong(1);
    public static volatile AtomicLong edgePropertyIndex = new AtomicLong(1);

    public static AtomicLong getVertexLabelIndex() {
        return vertexLabelIndex;
    }

    public static AtomicLong getVertexPropertyIndex() {
        return vertexPropertyIndex;
    }

    public static AtomicLong getEdgeLabelIndex() {
        return edgeLabelIndex;
    }

    public static AtomicLong getEdgePropertyIndex() {
        return edgePropertyIndex;
    }

    // Import GraphData
    private GraphData graphData;

    public GraphData getGraphData() {
        return graphData;
    }

    public void setGraphData(GraphData graphData) {
        this.graphData = graphData;
    }

    public int getInEdgeDegree(String nodeId) {
        if (!inEdgeDegree.containsKey(nodeId)) {
            inEdgeDegree.put(nodeId, 0);
            return 0;
        }
        return inEdgeDegree.get(nodeId);
    }

    public void setInEdgeDegree(String nodeId, int inEdgeCount) {
        inEdgeDegree.replace(nodeId, inEdgeCount);
    }

    public int getOutEdgeDegree(String nodeId) {
        if (!outEdgeDegree.containsKey(nodeId)) {
            outEdgeDegree.put(nodeId, 0);
            return 0;
        }
        return outEdgeDegree.get(nodeId);
    }

    public void setOutEdgeDegree(String nodeId, int outEdgeCount) {
        outEdgeDegree.replace(nodeId, outEdgeCount);
    }

}
