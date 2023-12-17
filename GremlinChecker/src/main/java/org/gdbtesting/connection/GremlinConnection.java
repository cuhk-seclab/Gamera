package org.gdbtesting.connection;

import com.baidu.hugegraph.driver.HugeClient;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.gdbtesting.GraphDBConnection;

/**
 * Connect database and set basic configuration about Graph etc.
 */
public class GremlinConnection implements GraphDBConnection {

    protected Client client;
    protected GraphTraversalSource g;
    protected Cluster cluster;
    protected String version;
    protected String database;
    // protected RequestOptions options;
    protected Graph graph;
    protected HugeClient hugespecial;
    private String filename;

    public GremlinConnection(String version, String database, String filename) {
        this.version = version;
        this.database = database;
        this.filename = filename;
        hugespecial = null;
        connect();
    }

    public GremlinConnection(String version, String database) {
        this.version = version;
        this.database = database;
        hugespecial = null;
        connect();
    }

    public void connect() {
        System.out.println("skip");
    }

    public void close() {
        try {
            g.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Client getClient() {
        return client;
    }

    public void setClient(Client client) {
        this.client = client;
    }

    public GraphTraversalSource getG() {
        return g;
    }

    public void setG(GraphTraversalSource g) {
        this.g = g;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public String getVersion() {
        return version;
    }

    public String getDatabase() {
        return database;
    }

    public HugeClient getHugespecial() {
        return hugespecial;
    }

    public Graph getGraph() {
        return graph;
    }

    public void setGraph(Graph graph) {
        this.graph = graph;
    }

    public String getFilename() {
        return filename;
    }

    @Override
    public String getDatabaseVersion() throws Exception {
        return getDatabase() + "::" + getVersion();
    }
}
