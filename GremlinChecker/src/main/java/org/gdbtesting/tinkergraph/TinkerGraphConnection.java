package org.gdbtesting.tinkergraph;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.gdbtesting.connection.GremlinConnection;

import java.util.List;

import static java.lang.Double.POSITIVE_INFINITY;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TinkerGraphConnection extends GremlinConnection {

    private TinkerGraph graph;

    public TinkerGraphConnection(String version) {
        super(version, "TinkerGraph");
    }

    public TinkerGraphConnection(String version, String filename) {
        super(version, "TinkerGraph", filename);
    }

    public void connect() {
        try {
            String file = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
            file = file.substring(0, file.lastIndexOf("target") + 6);
            String exactfile = System.getProperty("user.dir") + "/conf/tinkergraph.yaml";
            cluster = Cluster.open(exactfile);
            client = cluster.connect();
            setClient(client);
            setCluster(cluster);

            g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));
            setG(g);
            setGraph(g.getGraph());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public TinkerGraph getGraph() {
        return graph;
    }

    public static void main(String[] args) {
        TinkerGraphConnection connection = new TinkerGraphConnection("");
        GraphTraversalSource g = connection.getG();
        g.E().drop().iterate();
        g.V().drop().iterate();

        Vertex bob = g.addV("person").property("name", "Bob").next();
        Vertex alex = g.addV("person").property("age", 0.29027268300579956).next();
        Vertex john = g.addV("person").property("age", 2.94858941E8).next();
        Vertex alice = g.addV("person").property("age", POSITIVE_INFINITY).next();
        Vertex book = g.addV("person").property("name", "book1").next();
        g.V(alex).property("name", "Alex").iterate();

        Edge edge1 = g.addE("knows").from(bob).to(alice).property("d", 0.9, "C", 55).next();
        Edge edge2 = g.addE("write").from(alice).to(book).property("d", 0.94461).next();
        // g.E(edge2.id()).property("d", 0.94461).iterate();
        // System.out.println(POSITIVE_INFINITY);
        String query = "g.V().has('age',0.29027268300579956)";
        // g.V().has('vp1', inside(0.24070676216155018,4.13998472E8)).inE('el0','el4').outV().not(__.values('vp1'))

        try {
            List<Result> results = connection.getClient().submit(query).all().get();
            System.out.println(results.size());
            for (Result r : results) {
                System.out.println(r.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.exit(0);
    }
}
