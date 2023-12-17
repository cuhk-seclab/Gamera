package org.gdbtesting.janusgraph;


import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.gdbtesting.connection.GremlinConnection;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class JanusGraphConnection extends GremlinConnection {

    public JanusGraphConnection(String version, String filename) {
        super(version, "JanusGraph", filename);
    }

    public void connect() {
        try {
            String file = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
            file = file.substring(0, file.lastIndexOf("target") + 6);
            String exactfile = System.getProperty("user.dir") + "/conf/janusgraph.yaml";
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

    public static void main(String[] args) {
        JanusGraphConnection connection = new JanusGraphConnection("0.6.2", "conf/remote-janusgraph.properties");
        GraphTraversalSource g = connection.getG();
        Vertex Ironman = g.addV("Hero").property("name", "Tony").property("ATK", 100.00).next();
        Vertex Superman = g.addV("Hero").property("name", "Clark").property("ATK", Double.POSITIVE_INFINITY).next();
        Vertex Moly = g.addV("student").property("grade", 9).next();
        Vertex notebook = g.addV("homework").property("subject", "Math").next();
        Edge edge1 = g.addE("write").from(Moly).to(notebook).property("date", "0.8").next();

        String query = "g.V().where(__.out('el1').count().is(gt(-5)))";

        try {
            List<Result> results = connection.getClient().submit(query).all().get();
            System.out.println(results.size());
            for (Result r : results) {
                System.out.println(r);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.exit(0);
    }
}
