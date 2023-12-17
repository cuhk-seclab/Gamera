package org.gdbtesting.arangodb;

import com.arangodb.tinkerpop.gremlin.utils.ArangoDBConfigurationBuilder;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.gdbtesting.connection.GremlinConnection;

import java.util.List;

import static java.lang.Double.POSITIVE_INFINITY;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class ArangodbConnection extends GremlinConnection {

    public ArangodbConnection(String version, String filename) {
        super(version, "Arangodb", filename);
    }

    public void connect() {
        try {
            String file = this.getClass().getClassLoader().getResource("conf/arango.yaml").getPath();
            cluster = Cluster.open(file);
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
        ArangodbConnection connection = new ArangodbConnection("3.10.2", "conf/remote-arango.properties");
//        ArangoDBConfigurationBuilder builder = new ArangoDBConfigurationBuilder();
//        builder.arangoHosts("127.0.0.1:8529");
//        BaseConfiguration conf = builder.build();
//        Graph graph = GraphFactory.open(conf);
//        GraphTraversalSource g = new GraphTraversalSource(graph);
        GraphTraversalSource g = connection.getG();
        g.E().drop().iterate();
        g.V().drop().iterate();

        Vertex bob = g.addV("person").property("name", "Bob").next();
        Vertex alex = g.addV("person").property("age", 0.29027268300579956).next();
        Vertex alice = g.addV("person").property("age", POSITIVE_INFINITY).next();
        Vertex book = g.addV("person").property("name", "book1").next();
        g.V(alex).property("name", "Alex").iterate();

        Edge edge1 = g.addE("knows").from(bob).to(alice).property("d", 0.9, "C", 55).next();
        Edge edge2 = g.addE("write").from(alice).to(book).property("d", 0.94461).next();
        String query = "g.V().has('age',0.29027268300579956)";

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
