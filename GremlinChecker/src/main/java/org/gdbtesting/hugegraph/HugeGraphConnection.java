package org.gdbtesting.hugegraph;

import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.GremlinManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.HugeClientBuilder;
import org.gdbtesting.connection.GremlinConnection;

import java.util.Iterator;

public class HugeGraphConnection extends GremlinConnection {

    public HugeClient getHugeClient() {
        return hugeClient;
    }

    private HugeClient hugeClient;

    public void setup() {
        try {
            HugeClientBuilder builder = new HugeClientBuilder("http://localhost:8080", "hugegraph");
            HugeClient hugeClient = new HugeClient(builder);
            GraphManager graph = hugeClient.graph();
            System.out.println(graph.graph());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void connect() {
        try {
            // connect 1
            /*cluster = Cluster.open("/mnt/g/gdbtesting/src/main/resources/conf/hugegraph.yaml");
            client = cluster.connect();
            setClient(client);
            setCluster(cluster);*/

            HugeClient hugeClient = HugeClient.builder("http://127.0.0.1:8080", "hugegraph").build();
            hugespecial = hugeClient;

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public HugeGraphConnection(String version, String filename) {
        super(version, "HugeGraph", filename);
    }

    public static void main(String[] args) {
        HugeGraphConnection connection = new HugeGraphConnection("0.12.0", "conf/hugegraph.yaml");
        HugeClient hugegraph = connection.getHugespecial();
        GremlinManager gremlin = hugegraph.gremlin();

        String query1 = "g.V().or(__.order().by(asc)).has('vp1',0.5755699003757928).inE('el4').outV()";

        System.out.println("query0 : " + query1);
        try {
            com.baidu.hugegraph.structure.gremlin.ResultSet hugeResult = gremlin.gremlin(query1).execute();
            Iterator<com.baidu.hugegraph.structure.gremlin.Result> huresult = hugeResult.iterator();
            huresult.forEachRemaining(result -> {
                Object object = result.getObject();
                System.out.println(object);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}
