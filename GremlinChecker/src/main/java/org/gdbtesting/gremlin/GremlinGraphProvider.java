package org.gdbtesting.gremlin;

import org.gdbtesting.Randomly;
import org.gdbtesting.arangodb.ArangodbConnection;
import org.gdbtesting.arcadedb.ArcadedbConnection;
import org.gdbtesting.common.GDBCommon;
import org.gdbtesting.common.GraphDBProvider;
import org.gdbtesting.connection.GremlinConnection;
import org.gdbtesting.gremlin.gen.GraphAddEdgeAndPropertyGenerator;
import org.gdbtesting.gremlin.gen.GraphAddVertexAndProeprtyGenerator;
import org.gdbtesting.hugegraph.HugeGraphConnection;
import org.gdbtesting.janusgraph.JanusGraphConnection;
import org.gdbtesting.orientdb.OrientdbConnection;
import org.gdbtesting.tinkergraph.TinkerGraphConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class GremlinGraphProvider implements GraphDBProvider<GraphGlobalState, GraphOptions, GremlinConnection> {

    private static final Logger logger = LoggerFactory.getLogger(GremlinGraphProvider.class);

    protected GraphGlobalState state;
    protected GremlinConnection connection;
    private GraphDBExecutor graphDBSetup;

    public boolean containsHuge = true;
//    public boolean containsHuge = false;

    protected String version;
    protected Randomly randomly;

    List<GraphSchema.GraphVertexProperty> vertexProperties = new ArrayList<>();
    List<GraphSchema.GraphVertexIndex> vertexIndices = new ArrayList<>();
    List<GraphSchema.GraphVertexLabel> vertexLabels = new ArrayList<>();
    List<GraphSchema.GraphEdgeProperty> edgeProperties = new ArrayList<>();
    List<GraphSchema.GraphEdgeIndex> edgeIndices = new ArrayList<>();
    List<GraphSchema.GraphRelationship> edgeLabels = new ArrayList<>();
    List<String> indexList = new ArrayList<>();
    List<String> EdgeindexList = new ArrayList<>();

    BufferedWriter schema_out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log" + "/schema-out.txt"));

    public GremlinGraphProvider(GraphGlobalState globalState) throws IOException {
        this.state = globalState;
        this.randomly = globalState.getRandomly();
        this.version = globalState.getDbVersion();
        List<GremlinConnection> connections = Arrays.asList(
                new HugeGraphConnection("0.12.0", "conf/remote-hugegraph.properties"),
                // new ArangodbConnection("3.10.2", "conf/remote-arango.properties"),
//                new ArcadedbConnection("22.12.1","conf/remote-arcade.properties")
                new JanusGraphConnection("0.6.2", "conf/remote-janusgraph.properties"),
//                new OrientdbConnection("3.2.14", "conf/remote-orient.properties")
                new TinkerGraphConnection("3.4.10", "conf/remote-tinkergraph.properties")
        );

        graphDBSetup = new GraphDBExecutor(connections, globalState);
        connection = connections.get(0);
    }

    private enum Action {
        ADD_VERTEX_PROPERTY,
        ADD_EDGE_PROPERTY;
//        ALTER_EDGE_PROPERTY,
//        ALTER_VERTEX_PROPERTY,
//        ADD_VERTEX,
//        ADD_EDGE,
//        DROP_VERTEX,
//        DROP_EDGE;
    }

    private List<GraphData.VertexObject> addVMap;
    private List<GraphData.EdgeObject> addEMap;

    public void addVertexAndProperty(int number) {
        GraphAddVertexAndProeprtyGenerator add = new GraphAddVertexAndProeprtyGenerator(state);
        addVMap = add.addVertices(number);
    }

    public void addEdgeAndProperty(int number) {
        GraphAddEdgeAndPropertyGenerator addE = new GraphAddEdgeAndPropertyGenerator(state);
        addEMap = addE.addEdges(number);
    }

    @Override
    public Class getGlobalStateClass() {
        return null;
    }

    @Override
    public Class getOptionClass() {
        return null;
    }

    @Override
    public void generateAndTestDatabase(GraphGlobalState globalState) throws Exception {
        for (int repeat = 0; repeat < 1; repeat++) {
            createGraphSchema();
            createGraphData();
            generateRandomlyTest();
        }
    }

    @Override
    public void generateAndTestDatabaseWithRules(GraphGlobalState globalState) throws Exception {
        for (int repeat = 0; repeat < 1; repeat++) {
            createGraphSchema();
            createGraphData();

            // Path traversal 1: A -> B yes, B -> C yes -> A -> C yes
//             generateOracleTest();
//            generateOracleTestChoiceOne();                    // NO
            // Path traversal 2: Add edge
//            generateOracleTestWithChoices(2);                 // PART
            // Path traversal 3: Add deprecated node and edge
//            generateOracleTestWithChoices(3);                 // NO
            // Path traversal 4: Multiply counts
            generateOracleTestWithChoices(4);                 // YES, NOT CONFIRMED   ⚡️
            // Path traversal 5: Add node in the edge end
//            generateOracleTestWithChoices(5);                 // NO

            // Node search 1: not
//            generateOracleTestWithChoices(6);                 // NO
            // Node search 2: and
//            generateOracleTestWithChoices(7);                 // NO
            // Node search 3: or
//            generateOracleTestWithChoices(8);                 // NO

            // Edge search 1: not
//            generateOracleTestWithChoices(9);                 // NO
            // Edge search 2: and
//            generateOracleTestWithChoices(10);                // NO
            // Edge search 3: or
//            generateOracleTestWithChoices(11);                // NO

            // Deprecated added
//            generateOracleTestWithChoices(12);
//            generateOracleTestWithChoices(13);

            // Node search
            // Logical expression 1: and, match
//            generateOracleTestWithChoices(14);                // YES
            // Logical expression 2: or, union
//            generateOracleTestWithChoices(15);                // YES

            // Edge search
            // Logical expression 1: and
//            generateOracleTestWithChoices(16);                // YES
            // Logical expression 2: or
//            generateOracleTestWithChoices(17);                // YES

            // Node values
//            generateOracleTestWithChoices(18);                // Not complete
            // Edge values
//            generateOracleTestWithChoices(19);                // Not complete

            // New MR: Complex node pairs relationship
            // 1: Spouse
//            generateOracleTestWithChoices(20);                // NO
            // 2: Ancestral
//            generateOracleTestWithChoices(21);                // NO
            // 3: K-hop
//            generateOracleTestWithChoices(22);                // NO
            // 4: Shortest path
//            generateOracleTestWithChoices(23);                // NO

            // New MR2: Mutating both graph data and queries
            // For elaboration, the bugs triggered by the logical expression are actually from the graph data mutation operations

            // New MR3: MR fusion
            // Node (2) with query partitioning
//            generateOracleTestWithChoices(24);                // YES
            // Node (2) with count
//            generateOracleTestWithChoices(25);                // YES
            // Node (2) with deletion
//            generateOracleTestWithChoices(26);                // NO
            //          with deletion + count
//            generateOracleTestWithChoices(27);
            // Node (2) with addition
//            generateOracleTestWithChoices(28);                // PART, Mainly NO
            //          with addition + count
//            generateOracleTestWithChoices(29);                // NO

            // Node (3) Spouse + query partitioning
//            generateOracleTestWithChoices(30);                // NO
            // Node (3) Ancestral + query partitioning
//            generateOracleTestWithChoices(31);                // NO
            // Node (3) K-hop + query partitioning
//            generateOracleTestWithChoices(32);                // NO


            calculateDegreeDistribution();
        }
    }

    public void generateRandomlyTest(){
        try {
            //create graph with a randomly schema
            System.out.println("generate query");
            graphDBSetup.generateRandomQuery();
            System.out.println("setup Graph");
            graphDBSetup.setupGraph_Random(addVMap, addEMap);
            System.out.println("check result");
            graphDBSetup.checkResult();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void generateRandomlyTest_Backup() {
        try {
            int strategyFlag = 0;       // Disable strategy
            int mrFlag = 0;
            graphDBSetup.initiateGraphDBExecutor(strategyFlag, mrFlag);

            // create graph with a random schema
            System.out.println("generate query");
            graphDBSetup.generateRandomQuery();
            System.out.println("setup Graph");

            graphDBSetup.setupGraph(addVMap, addEMap, strategyFlag, mrFlag);
            System.out.println("check result");
            graphDBSetup.checkResult();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void generateOracleTest() {
        try {
            int strategyFlag = 1;
            int mrFlag = 0;
            graphDBSetup.initiateGraphDBExecutor(strategyFlag, mrFlag);

            System.out.println("Generate strategy query");
            graphDBSetup.generateStrategyQuery("path");

            System.out.println("Execute strategy query");
            graphDBSetup.setupGraph(addVMap, addEMap, strategyFlag, mrFlag);

            System.out.println("Generate strategy query with metamorphic relations");
            mrFlag = 1;
            graphDBSetup.initiateGraphDBExecutor(strategyFlag, mrFlag);
            graphDBSetup.setupMRGraph(strategyFlag, mrFlag);

            System.out.println("Check result");
            graphDBSetup.checkMRResult();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void generateOracleTestChoiceOne() {
        try {
            int strategyFlag = 1;
            int mrFlag = 0;
            graphDBSetup.initiateGraphDBExecutor(strategyFlag, mrFlag);
            mrFlag = 1;
            graphDBSetup.initiateGraphDBExecutor(strategyFlag, mrFlag);
            System.out.println("Generate and execute queries. Then check results.");
            graphDBSetup.setupGraph(addVMap, addEMap, strategyFlag, mrFlag);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void generateOracleTestWithChoices(int mrFlag) {
        try {
            int strategyFlag = 1;
            graphDBSetup.initiateGraphDBExecutor2();

            System.out.println("Generate and execute queries. Then check results.");
            graphDBSetup.setupGraph(addVMap, addEMap, strategyFlag, mrFlag);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Vertex schema:
     *      ID
     *      Label: vlx
     *      Properties: (Key, Value) / (vpx, type)
     * Edge schema:
     *      ID
     *      Label: elx
     *      Out: vlx, Properties: (vpx, type)
     *      In: vlx, Properties: (vpx, type)
     *      Properties: (Key, Value) / (epx, type)
     */
    public void createGraphSchema() throws IOException {
        System.out.println("createGraphSchema");
        // Vertex Property
        createVertexProperty();

        // Vertex Label
        createVertexLabel();

        // Edge Property
        createEdgeProperty();

        // Edge index
        createEdgeIndex();

        // Edge Label
        createEdgeLabel();

        // Initial GraphSchema
        GraphSchema newSchema =
                new GraphSchema(vertexLabels, edgeLabels, vertexProperties, vertexIndices, edgeProperties, edgeIndices, indexList, EdgeindexList);
        BufferedWriter out = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/log" + "/schema.txt"));
        // this.getConnection().getHugespecial().graph().addVertices(newSchema.vertexIndices);
        out.write(newSchema.toString());
        out.close();
//        System.out.println(newSchema.toString());
        state.setSchema(newSchema);

        schema_out.close();
    }

    public void createGraphData() {
        System.out.println("creatGraphData");
        GraphData graphData = new GraphData();
        state.setGraphData(graphData);
        executeActions();
        graphData.setEpValuesMap();
        graphData.setVpValuesMap();
    }

    public void executeActions() {
        Action[] actions = Action.values();
        int number = 0;
        for (Action a : actions) {
            number = mapActions(a);
            if (number != 0) {
                executeAction(a, number);
            }
        }
    }

    private int mapActions(Action a) {
        Randomly r = state.getRandomly();
        int number = 0;
        switch (a) {
            case ADD_VERTEX_PROPERTY:
                number = (int) state.getVerticesMaxNum();
                break;
            case ADD_EDGE_PROPERTY:
                number = (int) state.getEdgesMaxNum();
                break;
            default:
                throw new AssertionError(a);
        }
        return number;
    }

    public void executeAction(Action a, int number) {
        switch (a) {
            case ADD_VERTEX_PROPERTY:
                addVertexAndProperty(number);
                state.getGraphData().setVertices(addVMap);
                state.getGraphData().updateVertices();
                break;
            case ADD_EDGE_PROPERTY:
                addEdgeAndProperty(number);
                state.getGraphData().setEdges(addEMap);
                state.getGraphData().updateEdges();
                break;
            /*case ALTER_VERTEX_PROPERTY:
                alterVertexProperty(number);
                break;
            case ALTER_EDGE_PROPERTY:
                alterEdgeProperty(number);
                break;
            case DROP_EDGE:
                drop(number, "edge");
                break;
            case DROP_VERTEX:
                drop(number, "vertex");
                break;*/

            default:
                throw new AssertionError(a);
        }
    }

    public void createVertexProperty() throws IOException {
        for (int i = 0; i < randomly.getInteger(state.getPropertyMaxNum()); i++) {
            // for each property
            String propertyName = GDBCommon.createVertexPropertyName(i);
            ConstantType type = Randomly.fromOptions(ConstantType.getRandom());
            whetherHuge(propertyName, type);
            GraphSchema.GraphVertexProperty vertexProperty = new GraphSchema.GraphVertexProperty(propertyName, type, null);
            state.getVertexPropertyIndex().addAndGet(1);
            vertexProperties.add(vertexProperty);
        }
    }

    public void createVertexLabel() throws IOException {
        for (int i = 0; i < randomly.getInteger(state.getVertexLabelNum()); i++) {
            // for each label
            String labelName = GDBCommon.createVertexLabelName(i);
            // generate randomly properties
            List<GraphSchema.GraphVertexProperty> list = new ArrayList<>();
            if (this.getConnection().getHugespecial() != null) {
                this.getConnection().getHugespecial().schema().vertexLabel(labelName).ifNotExist().create();
                schema_out.write("schema().vertexLabel('" + labelName + "').ifNotExist().create();");
                schema_out.newLine();
            }
            int random = (int) randomly.getInteger(vertexProperties.size());
            for (int j = 0; j < random; j++) {
                GraphSchema.GraphVertexProperty gvp =
                        vertexProperties.get((int) randomly.getInteger(vertexProperties.size()) - 1);
                if (!list.contains(gvp)) {
                    list.add(gvp);
                    if (containsHuge) {
                        this.getConnection().getHugespecial().schema().vertexLabel(labelName).properties(gvp.getVertexPropertyName()).nullableKeys(gvp.getVertexPropertyName()).append();
                        schema_out.write("schema().vertexLabel('" + labelName + "').properties('" + gvp.getVertexPropertyName() + "').nullableKeys('" + gvp.getVertexPropertyName() + "').append();");
                        schema_out.newLine();
                        // index property
                        String indexname = labelName + "by" + gvp.getVertexPropertyName() + "Shard";
                        indexList.add(indexname);
                        this.getConnection().getHugespecial().schema().indexLabel(indexname).onV(labelName).by(gvp.getVertexPropertyName()).shard().ifNotExist().create();
                        schema_out.write("schema().indexLabel('" + indexname + "').onV('" + labelName + "').by('" + gvp.getVertexPropertyName() + "').shard().ifNotExist().create();");
                        schema_out.newLine();
                    }
                }
            }
            GraphSchema.GraphVertexLabel vertexLabel =
                    new GraphSchema.GraphVertexLabel(labelName, list, null);
            // generate randomly index
            GraphSchema.GraphVertexIndex index = GraphSchema.GraphVertexIndex.create(vertexLabel, Randomly.nonEmptySubList(list));
            vertexIndices.add(index);
            vertexLabel.setIndexes(index);
            state.getVertexLabelIndex().addAndGet(1);
            vertexLabels.add(vertexLabel);
        }
    }

    public void createEdgeProperty() throws IOException {
        for (int i = 0; i < randomly.getInteger(state.getPropertyMaxNum()); i++) {
            // for each property
            String propertyName = GDBCommon.createEdgePropertyName(i);
            ConstantType type = Randomly.fromOptions(ConstantType.values());
            GraphSchema.GraphEdgeProperty edgeProperty =
                    new GraphSchema.GraphEdgeProperty(propertyName, type, null);
            whetherHuge(propertyName, type);
            state.getEdgePropertyIndex().addAndGet(1);
            edgeProperties.add(edgeProperty);
        }
    }

    public void createEdgeIndex() {
        List<GraphSchema.GraphEdgeProperty> subEL = Randomly.nonEmptySubList(edgeProperties);
        for (int i = 0; i < subEL.size(); i++) {
            edgeIndices.add(GraphSchema.GraphEdgeIndex.create(subEL.get(i).getEdgePropertyName()));
        }
    }

    public void createEdgeLabel() throws IOException {
        for (int i = 0; i < randomly.getInteger(state.getEdgeLabelNum()); i++) {
            // for each label
            String labelName = GDBCommon.createEdgeLabelName(i);
            ArrayList<GraphSchema.GraphEdgeProperty> list = new ArrayList<>();
            // generate randomly index
            GraphSchema.GraphRelationship edgeLabel = new GraphSchema.GraphRelationship(labelName,
                    vertexLabels.get((int) randomly.getInteger(vertexLabels.size()) - 1),
                    vertexLabels.get((int) randomly.getInteger(vertexLabels.size()) - 1),
                    list, Randomly.nonEmptySubList(edgeIndices));
            state.getEdgeLabelIndex().addAndGet(1);
            edgeLabels.add(edgeLabel);
            if (containsHuge) {
                this.getConnection().getHugespecial().schema().edgeLabel(labelName).link(edgeLabel.getOutLabel().getLabelName(), edgeLabel.getInLabel().getLabelName()).ifNotExist().create();
                schema_out.write("schema().edgeLabel('" + labelName + "').link('" + edgeLabel.getOutLabel().getLabelName() + "', '" + edgeLabel.getInLabel().getLabelName() + "').ifNotExist().create();");
                schema_out.newLine();
            }
            // generate randomly properties
            int random = (int) randomly.getInteger(edgeProperties.size());
            for (int j = 0; j < random; j++) {
                GraphSchema.GraphEdgeProperty gep =
                        edgeProperties.get((int) randomly.getInteger(edgeProperties.size()) - 1);
                if (!list.contains(gep)) {
                    list.add(gep);
                    if (containsHuge) {
                        this.getConnection().getHugespecial().schema().edgeLabel(labelName).properties(gep.getEdgePropertyName()).nullableKeys(gep.getEdgePropertyName()).append();
                        schema_out.write("schema().edgeLabel('" + labelName + "').properties('" + gep.getEdgePropertyName() + "').nullableKeys('" + gep.getEdgePropertyName() + "').append();");
                        schema_out.newLine();
                    }
                    // index edge property
                    String indexname = labelName + "by" + gep.getEdgePropertyName() + "Shard";
                    EdgeindexList.add(indexname);
                    if (containsHuge) {
                        this.getConnection().getHugespecial().schema().indexLabel(indexname).onE(labelName).by(gep.getEdgePropertyName()).shard().ifNotExist().create();
                        schema_out.write("schema().indexLabel('" + indexname + "').onE('" + labelName + "').by('" + gep.getEdgePropertyName() + "').shard().ifNotExist().create();");
                        schema_out.newLine();
                    }
                }
            }
        }
    }

    public void whetherHuge(String propertyName, ConstantType type) throws IOException {
        if (containsHuge)
            switch (type) {
                case INTEGER:
                    this.getConnection().getHugespecial().schema().propertyKey(propertyName).asInt().ifNotExist().create();
                    schema_out.write("schema().propertyKey('" + propertyName + "').asInt().ifNotExist().create();");
                    schema_out.newLine();
                    break;
                case STRING:
                    this.getConnection().getHugespecial().schema().propertyKey(propertyName).asText().ifNotExist().create();
                    schema_out.write("schema().propertyKey('" + propertyName + "').asText().ifNotExist().create();");
                    schema_out.newLine();
                    break;
                case DOUBLE:
                    this.getConnection().getHugespecial().schema().propertyKey(propertyName).asDouble().ifNotExist().create();
                    schema_out.write("schema().propertyKey('" + propertyName + "').asDouble().ifNotExist().create();");
                    schema_out.newLine();
                    break;
                case BOOLEAN:
                    this.getConnection().getHugespecial().schema().propertyKey(propertyName).asBoolean().ifNotExist().create();
                    schema_out.write("schema().propertyKey('" + propertyName + "').asBoolean().ifNotExist().create();");
                    schema_out.newLine();
                    break;
                case FLOAT:
                    this.getConnection().getHugespecial().schema().propertyKey(propertyName).asFloat().ifNotExist().create();
                    schema_out.write("schema().propertyKey('" + propertyName + "').asFloat().ifNotExist().create();");
                    schema_out.newLine();
                    break;
                case LONG:
                    this.getConnection().getHugespecial().schema().propertyKey(propertyName).asLong().ifNotExist().create();
                    schema_out.write("schema().propertyKey('" + propertyName + "').asLong().ifNotExist().create();");
                    schema_out.newLine();
                    break;
            }
    }

    public GremlinConnection getConnection() {
        return connection;
    }

    public void calculateDegreeDistribution() {
        // Calculate graph degree complexity
        int[] inDegree = new int[200];
        int[] outDegree = new int[200];
        Arrays.fill(inDegree, 0);
        Arrays.fill(outDegree, 0);
        Double inAll = Double.valueOf((int) state.inEdgeDegree.values().stream().count());
        Double outAll = Double.valueOf((int) state.outEdgeDegree.values().stream().count());
        int maxIn = -1, maxOut = -1;
        for (Object in : state.inEdgeDegree.values()) {
            inDegree[Integer.valueOf(in.toString()) -1 ] += 1;
            if (maxIn < Integer.valueOf(in.toString())) {
                maxIn = Integer.valueOf(in.toString());
            }
        }
        for (Object out : state.outEdgeDegree.values()) {
            outDegree[Integer.valueOf(out.toString()) - 1] += 1;
            if (maxOut < Integer.valueOf(out.toString())) {
                maxOut = Integer.valueOf(out.toString());
            }
        }
        Double inExpectedEVal = 0.0, outExpectedVal = 0.0;
        for (int i = 0; i < 200; i++) {
            inExpectedEVal += i * inDegree[i] / inAll;
            outExpectedVal += i * outDegree[i] / outAll;
        }
        System.out.println("Graph in degree expected value: " + inExpectedEVal);
        System.out.println("Graph out degree expected value: " + outExpectedVal);
        System.out.println("Graph in max degree: " + maxIn);
        System.out.println("Graph out max degree: " + maxOut);
    }
}
