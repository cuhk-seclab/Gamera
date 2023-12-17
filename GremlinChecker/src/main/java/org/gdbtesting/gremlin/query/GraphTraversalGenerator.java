package org.gdbtesting.gremlin.query;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.gdbtesting.Randomly;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GraphSchema;
import org.gdbtesting.gremlin.GremlinPrint;
import org.gdbtesting.gremlin.gen.GraphExpressionGenerator;
import org.gdbtesting.gremlin.gen.GraphHasFilterGenerator;

import java.util.ArrayList;
import java.util.List;

/**
 * g.V()/E() // begin with this
 * .has(...) // filter some vertices
 * .in()/inV()/... // filter some vertices
 * .where(Predicate) // filter some vertices by generating some predicates with logical operation, i.e., is(), and(), or(), not()...
 * .or/and/not() // filter
 * .values()/properties()... // get the chosen vertices information
 */
public class GraphTraversalGenerator {

    private GraphGlobalState state;

    private GraphTraversalSource g;

    private GremlinPrint print = new GremlinPrint();

    // Support more SQL syntax/grammar
    public enum Traversal {
        FILTER, // has(key, value), has(label, key, value), hasLabel(labels)
        // hasId(ids), hasKey(keys), hasValue(values), has(key), hasNot(key)
        Neighbor,
        Property,
        /*ORDER, // order(), order().by("", asc)
        LOGICAL, // is(), and(), or(), not()
        AGGREGATE, // sum(), max(), min(), mean(), count()
        COMPARISON,
        ITERATOR, // repeat(), times(), until(), emit(), loops(),
        PATH, // path(), simplePath(), cyclicPath()
        TRANSFORM, // map(), flatMap()
        WHERE,
        BRANCH, // choose(), optional()*/
        START, // g.V() / g.E()
    }

    public GraphTraversalGenerator(GraphGlobalState state) {
        this.state = state;
    }

    public String generateRandomlyTraversal() {
        return getExpression();
    }

    public String generateRandomlyNodeTraversal() {
        return getNodeExpression();
    }

    public String generateRandomlyNodeValueTraversal() {
        return getNodeValueExpression();
    }

    public String generateRandomlyEdgeTraversal() {
        return getEdgeExpression();
    }

    public String generateRandomlyEdgeValueTraversal() {
        return getEdgeValueExpression();
    }

    public String getExpression() {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateGraphTraversal();
        return query;
    }

    public String getNodeExpression() {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateGraphNodeTraversal();
        return query;
    }

    public String getNodeValueExpression() {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateGraphNodeValueTraversal();
        return query;
    }

    public String getEdgeExpression() {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateGraphEdgeTraversal();
        return query;
    }

    public String getEdgeValueExpression() {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateGraphEdgeValueTraversal();
        return query;
    }

    public String getKHopNodesExpression(String startId, String k) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateKHopNodesTraversal(startId, k);
        return query;
    }

    public String getKHopNodesReverseExpression(String startId, String k) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateKHopNodesReverseTraversal(startId, k);
        return query;
    }

    public String getVSpouseExpression(String startId) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateVSpouseTraversal(startId);
        return query;
    }

    public String getVDescendantExpression(String startId) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateVDescendantTraversal(startId);
        return query;
    }

    public String getVAncestorExpression(String startId) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateVAncestorTraversal(startId);
        return query;
    }

    public String getVAllPathsExpression(String startId, String endId) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateVAllPathsTraversal(startId, endId);
        return query;
    }

    // Use & to concatenate
    public String getRandomlyNodeHasPartitionExpression() {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateRandomlyNodeHasPartitionTraversal(false);
        return query;
    }

    public String getRandomlyNodeHasPartitionCountExpression() {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateRandomlyNodeHasPartitionTraversal(true);
        return query;
    }

    public String getDropVerticesExpression(String startId) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateDropVerticesTraversal(startId);
        return query;
    }

    public String getRandomlyNodeHasPartitionDedupExpression() {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateRandomlyNodeHasPartitionDedupTraversal(false);
        return query;
    }

    public String getVSpouseHasPartitionExpression(String startId) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateVSpouseHasPartitionTraversal(startId, false);
        return query;
    }

    public String getVDescendantHasPartitionExpression(String startId) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateVDescendantHasPartitionTraversal(startId, false);
        return query;
    }

    public String getKHopNodesHasPartitionExpression(String startId, String k) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateKHopNodesHasPartitionTraversal(startId, k, false);
        return query;
    }

    /**
     * ==========  Start dividing line for oracle strategy ==========
     */
    // Add oracle strategies.
    // General cases
    public String generateStrategyTraversal(String strategy, int variant) {
        String expression = "";
        switch (strategy) {
            case "path":
                expression = getPathExpression(variant);
                break;
            case "node":
//                expression =  getNodeExpression(variant);
                break;
            case "edge":
//                expression = getEdgeExpression(variant);
                break;
        }
        return expression;
    }

    public String getPathExpression(int variant) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        if (variant == 2 || variant == 5) {
            String query = geg.generatePathSizeTraversal();
            return query;
        }
        boolean mr = false;
        String query = geg.generatePathTraversal(mr, "");
        return query;
    }

    public String generateStrategyMRTraversal(String strategy, int variant, String nodeId) {
        String expression = "";
        switch (strategy) {
            case "path":
                expression = getPathMRExpression(variant, nodeId);
                break;
            case "node":
                expression =  getNodeMRExpression(variant, nodeId);
                break;
            case "edge":
                expression = getEdgeMRExpression(variant, nodeId);
                break;
        }
        return expression;
    }

    public String getPathMRExpression(int variant, String nodeId) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        boolean mr = true;
        String query = geg.generatePathTraversal(mr, nodeId);
        return query;
    }

    public String getNodeMRExpression(int variant, String nodeId) {
        return null;
    }

    public String getEdgeMRExpression(int variant, String nodeId) {
        return null;
    }

    public String generateCheckTraversal(String strategy, String startId, String endId) {
        String expression = "";
        switch (strategy) {
            case "path":
                expression = getPathCheckExpression(startId, endId);
                break;
            case "node":
                expression = getNodeCheckExpression(startId, endId);
                break;
            case "edge":
                expression = getEdgeCheckExpression(startId, endId);
                break;
        }
        return expression;
    }

    public String getPathCheckExpression(String startId, String endId) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generatePathCheckTraversal(startId, endId);
        return query;
    }

    public String getPathHopCheckExpression(String id1, String id2, String id3) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generatePathHopCheckTraversal(id1, id2, id3);
        return query;
    }

    public String getOneHopCheckExpression(String startId, String endId) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateOneHopCheckTraversal(startId, endId);
        return query;
    }

    public String getNodeCheckExpression(String startId, String endId) {
        return null;
    }

    public String getEdgeCheckExpression(String startId, String endId) {
        return null;
    }

    // Special cases
    public String getVAllIdsExpression() {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateVAllIdsTraversal();
        return query;
    }

    public String getAddEBetweenVerticesExpression(String startId, String endId, String property) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateAddEBetweenVerticesTraversal(startId, endId, property);
        return query;
    }

    public String getAddMultipleEBetweenVerticesExpression(String startId) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateAddMultipleEBetweenVerticesTraversal(startId);
        return query;
    }

    public String getDropEBetweenVerticesExpression(String startId, String endId) {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateDropEBetweenVerticesTraversal(startId, endId);
        return query;
    }

    public String getAddVerticesExpression() {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateAddVerticesTraversal();
        return query;
    }

    public String getAddMultipleVerticesExpression() {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateAddMultipleVerticesTraversal();
        return query;
    }

    public String getNodeFilterExpression() {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateNodeFilterTraversal();
        return query;
    }

    public String getNodeSizeExpression() {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateNodeSizeTraversal();
        return query;
    }

    public String getEdgeFilterExpression() {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateEdgeFilterTraversal();
        return query;
    }

    public String getEdgeSizeExpression() {
        GraphExpressionGenerator geg = new GraphExpressionGenerator(state);
        geg.setInVertexLabelMap(state.getSchema().getInVertexLabelMap());
        geg.setOutVertexLabelMap(state.getSchema().getOutVertexLabelMap());
        String query = geg.generateEdgeSizeTraversal();
        return query;
    }

    /**
     * ==========  End dividing line for oracle strategy ==========
     */

    public GraphTraversal chooseVertex(GraphTraversal previous) {
        // candidate labels
        List<GraphSchema.GraphVertexLabel> labels = Randomly.nonEmptySubList(state.getSchema().getVertexList());
        System.out.println("choose vertex labels: ");
        for (GraphSchema.GraphVertexLabel label : labels) {
            System.out.println(label.getLabelName());
        }
        List<GraphSchema.GraphVertexProperty> properties = new ArrayList<>();
        System.out.println("choose vertex properties: ");
        for (GraphSchema.GraphVertexLabel label : labels) {
            for (GraphSchema.GraphVertexProperty p : label.getVertexProperties()) {
                if (!properties.contains(p)) {
                    System.out.println(p.getVertexPropertyName());
                    properties.add(p);
                }
            }
        }
        // has filter vertex
        GraphHasFilterGenerator ghfg = new GraphHasFilterGenerator(labels, properties, state);
        return ghfg.getHasFilter(previous);
    }
}
