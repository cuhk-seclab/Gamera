package org.gdbtesting.gremlin.gen;

import org.gdbtesting.Randomly;
import org.gdbtesting.common.gen.UntypedExpressionGenerator;
import org.gdbtesting.gremlin.ConstantType;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GraphSchema;
import org.gdbtesting.gremlin.ast.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GraphExpressionGenerator extends UntypedExpressionGenerator<GraphExpression, GraphSchema.GraphVertexProperty> {

    private final GraphGlobalState state;
    private GraphSchema.GraphVertexProperty vertexProperties;
    private GraphSchema.GraphEdgeProperty edgeProperties;

    private StringBuilder expression = new StringBuilder("g");

    // Record the out Vertex Label
    protected Map<String, List<GraphSchema.GraphVertexLabel>> outVertexLabelMap;
    // Record the in Vertex Label
    protected Map<String, List<GraphSchema.GraphVertexLabel>> inVertexLabelMap;

    /*  private GraphTraversalSource g;

        public GraphTraversalSource getG() {
            return g;
    }   */

    public GraphGlobalState getState() {
        return state;
    }

    public GraphExpressionGenerator(GraphGlobalState state) {
        this.state = state;
//        this.g = state.getConnection().getG();
    }

    private enum Actions {
        VERTEX_PROPERTY, /*EDGE_PROPERTY*/ LITERAL, BINARY_COMPARISON, BINARY_LOGICAL/*, UNARY_PREFIX*/;
    }

    private enum GraphTraversal {
        /*PROPERTY,*/ FILTER_TRAVERSAL, NEIGHBOR_TRAVERSAL/*, STATISTIC*/, ORDER;
    }

    private enum GraphFilterTraversal {
        PROPERTY, FILTER_TRAVERSAL, NEIGHBOR_TRAVERSAL, STATISTIC, ORDER;
    }

    List<Traversal> traversalList = new ArrayList<>();

    public String generateGraphTraversal() {
//        System.out.println("generate depth : " + state.getGenerateDepth());
        int length = Randomly.getInteger(2, state.getGenerateDepth());      // generateDepth = 5
        for (int i = 0; i < length; i++) {
            Traversal t = null;
            while (t == null) {
                t = generateExpressionTraversal(i);
            }
            traversalList.add(t);
            String Type = t.getTraversalType();
            if (Type.contains("property") || Type.contains("statistic")) {
                if (Type.contains("property") && Randomly.getBoolean()) {
                    Traversal ta = createStatistic(t);
                    traversalList.add(ta);
                }
                break;
            }
        }
        StringBuilder s = new StringBuilder("g");
        for (Traversal t : traversalList) {
            s.append(".").append(t.toString());
        }
        return s.toString();
    }

    public String generateGraphNodeTraversal() {
        int length = Randomly.getInteger(2, state.getGenerateDepth());
        for (int i = 0; i < length; i++) {
            Traversal t = null;
            while (t == null) {
                t = generateNodeExpressionTraversal(i, length);
            }
            traversalList.add(t);
//            String Type = t.getTraversalType();
//            if (Type.contains("property") || Type.contains("statistic")) {
//                if (Type.contains("property") && Randomly.getBoolean()) {
//                    Traversal ta = createStatistic(t);
//                    traversalList.add(ta);
//                }
//                break;
//            }
        }
        StringBuilder s = new StringBuilder("g");
        for (Traversal t : traversalList) {
            s.append(".").append(t.toString());
        }
        return s.toString();
    }

    public String generateGraphNodeValueTraversal() {
        int length = Randomly.getInteger(2, state.getGenerateDepth());
        for (int i = 0; i < length; i++) {
            Traversal t = null;
            while (t == null) {
                t = generateNodeValueExpressionTraversal(i, length);
            }
            traversalList.add(t);
        }
        StringBuilder s = new StringBuilder("g");
        for (Traversal t : traversalList) {
            s.append(".").append(t.toString());
        }
        return s.toString();
    }

    public String generateGraphEdgeTraversal() {
        int length = Randomly.getInteger(2, state.getGenerateDepth());
        for (int i = 0; i < length; i++) {
            Traversal t = null;
            while (t == null) {
                t = generateEdgeExpressionTraversal(i, length);
            }
            traversalList.add(t);
        }
        StringBuilder s = new StringBuilder("g");
        for (Traversal t : traversalList) {
            s.append(".").append(t.toString());
        }
        return s.toString();
    }

    public String generateGraphEdgeValueTraversal() {
        int length = Randomly.getInteger(2, state.getGenerateDepth());
        for (int i = 0; i < length; i++) {
            Traversal t = null;
            while (t == null) {
                t = generateEdgeValueExpressionTraversal(i, length);
            }
            traversalList.add(t);
        }
        StringBuilder s = new StringBuilder("g");
        for (Traversal t : traversalList) {
            s.append(".").append(t.toString());
        }
        return s.toString();
    }

    public Traversal generateExpressionTraversal(int depth) {
        if (depth == 0) {
            return createStartTraversal();
        }
        if (depth >= state.getGenerateDepth()) {
            return (Traversal) generateLeafNode();
        }
        int x = Randomly.getInteger(0, 100);
        if (0 <= x && x < 50) {
            return createFilterTraversalOperation(traversalList.get(depth - 1));
        } else if (51 <= x && x < 85) {
            return createNeighborTraversalOperation(traversalList.get(depth - 1));
        } else if (86 <= x && x < 98) {
            return createPropertyOrConstant(traversalList.get(depth - 1));
        } else {
            return createOrder(traversalList.get(depth - 1));
        }
    }

    public Traversal generateNodeExpressionTraversal(int depth, int maxDepth) {
        if (depth == 0) {
            return createStartNodeTraversal();
        }
        if (depth >= state.getGenerateDepth()) {
            return (Traversal) generateLeafNode();
        }
        int x = Randomly.getInteger(0, 100);
        if (depth == maxDepth - 1) {
            if (0 <= x && x < 60) {
                return createFilterTraversalOperation(traversalList.get(depth - 1));
            } else if (61 <= x && x < 95) {
                return createNeighborTraversalOperation(traversalList.get(depth - 1));
            } else {
                return createOrder(traversalList.get(depth - 1));
            }
        } else {
            if (0 <= x && x < 50) {
                return createFilterTraversalOperation(traversalList.get(depth - 1));
            } else if (51 <= x && x < 85) {
                return createNeighborTraversalOperation(traversalList.get(depth - 1));
            } else if (86 <= x && x < 98) {
                return createPropertyOrConstant(traversalList.get(depth - 1));
            } else {
                return createOrder(traversalList.get(depth - 1));
            }
        }
    }

    public Traversal generateNodeDedupExpressionTraversal(int depth, int maxDepth) {
        if (depth == 0) {
            return createStartNodeTraversal();
        }
        if (depth >= state.getGenerateDepth()) {
            return (Traversal) generateLeafNode();
        }
        int x = Randomly.getInteger(0, 100);
        if (depth == maxDepth - 1) {
            if (0 <= x && x < 90) {
                return createFilterTraversalOperation(traversalList.get(depth - 1));
            } else {
                return createOrder(traversalList.get(depth - 1));
            }
        } else {
            if (0 <= x && x < 70) {
                return createFilterTraversalOperation(traversalList.get(depth - 1));
            } else if (71 <= x && x < 95) {
                return createPropertyOrConstant(traversalList.get(depth - 1));
            } else {
                return createOrder(traversalList.get(depth - 1));
            }
        }
    }

    public Traversal generateNodeValueExpressionTraversal(int depth, int maxDepth) {
        if (depth == 0) {
            return createStartNodeTraversal();
        }
        if (depth >= state.getGenerateDepth()) {
            return (Traversal) generateLeafNode();
        }
        int x = Randomly.getInteger(0, 100);
        if (depth == maxDepth - 1) {
            return createProperties(traversalList.get(depth - 1));
        } else {
            if (0 <= x && x < 50) {
                return createFilterTraversalOperation(traversalList.get(depth - 1));
            } else if (51 <= x && x < 85) {
                return createNeighborTraversalOperation(traversalList.get(depth - 1));
            } else if (86 <= x && x < 98) {
                return createPropertyOrConstant(traversalList.get(depth - 1));
            } else {
                return createOrder(traversalList.get(depth - 1));
            }
        }
    }

    public Traversal generateEdgeExpressionTraversal(int depth, int maxDepth) {
        if (depth == 0) {
            return createStartEdgeTraversal();
        }
        if (depth >= state.getGenerateDepth()) {
            return (Traversal) generateLeafNode();
        }
        int x = Randomly.getInteger(0, 100);
        if (depth == maxDepth - 1) {
            if (0 <= x && x < 60) {
                return createFilterTraversalOperation(traversalList.get(depth - 1));
            } else if (61 <= x && x < 95) {
                return createNeighborTraversalOperation(traversalList.get(depth - 1));
            } else {
                return createOrder(traversalList.get(depth - 1));
            }
        } else {
            if (0 <= x && x < 50) {
                return createFilterTraversalOperation(traversalList.get(depth - 1));
            } else if (51 <= x && x < 85) {
                return createNeighborTraversalOperation(traversalList.get(depth - 1));
            } else if (86 <= x && x < 98) {
                return createPropertyOrConstant(traversalList.get(depth - 1));
            } else {
                return createOrder(traversalList.get(depth - 1));
            }
        }
    }

    public Traversal generateEdgeValueExpressionTraversal(int depth, int maxDepth) {
        if (depth == 0) {
            return createStartEdgeTraversal();
        }
        if (depth >= state.getGenerateDepth()) {
            return (Traversal) generateLeafNode();
        }
        int x = Randomly.getInteger(0, 100);
        if (depth == maxDepth - 1) {
            return createProperties(traversalList.get(depth - 1));
        } else {
            if (0 <= x && x < 50) {
                return createFilterTraversalOperation(traversalList.get(depth - 1));
            } else if (51 <= x && x < 85) {
                return createNeighborTraversalOperation(traversalList.get(depth - 1));
            } else if (86 <= x && x < 98) {
                return createPropertyOrConstant(traversalList.get(depth - 1));
            } else {
                return createOrder(traversalList.get(depth - 1));
            }
        }
    }

    /**
     * ==========  Start dividing line for path strategy ==========
     */

    // Uniformed traversal
    // 1
    public String generatePathTraversal(boolean mr, String nodeId) {
        Traversal t = null;
        while (t == null) {
            if (mr == false) {
                t = createStartPathTraversal();
            } else if (mr == true) {
                t = createStartPathTraversalWithId(nodeId);
            }
        }
        traversalList.add(t);

        // Randomly add filter traversal operation
        if (Randomly.getBoolean()) {
            Traversal t_filter = null;
            t_filter = createFilterTraversalOperation(traversalList.get(traversalList.size() - 1));
            traversalList.add(t_filter);
        }

        // Add path traversal operation
        int pathLength;
        if (mr) {
            pathLength = Randomly.getInteger(1, 2);
        } else {
            pathLength = Randomly.getInteger(1, 3);
        // pathLength = Randomly.getInteger(1, state.getGenerateDepth());
        }
        for (int i = 0; i < pathLength; i++) {
            Traversal t_path = null;
            while (t_path == null) {
                t_path = createPathTraversalOperation(traversalList.get(traversalList.size() - 1), mr);
            }
            traversalList.add(t_path);
        }

        // Add path operation
        Traversal t_key = null;
        t_key = createPathOperation(traversalList.get(traversalList.size() - 1));
        traversalList.add(t_key);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generatePathCheckTraversal(String startId, String endId) {
        Traversal t = null;
        while (t == null) {
            t = createStartPathTraversalWithId(startId);
        }
        traversalList.add(t);

        // Traversal path check with startId and endId
        Traversal t_check = null;
        while (t_check == null) {
            t_check = createPathCheckOperation(traversalList.get(traversalList.size() - 1), endId);
        }
        traversalList.add(t_check);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateOneHopCheckTraversal(String startId, String endId) {
        Traversal t = null;
        while (t == null) {
            t = createOneHopCheckOperation(startId, endId);
        }
        traversalList.add(t);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    // 2
    public String generateVAllIdsTraversal() {
        Traversal t = null;
        while (t == null) {
            t = createVAllIdsOperation();
        }
        traversalList.add(t);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateAddEBetweenVerticesTraversal(String startId, String endId, String label) {
        Traversal t = null;
        while (t == null) {
            t = createAddEBetweenVerticesOperation(startId, endId, label);
        }
        traversalList.add(t);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateAddMultipleEBetweenVerticesTraversal(String startId) {
        String retS = "";
        List<String> vertexIds = Arrays.asList(startId.split("&"));
        for (int i = 0; i < 50; i++) {
            String id1 = vertexIds.get((int) Randomly.getInteger(vertexIds.size()) - 1);
            String id2 = vertexIds.get((int) Randomly.getInteger(vertexIds.size()) - 1);
            String label = "el" + String.valueOf(Randomly.getInteger(50, 1000));
            Traversal t = createAddEBetweenVerticesOperation(id1, id2, label);
            StringBuilder s = new StringBuilder("g");
            s.append(".").append(t.toString());
            if (i != 49)
                retS += s.toString() + "&";
            else
                retS += s.toString();
        }
        return retS;
    }

    public String generateDropEBetweenVerticesTraversal(String startId, String endId) {
        Traversal t = null;
        while (t == null) {
            t = createDropEBetweenVerticesOperation(startId, endId);
        }
        traversalList.add(t);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    // 3
    public String generatePathSizeTraversal() {
        Traversal t = null;
        while (t == null) {
            t = createStartPathTraversalWithId(getRandomVertexIds());
        }
        traversalList.add(t);

        // Randomly add filter traversal operation
        if (Randomly.getBoolean()) {
            Traversal t_filter = null;
            t_filter = createFilterTraversalOperation(traversalList.get(traversalList.size() - 1));
            traversalList.add(t_filter);
        }

        // Add path traversal operation
        int pathLength = Randomly.getInteger(1, 3);
        for (int i = 0; i < pathLength; i++) {
            Traversal t_path = null;
            while (t_path == null) {
                t_path = createPathTraversalOperation(traversalList.get(traversalList.size() - 1), false);
            }
            traversalList.add(t_path);
        }

        // Add path size operation
        Traversal t_key = null;
        t_key = createPathSizeOperation(traversalList.get(traversalList.size() - 1));
        traversalList.add(t_key);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateAddVerticesTraversal() {
        Traversal t = null;
        while (t == null) {
            t = createAddVerticesOperation();
        }
        traversalList.add(t);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateAddMultipleVerticesTraversal() {
        String retS = "";
        for(int i = 0; i < 50; i++) {
            int rand = Randomly.getInteger(50, 1000);
            String vLabel = "vl" + String.valueOf(rand + i);
            String vProperty = "vp" + String.valueOf(rand + i);
            String vValue = "vp" + String.valueOf(rand + i) + "val";
            Traversal t = createAddVerticesOperation(vLabel, vProperty, vValue);
            StringBuilder s = new StringBuilder("g");
            s.append(".").append(t.toString());
            if (i != 49)
                retS += s.toString() + "&";
            else
                retS += s.toString();
        }

        return retS;
    }

    // 4
    public String generatePathHopCheckTraversal(String id1, String id2, String id3) {
        Traversal t = null;
        while (t == null) {
            t = createPathHopCheckOperation(id1, id2, id3);
        }
        traversalList.add(t);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateNodeFilterTraversal() {
        Traversal t = null;
        while (t == null) {
            t = createStartNodeTraversal();
        }
        traversalList.add(t);

        // Add node filter operation
        Traversal t_filter = null;
        while (t_filter == null) {
            t_filter = createFilterOperation(traversalList.get(traversalList.size() - 1));
        }
        traversalList.add(t_filter);

        Traversal t_size = null;
        while (t_size == null) {
            t_size = createSizeOperation();
        }
        traversalList.add(t_size);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateNodeSizeTraversal() {
        Traversal t = null;
        while (t == null) {
            t = createStartNodeTraversal();
        }
        traversalList.add(t);

        Traversal t_size = null;
        while (t_size == null) {
            t_size = createSizeOperation();
        }
        traversalList.add(t_size);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateEdgeSizeTraversal() {
        Traversal t = null;
        while (t == null) {
            t = createStartEdgeTraversal();
        }
        traversalList.add(t);

        Traversal t_size = null;
        while (t_size == null) {
            t_size = createSizeOperation();
        }
        traversalList.add(t_size);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateEdgeFilterTraversal() {
        Traversal t = null;
        while (t == null) {
            t = createStartEdgeTraversal();
        }
        traversalList.add(t);

        // Add node filter operation
        Traversal t_filter = null;
        while (t_filter == null) {
            t_filter = createFilterOperation(traversalList.get(traversalList.size() - 1));
        }
        traversalList.add(t_filter);

        Traversal t_size = null;
        while (t_size == null) {
            t_size = createSizeOperation();
        }
        traversalList.add(t_size);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateKHopNodesTraversal(String startId, String k) {
        Traversal t = null;
        while (t == null) {
            t = createKHopNodesOperation(startId, k);
        }
        traversalList.add(t);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateKHopNodesReverseTraversal(String startId, String k) {
        Traversal t = null;
        while (t == null) {
            t = createKHopNodesReverseOperation(startId, k);
        }
        traversalList.add(t);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateVSpouseTraversal(String startId) {
        Traversal t = null;
        while (t == null) {
            t = createVSpouseOperation(startId);
        }
        traversalList.add(t);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateVDescendantTraversal(String startId) {
        Traversal t = null;
        while (t == null) {
            t = createVDescendantOperation(startId);
        }
        traversalList.add(t);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateVAncestorTraversal(String startId) {
        Traversal t = null;
        while (t == null) {
            t = createVAncestorOperation(startId);
        }
        traversalList.add(t);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateVAllPathsTraversal(String startId, String endId) {
        Traversal t = null;
        while (t == null) {
            t = createVAllPathsOperation(startId, endId);
        }
        traversalList.add(t);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    public String generateRandomlyNodeHasPartitionTraversal(boolean isCount) {
        int length = Randomly.getInteger(1, state.getGenerateDepth());
        for (int i = 0; i < length; i++) {
            Traversal t = null;
            while (t == null) {
                t = generateNodeExpressionTraversal(i, length);
            }
            traversalList.add(t);
        }

        Traversal t_hasPredicate = null;
        Traversal t_hasNegatedPredicate = null;
        Traversal t_hasNotPredicate = null;

        List<Traversal> t_list = createHasTraversalOperation(traversalList.get(traversalList.size() - 1));
        t_hasPredicate = t_list.get(0);
        t_hasNegatedPredicate = t_list.get(1);
        t_hasNotPredicate = t_list.get(2);

        StringBuilder s = new StringBuilder("g");
        StringBuilder s_hasPredicate = new StringBuilder("g");
        StringBuilder s_hasNegatedPredicate = new StringBuilder("g");
        StringBuilder s_hasNotPredicate = new StringBuilder("g");
        for (Traversal t : traversalList) {
            s.append(".").append(t.toString());
            s_hasPredicate.append(".").append(t.toString());
            s_hasNegatedPredicate.append(".").append(t.toString());
            s_hasNotPredicate.append(".").append(t.toString());
        }
        s_hasPredicate.append(".").append(t_hasPredicate.toString());
        s_hasNegatedPredicate.append(".").append(t_hasNegatedPredicate.toString());
        s_hasNotPredicate.append(".").append(t_hasNotPredicate.toString());

        if (isCount) {
            Traversal t_count = createCountOperation();
            s.append(".").append(t_count.toString());
            s_hasPredicate.append(".").append(t_count.toString());
            s_hasNegatedPredicate.append(".").append(t_count.toString());
            s_hasNotPredicate.append(".").append(t_count.toString());
        }

        return s.toString() + "&" + s_hasPredicate.toString() + "&" + s_hasNegatedPredicate.toString() + "&" + s_hasNotPredicate.toString();
    }

    public String generateRandomlyNodeHasPartitionDedupTraversal(boolean isCount) {
        int length = Randomly.getInteger(1, state.getGenerateDepth());
        for (int i = 0; i < length; i++) {
            Traversal t = null;
            while (t == null) {
                t = generateNodeDedupExpressionTraversal(i, length);
            }
            traversalList.add(t);
        }
        Traversal t_dedup = createDedupTraversalOperation();
        traversalList.add(t_dedup);

        Traversal t_hasPredicate = null;
        Traversal t_hasNegatedPredicate = null;
        Traversal t_hasNotPredicate = null;

        List<Traversal> t_list = createHasTraversalOperation(traversalList.get(traversalList.size() - 1));
        t_hasPredicate = t_list.get(0);
        t_hasNegatedPredicate = t_list.get(1);
        t_hasNotPredicate = t_list.get(2);

        StringBuilder s = new StringBuilder("g");
        StringBuilder s_hasPredicate = new StringBuilder("g");
        StringBuilder s_hasNegatedPredicate = new StringBuilder("g");
        StringBuilder s_hasNotPredicate = new StringBuilder("g");
        for (Traversal t : traversalList) {
            s.append(".").append(t.toString());
            s_hasPredicate.append(".").append(t.toString());
            s_hasNegatedPredicate.append(".").append(t.toString());
            s_hasNotPredicate.append(".").append(t.toString());
        }
        s_hasPredicate.append(".").append(t_hasPredicate.toString());
        s_hasNegatedPredicate.append(".").append(t_hasNegatedPredicate.toString());
        s_hasNotPredicate.append(".").append(t_hasNotPredicate.toString());

        if (isCount) {
            Traversal t_count = createCountOperation();
            s.append(".").append(t_count.toString());
            s_hasPredicate.append(".").append(t_count.toString());
            s_hasNegatedPredicate.append(".").append(t_count.toString());
            s_hasNotPredicate.append(".").append(t_count.toString());
        }

        return s.toString() + "&" + s_hasPredicate.toString() + "&" + s_hasNegatedPredicate.toString() + "&" + s_hasNotPredicate.toString();
    }

    public String generateVSpouseHasPartitionTraversal(String startId, boolean isCount) {
        Traversal t_spouse = null;
        while (t_spouse == null) {
            t_spouse = createVSpouseNoDedupOperation(startId);
        }
        traversalList.add(t_spouse);

        Traversal t_hasPredicate = null;
        Traversal t_hasNegatedPredicate = null;
        Traversal t_hasNotPredicate = null;

        List<Traversal> t_list = createHasTraversalOperation(traversalList.get(traversalList.size() - 1));
        t_hasPredicate = t_list.get(0);
        t_hasNegatedPredicate = t_list.get(1);
        t_hasNotPredicate = t_list.get(2);

        StringBuilder s = new StringBuilder("g");
        StringBuilder s_hasPredicate = new StringBuilder("g");
        StringBuilder s_hasNegatedPredicate = new StringBuilder("g");
        StringBuilder s_hasNotPredicate = new StringBuilder("g");
        for (Traversal t : traversalList) {
            s.append(".").append(t.toString());
            s_hasPredicate.append(".").append(t.toString());
            s_hasNegatedPredicate.append(".").append(t.toString());
            s_hasNotPredicate.append(".").append(t.toString());
        }
        s_hasPredicate.append(".").append(t_hasPredicate.toString());
        s_hasNegatedPredicate.append(".").append(t_hasNegatedPredicate.toString());
        s_hasNotPredicate.append(".").append(t_hasNotPredicate.toString());

        if (isCount) {
            Traversal t_count = createCountOperation();
            s.append(".").append(t_count.toString());
            s_hasPredicate.append(".").append(t_count.toString());
            s_hasNegatedPredicate.append(".").append(t_count.toString());
            s_hasNotPredicate.append(".").append(t_count.toString());
        }

        return s.toString() + "&" + s_hasPredicate.toString() + "&" + s_hasNegatedPredicate.toString() + "&" + s_hasNotPredicate.toString();
    }

    public String generateVDescendantHasPartitionTraversal(String startId, boolean isCount) {
        Traversal t_descendant = null;
        while (t_descendant == null) {
            t_descendant = createVDescendantDedupOperation(startId);
        }
        traversalList.add(t_descendant);

        Traversal t_hasPredicate = null;
        Traversal t_hasNegatedPredicate = null;
        Traversal t_hasNotPredicate = null;

        List<Traversal> t_list = createHasTraversalOperation(traversalList.get(traversalList.size() - 1));
        t_hasPredicate = t_list.get(0);
        t_hasNegatedPredicate = t_list.get(1);
        t_hasNotPredicate = t_list.get(2);

        StringBuilder s = new StringBuilder("g");
        StringBuilder s_hasPredicate = new StringBuilder("g");
        StringBuilder s_hasNegatedPredicate = new StringBuilder("g");
        StringBuilder s_hasNotPredicate = new StringBuilder("g");
        for (Traversal t : traversalList) {
            s.append(".").append(t.toString());
            s_hasPredicate.append(".").append(t.toString());
            s_hasNegatedPredicate.append(".").append(t.toString());
            s_hasNotPredicate.append(".").append(t.toString());
        }
        s_hasPredicate.append(".").append(t_hasPredicate.toString());
        s_hasNegatedPredicate.append(".").append(t_hasNegatedPredicate.toString());
        s_hasNotPredicate.append(".").append(t_hasNotPredicate.toString());

        if (isCount) {
            Traversal t_count = createCountOperation();
            s.append(".").append(t_count.toString());
            s_hasPredicate.append(".").append(t_count.toString());
            s_hasNegatedPredicate.append(".").append(t_count.toString());
            s_hasNotPredicate.append(".").append(t_count.toString());
        }

        return s.toString() + "&" + s_hasPredicate.toString() + "&" + s_hasNegatedPredicate.toString() + "&" + s_hasNotPredicate.toString();
    }

    public String generateKHopNodesHasPartitionTraversal(String startId, String k, boolean isCount) {
        Traversal t_khop = null;
        while (t_khop == null) {
            t_khop = createKHopNodesOperation(startId, k);
        }
        traversalList.add(t_khop);

        Traversal t_hasPredicate = null;
        Traversal t_hasNegatedPredicate = null;
        Traversal t_hasNotPredicate = null;

        List<Traversal> t_list = createHasTraversalOperation(traversalList.get(traversalList.size() - 1));
        t_hasPredicate = t_list.get(0);
        t_hasNegatedPredicate = t_list.get(1);
        t_hasNotPredicate = t_list.get(2);

        StringBuilder s = new StringBuilder("g");
        StringBuilder s_hasPredicate = new StringBuilder("g");
        StringBuilder s_hasNegatedPredicate = new StringBuilder("g");
        StringBuilder s_hasNotPredicate = new StringBuilder("g");
        for (Traversal t : traversalList) {
            s.append(".").append(t.toString());
            s_hasPredicate.append(".").append(t.toString());
            s_hasNegatedPredicate.append(".").append(t.toString());
            s_hasNotPredicate.append(".").append(t.toString());
        }
        s_hasPredicate.append(".").append(t_hasPredicate.toString());
        s_hasNegatedPredicate.append(".").append(t_hasNegatedPredicate.toString());
        s_hasNotPredicate.append(".").append(t_hasNotPredicate.toString());

        if (isCount) {
            Traversal t_count = createCountOperation();
            s.append(".").append(t_count.toString());
            s_hasPredicate.append(".").append(t_count.toString());
            s_hasNegatedPredicate.append(".").append(t_count.toString());
            s_hasNotPredicate.append(".").append(t_count.toString());
        }

        return s.toString() + "&" + s_hasPredicate.toString() + "&" + s_hasNegatedPredicate.toString() + "&" + s_hasNotPredicate.toString();
    }

    public String generateDropVerticesTraversal(String startId) {
        Traversal t = null;
        while (t == null) {
            t = createDropVerticesOperation(startId);
        }
        traversalList.add(t);

        StringBuilder s = new StringBuilder("g");
        for (Traversal t_add : traversalList) {
            s.append(".").append(t_add.toString());
        }
        return s.toString();
    }

    // Unit traversal
    public StartTraversalOperation createStartNodeTraversal() {
        StartTraversalOperation.V v = StartTraversalOperation.createV();
        v.setTraversalType("V");
        v.setStartStep("vertex");
        v.setEndStep("vertex");
        return v;
    }

    public StartTraversalOperation createStartEdgeTraversal() {
        StartTraversalOperation.E e = StartTraversalOperation.createE();
        e.setTraversalType("E");
        e.setStartStep("edge");
        e.setEndStep("edge");
        return e;
    }

    public StartTraversalOperation createStartPathTraversal() {
        switch (StartTraversalOperation.getRandomStartPathTraversal()) {
            case V:
                StartTraversalOperation.V v = StartTraversalOperation.createV();
                v.setTraversalType("V");
                v.setStartStep("vertex");
                v.setEndStep("vertex");
                return v;
            case V_ids:
                StartTraversalOperation.VWithIds vWithIds = StartTraversalOperation.createVWithIds(getRandomVertexIds());
                vWithIds.setStartStep("vertex");
                vWithIds.setEndStep("vertex");
                return vWithIds;
            default:
                throw new AssertionError();
        }
    }

    public StartTraversalOperation createStartPathTraversalWithId(String id) {
        List<String> vertexIds = Arrays.asList(id);
        StartTraversalOperation.VWithIds vWithIds = StartTraversalOperation.createVWithIds(vertexIds);
        vWithIds.setStartStep("vertex");
        vWithIds.setEndStep("vertex");
        return vWithIds;
    }

    public StartTraversalOperation createStartPathTraversalWithId(List<String> vertexIds) {
        StartTraversalOperation.VWithIds vWithIds = StartTraversalOperation.createVWithIds(vertexIds);
        vWithIds.setStartStep("vertex");
        vWithIds.setEndStep("vertex");
        return vWithIds;
    }

    public NeighborTraversalOperation createPathTraversalOperation(Traversal startTraversal, boolean mr) {
        String type = startTraversal.getEndStep();
        if (!type.equals("vertex")) {
            return null;
        }
        return createPathVertexNeighbor(type, mr);
    }

    public NeighborTraversalOperation createPathVertexNeighbor(String type, boolean mr) {
        switch (Randomly.fromOptions(NeighborTraversalOperation.NeighborVPath.values())) {
            case out:
                NeighborTraversalOperation.Out out = null;
                out = NeighborTraversalOperation.createOut(null);

                if (mr == false) {
                    if (Randomly.getBoolean()) {
                        out = NeighborTraversalOperation.createOut(getEdgeLabelList());
                    } else {
                        out = NeighborTraversalOperation.createOut(null);
                    }
                } else if (mr) {
                    out = NeighborTraversalOperation.createOut(null);
                }

                out.setStartStep(type);
                out.setEndStep("vertex");
                out.setTraversalType("neighbor");
                return out;
//            case in:
//                NeighborTraversalOperation.In in = NeighborTraversalOperation.createIn(getEdgeLabelList());
//                in.setStartStep(type);
//                in.setEndStep("vertex");
//                in.setTraversalType("neighbor");
//                return in;
//            case both:
//                NeighborTraversalOperation.Both both = NeighborTraversalOperation.createBoth(getEdgeLabelList());
//                both.setStartStep(type);
//                both.setEndStep("vertex");
//                both.setTraversalType("neighbor");
//                return both;
            default:
                throw new AssertionError();
        }
    }

    public PathOperation createPathOperation(Traversal startTraversal) {
        String type = startTraversal.getEndStep();
        if (!type.equals("vertex")) {
            return null;
        }
        // case: path()
        PathOperation.PathOp path = PathOperation.createPath();
        path.setStartStep("vertex");
        path.setEndStep("path");
        path.setTraversalType("path");
        return path;
/*        switch (Randomly.fromOptions(PathOperation.Path.values())) {
            case path:
                PathOperation.PathOp path = PathOperation.createPath();
                path.setStartStep("vertex");
                path.setEndStep("path");
                path.setTraversalType("path");
                return path;
            case path_size:
                PathOperation.PathSize pathSize = PathOperation.createPathSize();
                pathSize.setStartStep("vertex");
                pathSize.setEndStep("path");
                pathSize.setTraversalType("path");
                return pathSize;
            case path_count:
                PathOperation.PathCount pathCount = PathOperation.createPathCount();
                pathCount.setStartStep("vertex");
                pathCount.setEndStep("path");
                pathCount.setTraversalType("path");
                return pathCount;
            default:
                throw new AssertionError();
        }*/
    }

    public PathOperation createPathCheckOperation(Traversal startTraversal, String endId) {
        String type = startTraversal.getEndStep();
        if (!type.equals("vertex")) {
            return null;
        }
        PathOperation.PathCheck pathCheck = PathOperation.createPathCheck(endId);
        pathCheck.setStartStep("vertex");
        pathCheck.setEndStep("path");
        pathCheck.setTraversalType("path");
        return pathCheck;
    }

    public PathOperation createPathHopCheckOperation(String id1, String id2, String id3) {
        PathOperation.PathHopCheck pathHopCheck = PathOperation.createPathHopCheck(id1, id2, id3);
        pathHopCheck.setTraversalType("path");
        return pathHopCheck;
    }

    public PathOperation createOneHopCheckOperation(String startId, String endId) {
        PathOperation.OneHopCheck oneHopCheck = PathOperation.createOneHopCheck(startId, endId);
        oneHopCheck.setTraversalType("path");
        return oneHopCheck;
    }

    public StartTraversalOperation createVAllIdsOperation() {
        StartTraversalOperation.VAllIds vAllIds = StartTraversalOperation.createVAllId();
        vAllIds.setTraversalType("V");
        vAllIds.setStartStep("vertex");
        vAllIds.setEndStep("id");
        return vAllIds;
    }

    public StartTraversalOperation createAddEBetweenVerticesOperation(String startId, String endId, String property) {
        StartTraversalOperation.AddEBetweenVertices addEBetweenVertices = StartTraversalOperation.createAddEBetweenVertices(startId, endId, property);
        addEBetweenVertices.setStartStep("vertex");
        addEBetweenVertices.setEndStep("");
        return addEBetweenVertices;
    }

    public StartTraversalOperation createDropEBetweenVerticesOperation(String startId, String endId) {
        StartTraversalOperation.DropEBetweenVertices dropEBetweenVertices = StartTraversalOperation.createDropEBetweenVertices(startId, endId);
        dropEBetweenVertices.setStartStep("vertex");
        dropEBetweenVertices.setEndStep("");
        return dropEBetweenVertices;
    }

    public PathOperation createPathSizeOperation(Traversal startTraversal) {
        String type = startTraversal.getEndStep();
        if (!type.equals("vertex")) {
            return null;
        }
        PathOperation.PathSize pathSize = PathOperation.createPathSize();
        pathSize.setStartStep("vertex");
        pathSize.setEndStep("path");
        pathSize.setTraversalType("path");
        return pathSize;
    }

    public StartTraversalOperation createAddVerticesOperation() {
        String vLabel = "vl" + String.valueOf(Randomly.getInteger(50, 100));
        String vProperty = "vp" + String.valueOf(Randomly.getInteger(50, 100));
        String vValue = "vp" + String.valueOf(Randomly.getInteger(50, 100)) + "val";
        StartTraversalOperation.AddVerticesTraversal addVerticesTraversal = StartTraversalOperation.createAddVertices(vLabel, vProperty, vValue);
        addVerticesTraversal.setStartStep("");
        addVerticesTraversal.setEndStep("");
        return addVerticesTraversal;
    }

    public StartTraversalOperation createAddVerticesOperation(String vLabel, String vProperty, String vValue) {
        StartTraversalOperation.AddVerticesTraversal addVerticesTraversal = StartTraversalOperation.createAddVertices(vLabel, vProperty, vValue);
        addVerticesTraversal.setStartStep("");
        addVerticesTraversal.setEndStep("");
        return addVerticesTraversal;
    }

    public FilterTraversalOperation createFilterOperation(Traversal startTraversal) {
        String type = startTraversal.getEndStep();
        if (type.equals("vertex") || type.equals("edge")) {
            switch (FilterTraversalOperation.getRandomFilterLite()) {
                case has:
                    FilterTraversalOperation.HasKey hasKey = null;
                    if (type.equals("vertex")) {
                        hasKey = FilterTraversalOperation.
                                createHasKey(Randomly.fromList(getVertexLabels()).getRandomVertexProperties().getVertexPropertyName());
                    } else {
                        hasKey = FilterTraversalOperation
                                .createHasKey(Randomly.fromList(getEdgeLabels()).getRandomEdgeProperties().getEdgePropertyName());
                    }
                    hasKey.setStartStep(type);
                    hasKey.setEndStep(type);
                    hasKey.setTraversalType("filter");
                    return hasKey;
                case hasNot:
                    FilterTraversalOperation.HasNotKey hasNotKey = null;
                    if (type.equals("vertex")) {
                        hasNotKey = FilterTraversalOperation
                                .createHasNotKey(Randomly.fromList(getVertexLabels()).getRandomVertexProperties().getVertexPropertyName());
                    } else {
                        hasNotKey = FilterTraversalOperation
                                .createHasNotKey(Randomly.fromList(getEdgeLabels()).getRandomEdgeProperties().getEdgePropertyName());
                    }
                    hasNotKey.setStartStep(type);
                    hasNotKey.setEndStep(type);
                    hasNotKey.setTraversalType("filter");
                    return hasNotKey;
                case hasLabel:
                    FilterTraversalOperation.HasLabel hasLabel = null;
                    if (type.equals("vertex")) {
                        hasLabel = FilterTraversalOperation.createHasLabel(getVertexLabelList());
                    } else {
                        hasLabel = FilterTraversalOperation.createHasLabel(getEdgeLabelList());
                    }
                    hasLabel.setStartStep(type);
                    hasLabel.setEndStep(type);
                    hasLabel.setTraversalType("filter");
                    return hasLabel;
                default:
                    throw new AssertionError();
            }
        } else {
            return null;
        }
    }

    public StatisticTraversalOperation createSizeOperation() {
        StatisticTraversalOperation.Size size = StatisticTraversalOperation.createSize();
        return size;
    }

    public PathOperation createKHopNodesOperation(String startId, String k) {
        PathOperation.KHopNodes kHopNodes = PathOperation.createKHopNodes(startId, k);
        return kHopNodes;
    }

    public PathOperation createKHopNodesReverseOperation(String startId, String k) {
        PathOperation.KHopNodesReverse kHopNodesReverse = PathOperation.createKHopNodesReverse(startId, k);
        return kHopNodesReverse;
    }

    public PathOperation createVSpouseOperation(String startId) {
        PathOperation.VSpouse vSpouse = PathOperation.createVSpouse(startId);
        return vSpouse;
    }

    public PathOperation createVSpouseNoDedupOperation(String startId) {
        PathOperation.VSpouseNoDedup vSpouseNoDedup = PathOperation.createVSpouseNoDedup(startId);
        return vSpouseNoDedup;
    }

    public PathOperation createVDescendantOperation(String startId) {
        PathOperation.VDescendant vDescendant = PathOperation.createVDescendant(startId);
        return vDescendant;
    }

    public PathOperation createVDescendantDedupOperation(String startId) {
        PathOperation.VDescendantDedup vDescendantDedup = PathOperation.createVDescendantDedup(startId);
        return vDescendantDedup;
    }

    public PathOperation createVAncestorOperation(String startId) {
        PathOperation.VAncestor vAncestor = PathOperation.createVAncestor(startId);
        return vAncestor;
    }

    public PathOperation createVAllPathsOperation(String startId, String endId) {
        PathOperation.VAllPaths vAllPaths = PathOperation.createVAllPaths(startId, endId);
        return vAllPaths;
    }

    public StatisticTraversalOperation createCountOperation() {
        StatisticTraversalOperation.Count count = StatisticTraversalOperation.createCount();
        count.setEndStep(ConstantType.LONG.toString());
        count.setTraversalType("statistic");
        return count;
    }

    public StartTraversalOperation createDropVerticesOperation(String startId) {
        StartTraversalOperation.DropVertices dropVertices = StartTraversalOperation.createDropVertices(startId);
        dropVertices.setStartStep("vertex");
        dropVertices.setEndStep("");
        return dropVertices;
    }

    /**
     * ==========  End dividing line for path strategy ==========
     */

    public StartTraversalOperation createStartTraversal() {
        switch (StartTraversalOperation.getRandomStartTraversal()) {
            /*case addV:
                StartTraversalOperation.AddV addV = StartTraversalOperation.createAddV();
                addV.setStartStep("vertex");
                addV.setEndStep("vertex");
                return addV;
            case addV_label:
                StartTraversalOperation.AddVWithLabel addVWithLabel =
                        StartTraversalOperation.createAddVWithLabel(Randomly.fromList(state.getSchema().getVertexList()).getLabelName());
                addVWithLabel.setStartStep("vertex");
                addVWithLabel.setEndStep("vertex");
                return addVWithLabel;
            case addE_label:
                StartTraversalOperation.AddEWithLabel addEWithLabel =
                        StartTraversalOperation.createAddEWithLabel(Randomly.fromList(state.getSchema().getEdgeList()).getLabelName());
                addEWithLabel.setStartStep("edge");
                addEWithLabel.setEndStep("edge");
                return addEWithLabel;*/
            case V:
                StartTraversalOperation.V v = StartTraversalOperation.createV();
                v.setTraversalType("V");
                v.setStartStep("vertex");
                v.setEndStep("vertex");
                return v;
            /*case V_ids:
                StartTraversalOperation.VWithIds vWithIds = StartTraversalOperation.createVWithIds(getVertexIds());
                vWithIds.setStartStep("vertex");
                vWithIds.setEndStep("vertex");
                return vWithIds;*/
            case E:
                StartTraversalOperation.E e = StartTraversalOperation.createE();
                e.setTraversalType("E");
                e.setStartStep("edge");
                e.setEndStep("edge");
                return e;
           /* case E_ids:
                StartTraversalOperation.EWithIds eWithIds = StartTraversalOperation.createEWithIds(getEdgeIds());
                eWithIds.setStartStep("edge");
                eWithIds.setEndStep("edge");
                return eWithIds;*/
            /*case tx:
                return StartTraversalOperation.createTx();*/
            default:
                throw new AssertionError();
        }
    }

    public Traversal getRandomTraversal(Traversal startTraversal) {
        switch (Randomly.fromOptions(GraphFilterTraversal.values())) {
            case FILTER_TRAVERSAL:
                return createFilterTraversalOperation(startTraversal);
            case NEIGHBOR_TRAVERSAL:
                return createNeighborTraversalOperation(startTraversal);
            case PROPERTY:
                return createProperties(startTraversal);
            case STATISTIC:
                return createStatistic(startTraversal);
            case ORDER:
                return createOrder(startTraversal);
            default:
                throw new AssertionError();
        }
    }

    public Traversal getIsPredicate(String type) {
        FilterTraversalOperation.IsPredicate isPredicate = FilterTraversalOperation.createIsPredicate(createPredicate(ConstantType.valueOf(type)));
        isPredicate.setStartStep(type);
        isPredicate.setEndStep(type);
        isPredicate.setTraversalType("filter");
        return isPredicate;
    }

    public FilterTraversalOperation createFilterTraversalOperation(Traversal startTraversal) {
        Traversal t = getRandomTraversal(startTraversal);
        String type = startTraversal.getEndStep();
//        System.out.println("start type: " + type);
        if (type.equals("vertex") || type.equals("edge")) {
            switch (FilterTraversalOperation.getRandomFilter()) {
                case or:
                    FilterTraversalOperation.Or or = FilterTraversalOperation.createOr(t);
                    or.setStartStep(type);
                    or.setEndStep(type);
                    or.setTraversalType("filter");
                    return or;
                case and:
                    FilterTraversalOperation.And and = FilterTraversalOperation.createAnd(t);
                    and.setStartStep(type);
                    and.setEndStep(type);
                    and.setTraversalType("filter");
                    return and;
                case not:
                    FilterTraversalOperation.Not not = FilterTraversalOperation.createNot(t);
                    not.setStartStep(type);
                    not.setEndStep(type);
                    not.setTraversalType("filter");
                    return not;

                case where_traversal:
                    FilterTraversalOperation.WhereTraversal whereTraversal =
                            FilterTraversalOperation.createWhereTraversal(t);
                    whereTraversal.setStartStep(type);
                    whereTraversal.setEndStep(type);
                    whereTraversal.setTraversalType("filter");
                    return whereTraversal;
                case has_key_predicate:
                    FilterTraversalOperation.HasKeyPredicate hasKeyPredicate = null;
                    if (type.equals("vertex")) {
                        GraphSchema.GraphVertexProperty property = Randomly.fromList(getVertexLabels()).getRandomVertexProperties();
                        hasKeyPredicate =
                                FilterTraversalOperation.createHasKeyPredicate(property.getVertexPropertyName(), createPredicate(property.getDataType()));
                    } else {
                        GraphSchema.GraphEdgeProperty property = Randomly.fromList(getEdgeLabels()).getRandomEdgeProperties();
                        hasKeyPredicate =
                                FilterTraversalOperation.createHasKeyPredicate(property.getEdgePropertyName(), createPredicate(property.getDataType()));
                    }
                    hasKeyPredicate.setStartStep(type);
                    hasKeyPredicate.setEndStep(type);
                    hasKeyPredicate.setTraversalType("filter");
                    return hasKeyPredicate;
                case has_key_value:
                    FilterTraversalOperation.HasKeyValue hasKeyValue = null;
                    if (type.equals("vertex")) {
                        GraphSchema.GraphVertexProperty property = Randomly.fromList(getVertexLabels()).getRandomVertexProperties();
                        String vp = property.getVertexPropertyName();
                        List<GraphConstant> vlist = state.getGraphData().getVpValuesMap().get(vp);
                        hasKeyValue =
                                FilterTraversalOperation.createHasKeyValue(vp, vlist.get(Randomly.getInteger(0, vlist.size())));
                    } else {
                        GraphSchema.GraphEdgeProperty property = Randomly.fromList(getEdgeLabels()).getRandomEdgeProperties();
                        String ep = property.getEdgePropertyName();
                        List<GraphConstant> elist = state.getGraphData().getEpValueMap().get(ep);
                        hasKeyValue =
                                FilterTraversalOperation.createHasKeyValue(ep, elist.get(Randomly.getInteger(0, elist.size())));
                    }
                    hasKeyValue.setStartStep(type);
                    hasKeyValue.setEndStep(type);
                    hasKeyValue.setTraversalType("filter");
                    return hasKeyValue;
                /*case has_key_traversal:
                    FilterTraversalOperation.HasKeyTraversal hasKeyTraversal = null;
                    if(type.equals("vertex")){
                        GraphSchema.GraphVertexProperty property = Randomly.fromList(getVertexLabels()).getRandomVertexProperties();
                        t = getRandomTraversal(createProperties(startTraversal));
                        hasKeyTraversal = FilterTraversalOperation.createHasKeyTraversal(property.getVertexPropertyName(), t);
                    }else{
                        GraphSchema.GraphEdgeProperty property = Randomly.fromList(getEdgeLabels()).getRandomEdgeProperties();
                        while(t.getTraversalType().equals("property")){
                            t = getRandomTraversal(startTraversal);
                        }
                        hasKeyTraversal = FilterTraversalOperation.createHasKeyTraversal(property.getEdgePropertyName(), t);
                    }
                    hasKeyTraversal.setStartStep(type);
                    hasKeyTraversal.setEndStep(type);
                    hasKeyTraversal.setTraversalType("filter");
                    return hasKeyTraversal;*/
                case has_label_key_value:
                    FilterTraversalOperation.HasLabelKeyPredicate hasLabelKeyPredicate = null;
                    if (type.equals("vertex")) {
                        GraphSchema.GraphVertexLabel label = Randomly.fromList(getVertexLabels());
                        GraphSchema.GraphVertexProperty property = label.getRandomVertexProperties();
                        hasLabelKeyPredicate =
                                FilterTraversalOperation.createHasLabelKeyPredicate(label.getLabelName(),
                                        property.getVertexPropertyName(), createPredicate(property.getDataType()));
                    } else {
                        GraphSchema.GraphRelationship label = Randomly.fromList(getEdgeLabels());
                        GraphSchema.GraphEdgeProperty property = label.getRandomEdgeProperties();
                        hasLabelKeyPredicate =
                                FilterTraversalOperation.createHasLabelKeyPredicate(label.getLabelName(),
                                        property.getEdgePropertyName(), createPredicate(property.getDataType()));
                    }
                    hasLabelKeyPredicate.setStartStep(type);
                    hasLabelKeyPredicate.setEndStep(type);
                    hasLabelKeyPredicate.setTraversalType("filter");
                    return hasLabelKeyPredicate;
                case has:
                    FilterTraversalOperation.HasKey hasKey = null;
                    if (type.equals("vertex")) {
                        hasKey = FilterTraversalOperation.
                                createHasKey(Randomly.fromList(getVertexLabels()).getRandomVertexProperties().getVertexPropertyName());
                    } else {
                        hasKey = FilterTraversalOperation
                                .createHasKey(Randomly.fromList(getEdgeLabels()).getRandomEdgeProperties().getEdgePropertyName());
                    }
                    hasKey.setStartStep(type);
                    hasKey.setEndStep(type);
                    hasKey.setTraversalType("filter");
                    return hasKey;
                case hasNot:
                    FilterTraversalOperation.HasNotKey hasNotKey = null;
                    if (type.equals("vertex")) {
                        hasNotKey = FilterTraversalOperation
                                .createHasNotKey(Randomly.fromList(getVertexLabels()).getRandomVertexProperties().getVertexPropertyName());
                    } else {
                        hasNotKey = FilterTraversalOperation
                                .createHasNotKey(Randomly.fromList(getEdgeLabels()).getRandomEdgeProperties().getEdgePropertyName());
                    }
                    hasNotKey.setStartStep(type);
                    hasNotKey.setEndStep(type);
                    hasNotKey.setTraversalType("filter");
                    return hasNotKey;
                case hasLabel:
                    FilterTraversalOperation.HasLabel hasLabel = null;
                    if (type.equals("vertex")) {
                        hasLabel = FilterTraversalOperation.createHasLabel(getVertexLabelList());
                    } else {
                        hasLabel = FilterTraversalOperation.createHasLabel(getEdgeLabelList());
                    }
                    hasLabel.setStartStep(type);
                    hasLabel.setEndStep(type);
                    hasLabel.setTraversalType("filter");
                    return hasLabel;
                /*case hasId:
                    FilterTraversalOperation.HasId hasId = null;
                    if(type.equals("vertex")){
                        hasId = FilterTraversalOperation.createHasId(getVertexIds());
                    }else{
                        hasId = FilterTraversalOperation.createHasId(getEdgeIds());
                    }
                    hasId.setStartStep(type);
                    hasId.setEndStep(type);
                    hasId.setTraversalType("filter");
                    return hasId;*/
                case where_count_is:
                    NeighborTraversalOperation neighbor = type.equals("vertex") ? createVertexNeighbor(type) : createEdgeNeighbor(startTraversal);
                    FilterTraversalOperation.WhereCountIs whereCountIs =
                            FilterTraversalOperation.createWhereCountIs((FilterTraversalOperation.IsPredicate) getIsPredicate(ConstantType.LONG.toString()),
                                    neighbor, (StatisticTraversalOperation.Count) createStatistic(startTraversal));
                    whereCountIs.setStartStep(type);
                    whereCountIs.setEndStep(type);
                    whereCountIs.setTraversalType("filter");
                    return whereCountIs;
                case where_value_is:
                    NeighborTraversalOperation neighbor1 = type.equals("vertex") ? createVertexNeighbor(type) : createEdgeNeighbor(startTraversal);
                    FilterTraversalOperation.WhereValuesIs whereValuesIs = null;
                    if (type.equals("vertex")) {
                        EdgePropertyReference edgePropertyReference = (EdgePropertyReference) getEdgeProperty(type);
                        whereValuesIs =
                                FilterTraversalOperation.createWhereValueIs((FilterTraversalOperation.IsPredicate) getIsPredicate(edgePropertyReference.getDataType().toString()),
                                        neighbor1, edgePropertyReference);
                    } else if (type.equals("edge")) {
                        VertexPropertyReference vertexPropertyReference = (VertexPropertyReference) getVertexProperty(type);
                        whereValuesIs =
                                FilterTraversalOperation.createWhereValueIs((FilterTraversalOperation.IsPredicate) getIsPredicate(vertexPropertyReference.getDataType().toString()),
                                        neighbor1, vertexPropertyReference);
                    }
                    whereValuesIs.setStartStep(type);
                    whereValuesIs.setEndStep(type);
                    whereValuesIs.setTraversalType("filter");
                    return whereValuesIs;
                case where_statistic_is:
                    StatisticTraversalOperation statistic = null;
                    FilterTraversalOperation.IsPredicate isPredicate = null;
                    PropertyTraversalOperation.Values properties = null;
                    ConstantType constantType =
                            (ConstantType) (type.equals("vertex")
                                    ? state.getSchema().getVertexPropertyMap().keySet().toArray()[Randomly.getInteger(0, state.getSchema().getVertexPropertyMap().size() - 1)]
                                    : state.getSchema().getEdgePropertyMap().keySet().toArray()[Randomly.getInteger(0, state.getSchema().getEdgePropertyMap().size() - 1)]);
                    if (type.equals("vertex")) {
                        List list = state.getSchema().getVertexPropertyMap().get(constantType);
                        GraphSchema.GraphVertexProperty property = (GraphSchema.GraphVertexProperty) list.get(Randomly.getInteger(0, list.size() - 1));
                        properties = new PropertyTraversalOperation.Values(Arrays.asList(property.getVertexPropertyName()));
                    } else {
                        List list = state.getSchema().getEdgePropertyMap().get(constantType);
                        GraphSchema.GraphEdgeProperty property = (GraphSchema.GraphEdgeProperty) list.get(Randomly.getInteger(0, list.size() - 1));
                        properties = new PropertyTraversalOperation.Values(Arrays.asList(property.getEdgePropertyName()));
                    }
                    if (ConstantType.isNumber(constantType.toString())) {
                        switch (Randomly.fromOptions(StatisticTraversalOperation.AggregateOperator.values())) {
                            case sum:
                                statistic = StatisticTraversalOperation.createSum();
                                break;
                            case mean:
                                statistic = StatisticTraversalOperation.createMean();
                                break;
                            case max:
                                statistic = StatisticTraversalOperation.createMax();
                                break;
                            case min:
                                statistic = StatisticTraversalOperation.createMin();
                                break;
                        }
                    } else {
                        statistic = Randomly.getBoolean() ? StatisticTraversalOperation.createMax() : StatisticTraversalOperation.createMin();
                    }
                    isPredicate = (FilterTraversalOperation.IsPredicate) getIsPredicate(constantType.toString());
                    FilterTraversalOperation.WhereStatisticIs whereStatisticIs = FilterTraversalOperation.createWhereStatisticIs(isPredicate, properties, statistic);
                    whereStatisticIs.setStartStep(type);
                    whereStatisticIs.setEndStep(type);
                    whereStatisticIs.setTraversalType("filter");
                    return whereStatisticIs;
                default:
                    throw new AssertionError();
            }
        } else {
            return null;
        }
    }

    public List<Traversal> createHasTraversalOperation(Traversal startTraversal) {
        List<Traversal> res = new ArrayList<>();
        String type = startTraversal.getEndStep();
        FilterTraversalOperation.HasKeyPredicate hasKeyPredicate = null;
        FilterTraversalOperation.HasKeyPredicate hasKeyNegatedPredicate = null;
        FilterTraversalOperation.HasNotKey hasNotKey = null;
        Predicate hasPredicate = null;
        Predicate hasNegatedPredicate = null;
        if (type.equals("vertex")) {
            GraphSchema.GraphVertexProperty property = Randomly.fromList(getVertexLabels()).getRandomVertexProperties();
            hasPredicate = createPredicate(property.getDataType());
            hasNegatedPredicate = createNegatedPredicate(hasPredicate);
            hasKeyPredicate =
                    FilterTraversalOperation.createHasKeyPredicate(property.getVertexPropertyName(), hasPredicate);
            hasKeyNegatedPredicate =
                    FilterTraversalOperation.createHasKeyPredicate(property.getVertexPropertyName(), hasNegatedPredicate);
            hasNotKey =
                    FilterTraversalOperation.createHasNotKey(property.getVertexPropertyName());
        } else {
            GraphSchema.GraphEdgeProperty property = Randomly.fromList(getEdgeLabels()).getRandomEdgeProperties();
            hasPredicate = createPredicate(property.getDataType());
            hasNegatedPredicate = createNegatedPredicate(hasPredicate);
            hasKeyPredicate =
                    FilterTraversalOperation.createHasKeyPredicate(property.getEdgePropertyName(), hasPredicate);
            hasKeyNegatedPredicate =
                    FilterTraversalOperation.createHasKeyPredicate(property.getEdgePropertyName(), hasNegatedPredicate);
            hasNotKey =
                    FilterTraversalOperation.createHasNotKey(property.getEdgePropertyName());
        }
        hasKeyPredicate.setStartStep(type);
        hasKeyPredicate.setEndStep(type);
        hasKeyPredicate.setTraversalType("filter");
        res.add(hasKeyPredicate);
        hasKeyNegatedPredicate.setStartStep(type);
        hasKeyNegatedPredicate.setEndStep(type);
        hasKeyNegatedPredicate.setTraversalType("filter");
        res.add(hasKeyNegatedPredicate);
        hasNotKey.setStartStep(type);
        hasNotKey.setEndStep(type);
        hasNotKey.setTraversalType("filter");
        res.add(hasNotKey);
        return res;
    }

    public Traversal createDedupTraversalOperation() {
        Predicate.Dedup dedup = Predicate.createDedup();
        return dedup;
    }

    public NeighborTraversalOperation createNeighborTraversalOperation(Traversal startTraversal) {
        String type = startTraversal.getEndStep();
        if (type.equals("vertex")) {
            return createVertexNeighbor(type);
        } else if (type.equals("edge")) {
            return createEdgeNeighbor(startTraversal);
        } else {
            return null;
        }
    }

    public Traversal createPropertyOrConstant(Traversal startTraversal) {
        Traversal t = null;
        while (t == null) {
            switch (Randomly.fromOptions(GraphFilterTraversal.values())) {
                case PROPERTY:
                    return createProperties(startTraversal);
                case STATISTIC:
                    return createStatistic(startTraversal);
                default:
                    continue;
            }
        }
        return t;
    }

    public Traversal createProperty(Traversal startTraversal) {
        Traversal t = null;
        while (t == null) {
            t = createProperties(startTraversal);
        }
        return t;
    }

    public Predicate createPredicate(ConstantType type) {
        switch (Predicate.getRandomPredicate()) {
            case eq:
                return Predicate.createEq(generateConstant(type));
            case neq:
                return Predicate.createNeq(generateConstant(type));
            case lt:
                return Predicate.createLt(generateConstant(type));
            case lte:
                return Predicate.createLte(generateConstant(type));
            case gt:
                return Predicate.createGt(generateConstant(type));
            case gte:
                return Predicate.createGte(generateConstant(type));
            case inside:
                return Predicate.createInside(generateConstant(type),
                        generateConstant(type));
            case outside:
                return Predicate.createOutside(generateConstant(type),
                        generateConstant(type));
            case between:
                return Predicate.createBetween(generateConstant(type),
                        generateConstant(type));
            case not:
                return Predicate.createNot(createPredicate(type));
            case and:
                return Predicate.createAnd(createPredicate(type), createPredicate(type));
            case or:
                return Predicate.createOr(createPredicate(type), createPredicate(type));
            default:
                throw new AssertionError();
        }
    }

    public Predicate createNegatedPredicate(Predicate predicate) {
        return Predicate.createNegatedPredicate(predicate);
    }

    public NeighborTraversalOperation createVertexNeighbor(String type) {
        switch (Randomly.fromOptions(NeighborTraversalOperation.NeighborV.values())) {
            case out:
                NeighborTraversalOperation.Out out = NeighborTraversalOperation.createOut(getEdgeLabelList());
                out.setStartStep(type);
                out.setEndStep("vertex");
                out.setTraversalType("neighbor");
                return out;
            case in:
                NeighborTraversalOperation.In in = NeighborTraversalOperation.createIn(getEdgeLabelList());
                in.setStartStep(type);
                in.setEndStep("vertex");
                in.setTraversalType("neighbor");
                return in;
            case both:
                NeighborTraversalOperation.Both both = NeighborTraversalOperation.createBoth(getEdgeLabelList());
                both.setStartStep(type);
                both.setEndStep("vertex");
                both.setTraversalType("neighbor");
                return both;
            case outE:
                NeighborTraversalOperation.OutE outE = NeighborTraversalOperation.createOutE(getEdgeLabelList());
                outE.setStartStep(type);
                outE.setEndStep("edge");
                outE.setTraversalType("neighbor");
                return outE;
            case inE:
                NeighborTraversalOperation.InE inE = NeighborTraversalOperation.createInE(getEdgeLabelList());
                inE.setStartStep(type);
                inE.setEndStep("edge");
                inE.setTraversalType("neighbor");
                return inE;
            case bothE:
                NeighborTraversalOperation.BothE bothE = NeighborTraversalOperation.createBothE(getEdgeLabelList());
                bothE.setStartStep(type);
                bothE.setEndStep("edge");
                bothE.setTraversalType("neighbor");
                return bothE;
            default:
                throw new AssertionError();
        }
    }

    public NeighborTraversalOperation createEdgeNeighbor(Traversal startTraversal) {
        String type = startTraversal.getEndStep();
        switch (Randomly.fromOptions(NeighborTraversalOperation.NeighborE.values())) {
            case outV:
                NeighborTraversalOperation.OutV outV = NeighborTraversalOperation.createOutV();
                outV.setStartStep(type);
                outV.setEndStep("vertex");
                outV.setTraversalType("neighbor");
                return outV;
            case inV:
                NeighborTraversalOperation.InV inV = NeighborTraversalOperation.createInV();
                inV.setStartStep(type);
                inV.setEndStep("vertex");
                inV.setTraversalType("neighbor");
                return inV;
            case bothV:
                NeighborTraversalOperation.BothV bothV = NeighborTraversalOperation.createBothV();
                bothV.setStartStep(type);
                bothV.setEndStep("vertex");
                bothV.setTraversalType("neighbor");
                return bothV;
/*            case otherV:
                // g.E().otherV() is not true
                if(startTraversal.getTraversalType().equals("E")) {
                    return createNeighborTraversalOperation(startTraversal);
                }
                NeighborTraversalOperation.OtherV otherV = NeighborTraversalOperation.createOtherV();
                otherV.setStartStep(type);
                otherV.setEndStep("vertex");
                otherV.setTraversalType("neighbor");
                return otherV;  */
            default:
                throw new AssertionError();
        }
    }

    public StatisticTraversalOperation createStatistic(Traversal startTraversal) {
        if (startTraversal.getEndStep().equals("vertex") || startTraversal.getEndStep().equals("edge")) {
            StatisticTraversalOperation.Count count = StatisticTraversalOperation.createCount();
            count.setStartStep(startTraversal.getEndStep());
            count.setEndStep(ConstantType.LONG.toString());
            count.setTraversalType("statistic");
            return count;
        } else if (ConstantType.isNumber(startTraversal.getEndStep())) {
            switch (Randomly.fromOptions(StatisticTraversalOperation.AggregateOperator.values())) {
                case sum:
                    StatisticTraversalOperation.Sum sum = StatisticTraversalOperation.createSum();
                    sum.setStartStep(startTraversal.getEndStep());
                    sum.setEndStep(startTraversal.getEndStep());
                    sum.setTraversalType("statistic");
                    return sum;
                case mean:
                    StatisticTraversalOperation.Mean mean = StatisticTraversalOperation.createMean();
                    mean.setStartStep(startTraversal.getEndStep());
                    mean.setEndStep(startTraversal.getEndStep());
                    mean.setTraversalType("statistic");
                    return mean;
                case max:
                    StatisticTraversalOperation.Max max = StatisticTraversalOperation.createMax();
                    max.setStartStep(startTraversal.getEndStep());
                    max.setEndStep(startTraversal.getEndStep());
                    max.setTraversalType("statistic");
                    return max;
                case min:
                    StatisticTraversalOperation.Min min = StatisticTraversalOperation.createMin();
                    min.setStartStep(startTraversal.getEndStep());
                    min.setEndStep(startTraversal.getEndStep());
                    min.setTraversalType("statistic");
                    return min;
                default:
                    throw new AssertionError();
            }
        } else {
            // max, min can be used for any object
            if (Randomly.getBoolean()) {
                StatisticTraversalOperation.Max max = StatisticTraversalOperation.createMax();
                max.setStartStep(startTraversal.getEndStep());
                max.setEndStep(startTraversal.getEndStep());
                max.setTraversalType("statistic");
                return max;
            } else {
                StatisticTraversalOperation.Min min = StatisticTraversalOperation.createMin();
                min.setStartStep(startTraversal.getEndStep());
                min.setEndStep(startTraversal.getEndStep());
                min.setTraversalType("statistic");
                return min;
            }
        }
    }

    public OrderingTermOperation createOrder(Traversal startTraversal) {
        switch (OrderingTermOperation.getRandom()) {
            case ASC:
                OrderingTermOperation.ASC asc = OrderingTermOperation.createASC();
                asc.setStartStep(startTraversal.getEndStep());
                asc.setEndStep(startTraversal.getEndStep());
                asc.setTraversalType("order");
                return asc;
            case DESC:
                OrderingTermOperation.DESC desc = OrderingTermOperation.createDESC();
                desc.setStartStep(startTraversal.getEndStep());
                desc.setEndStep(startTraversal.getEndStep());
                desc.setTraversalType("order");
                return desc;
        }
        return null;
    }

    public List<GraphSchema.GraphVertexLabel> getVertexLabels() {
        return Randomly.nonEmptySubList(state.getSchema().getVertexList());
    }

    public List<GraphSchema.GraphRelationship> getEdgeLabels() {
        return Randomly.nonEmptySubList(state.getSchema().getEdgeList());
    }

    public StringBuilder getExpression() {
        return expression;
    }

    public List<String> getVertexLabelList() {
        return Randomly.nonEmptySubList(state.getGraphData().getVertexLabels());
    }

    public List<String> getEdgeLabelList() {
        return Randomly.nonEmptySubList(state.getGraphData().getEdgeLabels());
    }

    public List<String> getVertexIds() {
//        return Randomly.nonEmptySubList(state.getConnection().getG().V().id().toList());
        List<Integer> idsList = IntStream.range(0, (int) state.getVerticesMaxNum()).boxed().collect(Collectors.toList());
        List<String> vertexIdsList = idsList.stream().map(String::valueOf).collect(Collectors.toList());
        return Randomly.nonEmptySubList(vertexIdsList);
    }

    public List<String> getRandomVertexIds() {
        List<String> vertexIdsList = new ArrayList<>();
        vertexIdsList.add(String.valueOf(Randomly.getInteger(0, (int) state.getVerticesMaxNum())));
        return vertexIdsList;
    }

    public List<Object> getEdgeIds() {
        return Randomly.nonEmptySubList(state.getConnection().getG().E().id().toList());
    }

    public List<String> getRandomEdgeIds() {
        List<String> edgeIdsList = new ArrayList<>();
        edgeIdsList.add(String.valueOf(Randomly.getInteger(0, (int) state.getEdgesMaxNum())));
        return edgeIdsList;
    }

    @Override
    public GraphExpression generateExpression(int depth) {
        if (depth >= state.getGenerateDepth()) return generateLeafNode();
        // randomly generate actions
        switch (Randomly.fromOptions(Actions.values())) {
            //TODO: edge_property
            case VERTEX_PROPERTY:
                System.out.println("VERTEX_PROPERTY : " + depth);
                return generateProperties();
            case LITERAL:
                System.out.println("LITERAL : " + depth);
                GraphExpression d = generateConstant();
                System.out.println(d.toString());
                return d;
            /*case UNARY_PREFIX:
                System.out.println("UNARY_PREFIX : " + depth);
                UnaryPrefixOperation a = new UnaryPrefixOperation(generateExpression(depth + 1), UnaryPrefixOperation.EmunUnaryPrefixOperator.getRandom());
                System.out.println(a.toString());
                return a;*/
            case BINARY_COMPARISON:
                System.out.println("BINARY_COMPARISON : " + depth);
                BinaryComparisonOperation b = new BinaryComparisonOperation(null, generateExpression(depth + 1),
                        BinaryComparisonOperation.EnumBinaryComparisonOperator.getRandom());
                System.out.println(b.toString());
                return b;
            case BINARY_LOGICAL:
                System.out.println("BINARY_LOGICAL : " + depth);
                BinaryLogicalOperation c = new BinaryLogicalOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                        BinaryLogicalOperation.EnumBinaryLogicalOperator.getRandom());
                System.out.println(c.toString());
                return c;
            default:
                throw new AssertionError();
        }
    }

    @Override
    protected GraphExpression generateProperties() {
        return null;
    }

    public GraphConstant generateConstant(ConstantType type) {
        switch (type) {
            case INTEGER:
                int int_value = (int) state.getRandomly().getInteger();
                return GraphConstant.createIntConstant(int_value);
            /*case NULL:
                return GraphConstant.createNullConstant();*/
            case STRING:
                String string_value = state.getRandomly().getString();
                return GraphConstant.createStringConstant(string_value);
            case DOUBLE:
                double double_value = state.getRandomly().getDouble();
                return GraphConstant.createDoubleConstant(double_value);
            case BOOLEAN:
                boolean boolean_value = state.getRandomly().getBoolean();
                return GraphConstant.createBooleanConstant(boolean_value);
            case FLOAT:
                return GraphConstant.createFloatConstant(state.getRandomly().getFloat());
            case LONG:
                return GraphConstant.createLongConstant(state.getRandomly().getLong());
            default:
                throw new AssertionError();
        }
    }

    @Override
    public Traversal generateConstant() {
        ConstantType type = Randomly.fromOptions(ConstantType.values());
        return generateConstant(type);
    }


    public Traversal getVertexProperty(String type) {
        // randomly choose one property
        GraphSchema.GraphVertexProperty property = null;
        // get the value of this property
        VertexPropertyReference vertexProperty = null;
        while (vertexProperty == null) {
            property = Randomly.fromList(state.getSchema().getVertexProperties());
            vertexProperty = getVertexPropertyValue(property);
        }
        vertexProperty.setStartStep(type);
        vertexProperty.setEndStep(property.getDataType().toString());
        vertexProperty.setDataType(property.getDataType());
        vertexProperty.setTraversalType("property");
        return vertexProperty;
    }

    public Traversal getEdgeProperty(String type) {
        GraphSchema.GraphEdgeProperty property = null;
        EdgePropertyReference edgeProperty = null;
        while (edgeProperty == null) {
            property = Randomly.fromList(state.getSchema().getEdgeProperties());
            edgeProperty = getEdgePropertyValue(property);
        }
        edgeProperty.setStartStep(type);
        edgeProperty.setEndStep(property.getDataType().toString());
        edgeProperty.setDataType(property.getDataType());
        edgeProperty.setTraversalType("property");
        return edgeProperty;
    }

    /**
     * Generate property of a certain vertex label
     *
     * @return
     */
    protected Traversal createProperties(Traversal startTraversal) {
        String type = startTraversal.getEndStep();
        if (type.equals("vertex")) {
            return getVertexProperty(type);
        } else if (type.equals("edge")) {
            return getEdgeProperty(type);
        } else {
            return null;
        }
    }

    public EdgePropertyReference getEdgePropertyValue(GraphSchema.GraphEdgeProperty property) {
        ConstantType type = property.getDataType();
        List list = state.getGraphData().getEdgeProperties().get(property.getEdgePropertyName());
        if (list == null || list.size() == 0) return null;
        Object o = Randomly.fromList(list);
        switch (type) {
            case INTEGER:
                return EdgePropertyReference.create(property, new GraphConstant.GraphIntConstant(Long.valueOf(o.toString())));
            case DOUBLE:
                return EdgePropertyReference.create(property, new GraphConstant.GraphDoubleConstant(Double.valueOf(o.toString())));
            case BOOLEAN:
                return EdgePropertyReference.create(property, new GraphConstant.GraphBooleanConstant(Boolean.valueOf(o.toString())));
            case FLOAT:
                return EdgePropertyReference.create(property, new GraphConstant.GraphFloatConstant(Float.valueOf(o.toString())));
            case LONG:
                return EdgePropertyReference.create(property, new GraphConstant.GraphLongConstant(Long.valueOf(o.toString())));
            /*case NULL:
                return EdgePropertyReference.create(property, new GraphConstant.GraphNullConstant());*/
            default:
                return EdgePropertyReference.create(property, new GraphConstant.GraphStringConstant(o.toString()));
        }
    }

    //INTEGER, NULL, STRING, DOUBLE, BOOLEAN, FLOAT, LONG;
    public VertexPropertyReference getVertexPropertyValue(GraphSchema.GraphVertexProperty property) {
        ConstantType type = property.getDataType();
        List list = state.getGraphData().getVertexProperties().get(property.getVertexPropertyName());
        if (list == null || list.size() == 0) return null;
        Object o = Randomly.fromList(list);
        switch (type) {
            case INTEGER:
                return VertexPropertyReference.create(property, new GraphConstant.GraphIntConstant(Long.valueOf(o.toString())));
            case DOUBLE:
                return VertexPropertyReference.create(property, new GraphConstant.GraphDoubleConstant(Double.valueOf(o.toString())));
            case BOOLEAN:
                return VertexPropertyReference.create(property, new GraphConstant.GraphBooleanConstant(Boolean.valueOf(o.toString())));
            case FLOAT:
                return VertexPropertyReference.create(property, new GraphConstant.GraphFloatConstant(Float.valueOf(o.toString())));
            case LONG:
                return VertexPropertyReference.create(property, new GraphConstant.GraphLongConstant(Long.valueOf(o.toString())));
            /*case NULL:
                return VertexPropertyReference.create(property, new GraphConstant.GraphNullConstant());*/
            default:
                return VertexPropertyReference.create(property, new GraphConstant.GraphStringConstant(o.toString()));
        }
    }


    @Override
    public GraphExpression negatePredicate(GraphExpression predicate) {
        return null;
    }

    @Override
    public GraphExpression isNull(GraphExpression expr) {
        return null;
    }

    public Map<String, List<GraphSchema.GraphVertexLabel>> getInVertexLabelMap() {
        return inVertexLabelMap;
    }

    public void setInVertexLabelMap(Map<String, List<GraphSchema.GraphVertexLabel>> inVertexLabelMap) {
        this.inVertexLabelMap = inVertexLabelMap;
    }

    public Map<String, List<GraphSchema.GraphVertexLabel>> getOutVertexLabelMap() {
        return outVertexLabelMap;
    }

    public void setOutVertexLabelMap(Map<String, List<GraphSchema.GraphVertexLabel>> outVertexLabelMap) {
        this.outVertexLabelMap = outVertexLabelMap;
    }

}
