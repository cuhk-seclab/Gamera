package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;

import java.util.List;

public class StartTraversalOperation extends Traversal implements GraphExpression {

    public enum Start {
    /*  addV("addV"),
        addV_label("addV_label"),
        addV_Traversal("addV_Traversal"),
        addE_label("addE_label"),
        addE_Traversal("addE_Traversal"),  */
        V("V"),
    /*  V_ids("V_ids"),
        E_ids("E_ids"),  */
        E("E");
    /*  tx("tx")  */

        private String start;

        Start(String start) {
            this.start = start;
        }

        public String getStart() {
            return start;
        }
    }

    /**
     * Path strategy
     */
    public enum StartPath {
        V("V"),
        V_ids("V_ids");

        private String startPath;

        StartPath(String startPath) {
            this.startPath = startPath;
        }

        public String getStartPath() {
            return startPath;
        }
    }

    public static Start getRandomStartTraversal() {
        return Randomly.fromOptions(Start.values());
    }

    public static StartPath getRandomStartPathTraversal() {
        return Randomly.fromOptions(StartPath.values());
    }

    public static class AddV extends StartTraversalOperation {
        public AddV() {
        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "end";
        }

        @Override
        public String toString() {
            return "addV()";
        }
    }

    public static class AddVWithLabel extends StartTraversalOperation {
        private String label;

        public AddVWithLabel(String label) {
            this.label = label;
        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "vertex";
        }

        public String getLabel() {
            return label;
        }

        @Override
        public String toString() {
            return "addV(" + label + ")";
        }
    }

    public static class AddVTraversal extends StartTraversalOperation {
        private Traversal traversal;

        public AddVTraversal(Traversal traversal) {
            this.traversal = traversal;
        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "vertex";
        }

        public Traversal getTraversal() {
            return traversal;
        }

        @Override
        public String toString() {
            return "addV(" + traversal + ")";
        }
    }

    public static class AddVerticesTraversal extends StartTraversalOperation {
        private String vLabel;
        private String vProperty;
        private String vValue;

        public AddVerticesTraversal(String vLabel, String vProperty, String vValue) {
            this.vLabel = vLabel;
            this.vProperty = vProperty;
            this.vValue = vValue;
        }

        public String getStartType() {
            return "";
        }

        public String getEndType() {
            return "";
        }

        @Override
        public String toString() {
//            return "addV('" + "vl50" + "').property('" + "vp50" + "', '" + "vp50val" + "'";
//            return "addV('" + vLabel + "').property('" + vProperty + "','" + vValue + "'";
            return "addV('" + vLabel + "').property('" + vProperty + "','" + vValue + "')";
        }
    }

    public static class AddEWithLabel extends StartTraversalOperation {
        private String label;

        public AddEWithLabel(String label) {
            this.label = label;
        }

        public String getStartType() {
            return "edge";
        }

        public String getEndType() {
            return "edge";
        }

        public String getLabel() {
            return label;
        }

        @Override
        public String toString() {
            return "addE(" + label + ")";
        }
    }

    public static class AddETraversal extends StartTraversalOperation {
        private Traversal traversal;

        public AddETraversal(Traversal traversal) {
            this.traversal = traversal;
        }

        public String getStartType() {
            return "edge";
        }

        public String getEndType() {
            return "edge";
        }

        public Traversal getTraversal() {
            return traversal;
        }

        @Override
        public String toString() {
            return "addE(" + traversal + ")";
        }
    }

    public static class AddEBetweenVertices extends StartTraversalOperation {
        private String startId;
        private String endId;
        private String label;

        public AddEBetweenVertices(String startId, String endId, String label) {
            this.startId = startId;
            this.endId = endId;
            this.label = label;
        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "";
        }

        @Override
        public String toString() {
            return "V(" + startId + ").as(\"" + startId + "\").V(" + endId + ").as(\"" + endId +
                    "\").addE(\"" + label + "\").from(\"" + startId + "\").to(\"" + endId + "\")";
        }
    }

    public static class DropEBetweenVertices extends StartTraversalOperation {
        private String startId;
        private String endId;

        public DropEBetweenVertices(String startId, String endId) {
            this.startId = startId;
            this.endId = endId;
        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "";
        }

        @Override
        public String toString() {
            return "V(" + startId + ").outE().where(otherV().hasId(" + endId + ")).drop()";
        }
    }

    public static class DropVertices extends StartTraversalOperation {
        private String startId;

        public DropVertices(String startId) {
            this.startId = startId;
        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "";
        }

        @Override
        public String toString() {
            return "V(" + startId + ").drop()";
        }
    }

    public static class VWithIds extends StartTraversalOperation {
        private List<String> vertexIds;

        public VWithIds(List<String> vertexIds) {
            this.vertexIds = vertexIds;
        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "vertex";
        }

        public String getVertexIds() {
            if (vertexIds.size() > 1) {
                String result = "";
                for (String s : vertexIds) {
                    result += "'" + s + "',";
                }
                return result.substring(0, result.length() - 1);
            }
            return vertexIds.get(0);
        }

        @Override
        public String toString() {
            return "V(" + getVertexIds() + ")";
        }
    }

    public static class V extends StartTraversalOperation {

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "vertex";
        }

        @Override
        public String toString() {
            return "V()";
        }
    }

    public static class VAllIds extends StartTraversalOperation {

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "id";
        }

        @Override
        public String toString() {
            return "V().id()";
        }
    }

    public static class EWithIds extends StartTraversalOperation {
        private List<String> edgeIds;

        public EWithIds(List<String> edgeIds) {
            this.edgeIds = edgeIds;
        }

        public String getStartType() {
            return "edge";
        }

        public String getEndType() {
            return "edge";
        }

        public String getEdgeIds() {
            String result = "";
            for (String s : edgeIds) {
                result += "'" + s + "',";
            }
            return result.substring(0, result.length() - 1);
        }

        @Override
        public String toString() {
            return "E(" + getEdgeIds() + ")";
        }
    }

    public static class E extends StartTraversalOperation {

        public String getStartType() {
            return "edge";
        }

        public String getEndType() {
            return "edge";
        }

        @Override
        public String toString() {
            return "E()";
        }
    }

    public static AddV createAddV() {
        return new AddV();
    }

    public static AddVWithLabel createAddVWithLabel(String label) {
        return new AddVWithLabel(label);
    }

    public static AddVTraversal createAddVTraversal(Traversal traversal) {
        return new AddVTraversal(traversal);
    }

    public static AddEWithLabel createAddEWithLabel(String label) {
        return new AddEWithLabel(label);
    }

    public static AddETraversal createAddETraversal(Traversal traversal) {
        return new AddETraversal(traversal);
    }

    public static V createV() {
        return new V();
    }

    public static E createE() {
        return new E();
    }

    public static VWithIds createVWithIds(List<String> ids) {
        return new VWithIds(ids);
    }

    public static EWithIds createEWithIds(List<String> ids) {
        return new EWithIds(ids);
    }

    public static VAllIds createVAllId() {
        return new VAllIds();
    }

    public static AddEBetweenVertices createAddEBetweenVertices(String startId, String endId, String label) {
        return new AddEBetweenVertices(startId, endId, label);
    }

    public static AddVerticesTraversal createAddVertices(String vLabel, String vProperty, String vValue) {
        return new AddVerticesTraversal(vLabel, vProperty, vValue);
    }

    public static DropEBetweenVertices createDropEBetweenVertices(String startId, String endId) {
        return new DropEBetweenVertices(startId, endId);
    }

    public static DropVertices createDropVertices(String startId) {
        return new DropVertices(startId);
    }
}
