package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;

import java.util.List;

public class NeighborTraversalOperation extends Traversal implements GraphExpression {

    public enum NeighborV {
        out("out"),
        in("in"),
        both("both"),
        outE("outE"),
        inE("inE"),
        bothE("bothE");

        private String neighbor;

        NeighborV(String neighbor) {
            this.neighbor = neighbor;
        }

        public String getNeighbor() {
            return neighbor;
        }
    }

    public enum NeighborVPath {
        out("out"),;
//        in("in"),
//        both("both");

        private String neighbor;

        NeighborVPath(String neighbor) {
            this.neighbor = neighbor;
        }

        public String getNeighbor() {
            return neighbor;
        }
    }

    public enum NeighborE {
        outV("outV"),
        inV("inV"),
        bothV("bothV"),
        //otherV("otherV") // in general, otherV often follows bothE
        ;

        private String neighbor;

        NeighborE(String neighbor) {
            this.neighbor = neighbor;
        }

        public String getNeighbor() {
            return neighbor;
        }
    }

    public NeighborV getRandomNeighborV() {
        return Randomly.fromOptions(NeighborV.values());
    }

    public NeighborE getRandomNeighborE() {
        return Randomly.fromOptions(NeighborE.values());
    }

    public static class Out extends NeighborTraversalOperation {
        private List<String> edgelabels;

        public String getEndType() {
            return "Vertex";
        }

        public Out(List<String> edgelabels) {
            this.edgelabels = edgelabels;
        }

        public String getEdgelabels() {
            if (this.edgelabels == null) {
                return "";
            }
            String result = "";
            for (String s : edgelabels) {
                result += "'" + s + "',";
            }
            return result.substring(0, result.length() - 1);
        }


        public String toString() {
            return "out(" + getEdgelabels() + ")";
        }

    }

    public static class In extends NeighborTraversalOperation {
        private List<String> edgelabels;

        public String getEndType() {
            return "Vertex";
        }

        public In(List<String> edgelabels) {
            this.edgelabels = edgelabels;
        }

        public String getEdgelabels() {
            String result = "";
            for (String s : edgelabels) {
                result += "'" + s + "',";
            }
            return result.substring(0, result.length() - 1);
        }


        public String toString() {
            return "in(" + getEdgelabels() + ")";
        }

    }

    public static class Both extends NeighborTraversalOperation {
        private List<String> edgelabels;

        public Both(List<String> edgelabels) {
            this.edgelabels = edgelabels;
        }

        public String getEndType() {
            return "Vertex";
        }

        public String getEdgelabels() {
            String result = "";
            for (String s : edgelabels) {
                result += "'" + s + "',";
            }
            return result.substring(0, result.length() - 1);
        }

        public String toString() {
            return "both(" + getEdgelabels() + ")";
        }

    }

    public static class InE extends NeighborTraversalOperation {
        private List<String> edgelabels;

        public InE(List<String> edgelabels) {
            this.edgelabels = edgelabels;
        }

        public String getEndType() {
            return "Edge";
        }

        public String getEdgelabels() {
            String result = "";
            for (String s : edgelabels) {
                result += "'" + s + "',";
            }
            return result.substring(0, result.length() - 1);
        }

        public String toString() {
            return "inE(" + getEdgelabels() + ")";
        }

    }

    public static class OutE extends NeighborTraversalOperation {
        private List<String> edgelabels;

        public OutE(List<String> edgelabels) {
            this.edgelabels = edgelabels;
        }

        public String getEndType() {
            return "Edge";
        }

        public String getEdgelabels() {
            String result = "";
            for (String s : edgelabels) {
                result += "'" + s + "',";
            }
            return result.substring(0, result.length() - 1);
        }


        public String toString() {
            return "outE(" + getEdgelabels() + ")";
        }

    }

    public static class BothE extends NeighborTraversalOperation {
        private List<String> edgelabels;

        public BothE(List<String> edgelabels) {
            this.edgelabels = edgelabels;
        }

        public String getEndType() {
            return "Edge";
        }

        public String getEdgelabels() {
            String result = "";
            for (String s : edgelabels) {
                result += "'" + s + "',";
            }
            return result.substring(0, result.length() - 1);
        }


        public String toString() {
            return "bothE(" + getEdgelabels() + ")";
        }

    }

    public static class InV extends NeighborTraversalOperation {

        public InV() {
        }

        public String getEndType() {
            return "Vertex";
        }

        public String getStartType() {
            return "Edge";
        }

        public String toString() {
            return "inV()";
        }

    }

    public static class OutV extends NeighborTraversalOperation {

        public OutV() {
        }

        public String getEndType() {
            return "Vertex";
        }

        public String getStartType() {
            return "Edge";
        }

        public String toString() {
            return "outV()";
        }

    }

    public static class BothV extends NeighborTraversalOperation {

        public BothV() {
        }

        public String getEndType() {
            return "Vertex";
        }

        public String getStartType() {
            return "Edge";
        }

        public String toString() {
            return "bothV()";
        }

    }

    public static class OtherV extends NeighborTraversalOperation {

        public OtherV() {
        }

        public String getEndType() {
            return "Vertex";
        }

        public String getStartType() {
            return "Edge";
        }

        public String toString() {
            return "otherV()";
        }

    }

    public static Out createOut(List<String> edgeLabels) {
        return new Out(edgeLabels);
    }

    public static In createIn(List<String> edgeLabels) {
        return new In(edgeLabels);
    }

    public static Both createBoth(List<String> edgeLabels) {
        return new Both(edgeLabels);
    }

    public static OutE createOutE(List<String> edgeLabels) {
        return new OutE(edgeLabels);
    }

    public static InE createInE(List<String> edgeLabels) {
        return new InE(edgeLabels);
    }

    public static BothE createBothE(List<String> edgeLabels) {
        return new BothE(edgeLabels);
    }

    public static OutV createOutV() {
        return new OutV();
    }

    public static InV createInV() {
        return new InV();
    }

    public static BothV createBothV() {
        return new BothV();
    }

    public static OtherV createOtherV() {
        return new OtherV();
    }


}
