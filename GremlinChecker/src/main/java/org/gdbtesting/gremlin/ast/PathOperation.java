package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;

public class PathOperation extends Traversal {

    public enum Path {

        path("path"),
//        shortestPath("shortestPath"),
//        cyclicPath("cyclicPath"),
//        simplePath("simplePath"),
        path_size("path_size"),
        path_count("path_count");
//        path_objects("path_objects"),
//        path_labels("path_labels");

        private String pathop;

        Path(String path) {
            this.pathop = path;
        }

        public String getPath() {
            return pathop;
        }
    }

    public static Path getRandomPath() {
        return Randomly.fromOptions(Path.values());
    }

    public static class PathOp extends PathOperation {
        public PathOp() {

        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "path";
        }

        @Override
        public String toString() {
            return "path()";
        }
    }

    public static class ShortestPath extends PathOperation {
        public ShortestPath() {

        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "path";
        }

        @Override
        public String toString() {
            return "shortestPath()";
        }
    }

    public static class CyclicPath extends PathOperation {
        public CyclicPath() {

        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "path";
        }

        @Override
        public String toString() {
            return "cyclicPath()";
        }
    }

    public static class SimplePath extends PathOperation {
        public SimplePath() {

        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "path";
        }

        @Override
        public String toString() {
            return "simplePath()";
        }
    }

    public static class PathSize extends PathOperation {
        public PathSize() {

        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "path";
        }

        @Override
        public String toString() {
            return "path().size()";
        }
    }

    public static class PathCount extends PathOperation {
        public PathCount() {

        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "path";
        }

        @Override
        public String toString() {
            return "path().count()";
        }
    }

    public static class PathObjects extends PathOperation {
        public PathObjects() {

        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "path";
        }

        @Override
        public String toString() {
            return "path().objects()";
        }
    }

    public static class PathLabels extends PathOperation {
        public PathLabels() {

        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "path";
        }

        @Override
        public String toString() {
            return "path().labels()";
        }
    }

    public static class PathCheck extends PathOperation {
        private String endId;

        public PathCheck(String endId) {
            this.endId = endId;
        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "path";
        }

        public String getEndId() {
            if (this.endId == null) {
                return "";
            }
            return this.endId;
        }

        @Override
        public String toString() {
//            return "repeat(out().simplePath()).until(id().is(" + getEndId() + ")).path().size()";
            return "repeat(out().simplePath()).until(hasId(" + getEndId() + ")).path().size()";
        }
    }

    public static class PathHopCheck extends PathOperation {
        private String id1;
        private String id2;
        private String id3;

        public PathHopCheck(String id1, String id2, String id3) {
            this.id1 = id1;
            this.id2 = id2;
            this.id3 = id3;
        }

        public String getStartType() {
            return "";
        }

        public String getEndType() {
            return "";
        }

        @Override
        public String toString() {
            return "V(" + id1 + ").repeat(out().simplePath()).until(hasId(" + id2 + "))." +
                    "repeat(out()).until(hasId(" + id3 + ")).path().size()";
        }
    }

    public static class OneHopCheck extends PathOperation {
        private String startId;
        private String endId;

        public OneHopCheck(String startId, String endId) {
            this.startId = startId;
            this.endId = endId;
        }

        public String getStartType() {
            return "";
        }

        public String getEndType() {
            return "";
        }

        @Override
        public String toString() {
            return "V(" + startId + ").out().hasId(" + endId + ").path().size()";
        }
    }

    public static class KHopNodes extends PathOperation {
        private String startId;
        private String k;

        public KHopNodes(String startId, String k) {
            this.startId = startId;
            this.k = k;
        }

        public String getStartType() {
            return "";
        }

        public String getEndType() {
            return "";
        }

        @Override
        public String toString() {
            return "V(" + startId + ").repeat(outE().inV().simplePath()).times(" + k + ")";
        }
    }

    public static class KHopNodesReverse extends PathOperation {
        private String startId;
        private String k;

        public KHopNodesReverse(String startId, String k) {
            this.startId = startId;
            this.k = k;
        }

        public String getStartType() {
            return "";
        }

        public String getEndType() {
            return "";
        }

        @Override
        public String toString() {
            return "V(" + startId + ").repeat(inE().outV().simplePath()).times(" + k + ")";
        }
    }

    public static class VSpouse extends PathOperation {
        private String startId;

        public VSpouse(String startId) {
            this.startId = startId;
        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "vertex";
        }

        @Override
        public String toString() {
            return "V(" + startId + ").outE().inV().inE().outV().dedup()";
        }
    }

    public static class VSpouseNoDedup extends PathOperation {
        private String startId;

        public VSpouseNoDedup(String startId) {
            this.startId = startId;
        }

        public String getStartType() {
            return "vertex";
        }

        public String getEndType() {
            return "vertex";
        }

        @Override
        public String toString() {
            return "V(" + startId + ").outE().inV().inE().outV().dedup()";
        }
    }

    public static class VDescendant extends PathOperation {
        private String startId;

        public VDescendant(String startId) {
            this.startId = startId;
        }

        public String getStartType() {
            return "";
        }

        public String getEndType() {
            return "";
        }

        @Override
        public String toString() {
            return "V(" + startId + ").repeat(outE().inV().simplePath()).until(outE().count().is(0)).path()";
        }
    }

    public static class VDescendantDedup extends PathOperation {
        private String startId;

        public VDescendantDedup(String startId) {
            this.startId = startId;
        }

        public String getStartType() {
            return "";
        }

        public String getEndType() {
            return "";
        }

        @Override
        public String toString() {
            return "V(" + startId + ").repeat(outE().inV().simplePath()).until(outE().count().is(0)).dedup()";
        }
    }

    public static class VAncestor extends PathOperation {
        private String startId;

        public VAncestor(String startId) {
            this.startId = startId;
        }

        public String getStartType() {
            return "";
        }

        public String getEndType() {
            return "";
        }

        @Override
        public String toString() {
            return "V(" + startId + ").repeat(inE().outV().simplePath()).until(inE().count().is(0)).path()";
        }
    }

    public static class VAllPaths extends PathOperation {
        private String startId;
        private String endId;

        public VAllPaths(String startId, String endId) {
            this.startId = startId;
            this.endId = endId;
        }

        public String getStartType() {
            return "";
        }

        public String getEndType() {
            return "";
        }

        @Override
        public String toString() {
            return "V(" + startId + ").repeat(out().simplePath()).until(hasId(" + endId + ")).path()";
        }
    }

    public static PathOp createPath() {
        return new PathOp();
    }

    public static ShortestPath createShortestPath() {
        return new ShortestPath();
    }

    public static CyclicPath createCyclicPath() {
        return new CyclicPath();
    }

    public static SimplePath createSimplePath() {
        return new SimplePath();
    }

    public static PathSize createPathSize() {
        return new PathSize();
    }

    public static PathCount createPathCount() {
        return new PathCount();
    }

    public static PathObjects createPathObjects() {
        return new PathObjects();
    }

    public static PathLabels createPathLabels() {
        return new PathLabels();
    }

    public static PathCheck createPathCheck(String endId) {
        return new PathCheck(endId);
    }

    public static PathHopCheck createPathHopCheck(String id1, String id2, String id3) {
        return new PathHopCheck(id1, id2, id3);
    }

    public static OneHopCheck createOneHopCheck(String startId, String endId) {
        return new OneHopCheck(startId, endId);
    }

    public static KHopNodes createKHopNodes(String startId, String k) {
        return new KHopNodes(startId, k);
    }

    public static KHopNodesReverse createKHopNodesReverse(String startId, String k) {
        return new KHopNodesReverse(startId, k);
    }

    public static VSpouse createVSpouse(String startId) {
        return new VSpouse(startId);
    }

    public static VSpouseNoDedup createVSpouseNoDedup(String startId) {
        return new VSpouseNoDedup(startId);
    }

    public static VDescendant createVDescendant(String startId) {
        return new VDescendant(startId);
    }

    public static VDescendantDedup createVDescendantDedup(String startId) {
        return new VDescendantDedup(startId);
    }

    public static VAncestor createVAncestor(String startId) {
        return new VAncestor(startId);
    }

    public static VAllPaths createVAllPaths(String startId, String endId) {
        return new VAllPaths(startId, endId);
    }
}
