package org.gdbtesting.gremlin.gen;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.gdbtesting.Randomly;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GremlinPrint;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GraphNeighborGenerator {

    private GraphGlobalState state;
    private GremlinPrint print = new GremlinPrint();

    public GraphNeighborGenerator(GraphGlobalState state) {
        this.state = state;
    }

    public GraphNeighborGenerator() {
    }

    private enum ChooseEdgeNeighbor {
        IN_V, // traversal the destination vertex of this edge
        OUT_V, // traversal the source vertex of this edge
        BOTH_V, // traversal both the source and destination vertex of this edge
        OTHER_V // traversal the other vertex of this edge
    }

    /**
     * Example:
     * a -el1-> b -el2-> c
     * for the vertex b,
     * "in(el1)" would get the vertex a
     * "out(el2)" would get the vertex c
     * "both()" would get the vertex a and c
     * "inE()" would get the edge [a-el1->b]
     * "outE()" would get the edge [b-el2->c]
     * "bothE()" would get the edge [a-el1->b] and [b-el2->c]
     */
    private enum ChooseVertexNeighbor {
        IN, // traversal the source vertex of this vertex
        OUT, // traversal the destination vertex of this vertex
        BOTH,  // traversal both the source and destination vertex of this vertex
        IN_E, // traversal the in edge of this vertex
        OUT_E, // traversal the out edge of this vertex
        BOTH_E, // traversal both the in and out edge of this vertex
    }

    public List<String> getNeighbors(List<Vertex> vertexList, Direction type) {
        List<String> list = new ArrayList<>();
        for (Vertex v : vertexList) {
            Iterator i = v.edges(type);
            while (i.hasNext()) {
                String str = i.next().toString().split("-")[1];
                if (list.contains(str)) {
                    continue;
                } else {
                    System.out.println(str);
                    list.add(str);
                }
            }
        }
        return list;
    }

    /**
     * Note:
     * inE().outV() ==> in()
     * inE().otherV() ==> in()
     * outE().inV() ==> out()
     * outE().otherV() ==> out()
     * bothE().otherV() ==> both()
     *
     * @return s: the new GraphTraversal
     */
    public GraphTraversal getNeighborWithVertex(GraphTraversal s, List<Vertex> vertexList) {
        List<String> list;
        switch (Randomly.fromOptions(ChooseVertexNeighbor.values())) {
            case IN:
                System.out.println("choose vertex neighbor with in()");
                list = getNeighbors(vertexList, Direction.IN);
                if (list.size() == 0) {
                    return s;
                }
                if (Randomly.getBoolean()) {
                    return s.in(Randomly.fromList(list));
                }
                return s.in();
            case OUT:
                System.out.println("choose vertex neighbor with out()");
                list = getNeighbors(vertexList, Direction.OUT);
                if (list.size() == 0) {
                    return s;
                }
                if (Randomly.getBoolean()) {
                    return s.out(Randomly.fromList(list));
                }
                return s.out();
            case BOTH:
                System.out.println("choose vertex neighbor with both()");
                list = getNeighbors(vertexList, Direction.BOTH);
                if (list.size() == 0) {
                    return s;
                }
                if (Randomly.getBoolean()) {
                    return s.both(Randomly.fromList(list));
                }
                return s.both();
            case IN_E: // return edges
                System.out.println("choose vertex neighbor with inE()");
                list = getNeighbors(vertexList, Direction.IN);
                if (list.size() == 0) {
                    return s;
                }
                if (Randomly.getBoolean()) {
                    return getNeighborWithEdge(s.inE());
                }
                return getNeighborWithEdge(s.inE(Randomly.fromList(list)));
            case OUT_E: // return edges
                System.out.println("choose vertex neighbor with outE()");
                list = getNeighbors(vertexList, Direction.OUT);
                if (list.size() == 0) {
                    return s;
                }
                if (Randomly.getBoolean()) {
                    return getNeighborWithEdge(s.outE());
                }
                return getNeighborWithEdge(s.outE(Randomly.fromList(list)));
            case BOTH_E: // return edges
                System.out.println("choose vertex neighbor with bothE()");
                list = getNeighbors(vertexList, Direction.BOTH);
                if (list.size() == 0) {
                    return s;
                }
                if (Randomly.getBoolean()) {
                    return getNeighborWithEdge(s.bothE());
                }
                return getNeighborWithEdge(s.bothE(Randomly.fromList(list)));
        }
        return s;
    }

    public GraphTraversal getNeighborWithEdge(GraphTraversal s) {
        switch (Randomly.fromOptions(ChooseEdgeNeighbor.values())) {
            case IN_V:
                return s.inV();
            case OUT_V:
                return s.outV();
            case OTHER_V:
                return s.otherV();
            case BOTH_V:
                return s.bothV();
        }
        return s.bothV();
    }
}
