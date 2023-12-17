package org.gdbtesting.gremlin.gen;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.gdbtesting.Randomly;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GremlinPrint;

import java.util.ArrayList;
import java.util.List;

public class GraphDropGenerator {

    private GraphTraversalSource g;
    private GraphGlobalState state;
    private GremlinPrint print;

    public GraphDropGenerator(GraphGlobalState state) {
        this.state = state;
        this.g = state.getConnection().getG();
        this.print = new GremlinPrint();
    }

    public void dropAllEdge() {
        g.E().drop().iterate();
    }

    public void dropAllVertex() {
        g.V().drop().iterate();
    }

    public void dropVertexProperty(Vertex v, VertexProperty p) {
        g.V(v.id()).properties(p.key()).drop().iterate();
    }

    public void dropEdgeProperty(Edge e, Property p) {
        g.E(e.id()).properties(p.key()).drop().iterate();
    }

    public void dropVertex() {
        if (g.V().toList().size() == 0) return;
        System.out.println("Before drop: ");
        print.printVertices(state);
        g.V(Randomly.fromList(g.V().toList()).id()).drop().iterate();
        System.out.println("After drop: ");
        print.printVertices(state);
    }

    public void dropEdge() {
        if (g.E().toList().size() == 0) return;
        System.out.println("Before drop: ");
        print.printEdges(state);
        g.E(Randomly.fromList(g.E().toList()).id()).drop().iterate();
        System.out.println("After drop: ");
        print.printEdges(state);
    }

    public void dropMultiVertex() {
        List<Object> vertices = new ArrayList<>();
        List<Vertex> vL = Randomly.nonEmptySubList(g.V().toList());
        for (int i = 0; i < vL.size(); i++) {
            vertices.add(vL.get(i).id());
        }
        System.out.println("Before drop: ");
        print.printVertices(state);
        dropTraversal(vertices, "vertices");
        System.out.println("After drop: ");
        print.printVertices(state);
    }

    public void dropMultiEdge() {
        List<Object> edges = new ArrayList<>();
        List<Edge> eL = Randomly.nonEmptySubList(g.E().toList());
        for (int i = 0; i < eL.size(); i++) {
            edges.add(eL.get(i).id());
        }
        System.out.println("Before drop: ");
        print.printEdges(state);
        dropTraversal(edges, "edges");
        System.out.println("After drop: ");
        print.printEdges(state);
    }

    public void dropTraversal(List<Object> list, String type) {
        switch (type) {
            case "edges":
                g.E(list).as("edges").drop().iterate();
            case "vertices":
                g.V(list).as("vertices").drop().iterate();
        }
    }
}
