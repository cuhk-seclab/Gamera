package org.gdbtesting.gremlin;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.gdbtesting.common.Print;

import java.util.Iterator;
import java.util.List;

/**
 * Print the detailed information about the graphs. Print out on the console.
 */
public class GremlinPrint implements Print {
    @Override
    public void printVertex(Vertex v) {
        System.out.println("v[" + v.id() + "] " + "\tlabel:" + v.label());
        Iterator<VertexProperty<Object>> i = v.properties();
        while (i.hasNext()) {
            printVertexProperty(i.next());
        }
        System.out.println();
    }

    public void printVertexList(List<Vertex> vertexList) {
        for (Vertex v : vertexList) {
            printVertex(v);
        }
    }

    @Override
    public void printEdge(Edge e) {
        StringBuilder s = new StringBuilder();
        s.append("e[" + e.id() + "]");
        s.append("[").append(e.outVertex().id()).append("-").append(e.label())
                .append("->").append(e.inVertex().id()).append("]").append("\n");
        System.out.print(s.toString());
        Iterator<Property<Object>> i = e.properties();
        while (i.hasNext()) {
            printEdgeProperty(i.next());
        }
        System.out.println();
    }

    public void printProperty(Property p, String type) {
        if (type.equals("vertex")) {
            printVertexProperty(p);
        } else {
            printEdgeProperty(p);
        }
    }


    @Override
    public void printEdgeProperty(Property p) {
        if (p == null) {
            return;
        }
        StringBuilder s = new StringBuilder();
        s.append("ep[").append(p.key()).append("->").append(p.value()).append("]");
        print(s.toString());
    }

    @Override
    public void printVertexProperty(Property p) {
        if (p == null) {
            return;
        }
        StringBuilder s = new StringBuilder();
        s.append("vp[").append(p.key()).append("->").append(p.value()).append("]");
        print(s.toString());
    }

    @Override
    public void printGraph(GraphGlobalState state) {
        printVertices(state);
        printEdges(state);
    }

    @Override
    public void printVertices(GraphGlobalState state) {
        StringBuilder s = new StringBuilder();
        List<Vertex> vL = state.getConnection().getG().V().toList();
        s.append("vertex: " + vL.size() + "\n");
        for (Vertex v : vL) {
            s.append("v[").append(v.id()).append("]").append("\t");
        }
        s.append("\n");
        print(s.toString());
    }

    @Override
    public void printEdges(GraphGlobalState state) {
        StringBuilder s = new StringBuilder();
        List<Edge> eL = state.getConnection().getG().E().toList();
        s.append("edges: " + eL.size() + "\n");
        for (Edge e : eL) {
            s.append("e[").append(e.id()).append("]").append("\t");
        }
        s.append("\n");
        print(s.toString());
    }

    @Override
    public void print(String s) {
        System.out.println(s);
    }

    @Override
    public void printIndex(Object[] objects, String type) {
        StringBuilder s = new StringBuilder();
        s.append(type).append(" index: \n");
        for (Object o : objects) {
            s.append(o.toString()).append("\t");
        }
        s.append("\n");
        print(s.toString());
    }

}
