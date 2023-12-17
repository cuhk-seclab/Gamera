package org.gdbtesting.common;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.gdbtesting.gremlin.GraphGlobalState;

public interface Print {

    public void printVertex(Vertex v);

    public void printEdge(Edge e);

    public void printEdgeProperty(Property p);

    public void printVertexProperty(Property p);

    public void printGraph(GraphGlobalState state);

    public void printVertices(GraphGlobalState state);

    public void printEdges(GraphGlobalState state);

    public void print(String s);

    public void printIndex(Object[] objects, String type);

}
