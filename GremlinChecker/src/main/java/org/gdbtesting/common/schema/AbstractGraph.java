package org.gdbtesting.common.schema;

import java.util.List;

public class AbstractGraph<V extends AbstractGraphVertex, E extends AbstractGraphEdge> {

    private final List<V> vertices;
    private final List<E> edges;

    public AbstractGraph(List<V> vertices, List<E> edges) {
        this.vertices = vertices;
        this.edges = edges;
    }

    public List<V> getVertices() {
        return vertices;
    }

    public List<E> getEdges() {
        return edges;
    }

}
