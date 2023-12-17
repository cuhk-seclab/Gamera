package org.gdbtesting.gremlin.gen;

import org.gdbtesting.gremlin.GraphGlobalState;


public abstract class GraphVertexIndexGeneration {

    protected GraphGlobalState state;

    public GraphVertexIndexGeneration(GraphGlobalState state) {
        this.state = state;
    }

    // Here need certain graph
    public abstract void generateVertexIndex();
}
