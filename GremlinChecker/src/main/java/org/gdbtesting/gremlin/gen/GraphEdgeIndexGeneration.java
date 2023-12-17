package org.gdbtesting.gremlin.gen;

import org.gdbtesting.gremlin.GraphGlobalState;

public abstract class GraphEdgeIndexGeneration {

    protected GraphGlobalState state;

    public GraphEdgeIndexGeneration(GraphGlobalState state) {
        this.state = state;
    }

    // Here need certain graph
    public abstract void generateEdgeIndex();
}
