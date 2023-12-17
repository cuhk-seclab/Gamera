package org.gdbtesting.gremlin.gen;

import org.gdbtesting.gremlin.GraphGlobalState;

public abstract class GraphDropIndexGenerator {

    public GraphGlobalState state;

    public GraphDropIndexGenerator(GraphGlobalState state) {
        this.state = state;
    }

    public abstract void dropIndex(String type);
}
