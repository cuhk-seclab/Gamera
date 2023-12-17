package org.gdbtesting.gremlin.ast;

import org.gdbtesting.gremlin.GraphSchema;

public class VertexLabelReference implements GraphExpression {

    private final GraphSchema.GraphVertexLabel label;

    public VertexLabelReference(GraphSchema.GraphVertexLabel label) {
        this.label = label;
    }

    public GraphSchema.GraphVertexLabel getLabel() {
        return label;
    }

}
