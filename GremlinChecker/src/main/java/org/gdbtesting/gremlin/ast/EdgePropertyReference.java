package org.gdbtesting.gremlin.ast;

import org.gdbtesting.gremlin.GraphSchema;

public class EdgePropertyReference extends Traversal {
    private final GraphSchema.GraphEdgeProperty property;

    private final GraphConstant value;

    public EdgePropertyReference(GraphSchema.GraphEdgeProperty property, GraphConstant value) {
        this.property = property;
        this.value = value;
    }

    public static EdgePropertyReference create(GraphSchema.GraphEdgeProperty property, GraphConstant value) {
        return new EdgePropertyReference(property, value);
    }

    public GraphSchema.GraphEdgeProperty getProperty() {
        return property;
    }

    public GraphConstant getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "values('" + property.getEdgePropertyName() + "')";
    }
}
