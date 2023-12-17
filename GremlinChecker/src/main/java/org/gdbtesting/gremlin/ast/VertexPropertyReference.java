package org.gdbtesting.gremlin.ast;

import org.gdbtesting.gremlin.GraphSchema;

public class VertexPropertyReference extends Traversal {

    private final GraphSchema.GraphVertexProperty property;

    private final GraphConstant value;

    private String type;

    public VertexPropertyReference(GraphSchema.GraphVertexProperty property, GraphConstant value) {
        this.property = property;
        this.value = value;
    }

    public static VertexPropertyReference create(GraphSchema.GraphVertexProperty property, GraphConstant value) {
        return new VertexPropertyReference(property, value);
    }

    public GraphSchema.GraphVertexProperty getProperty() {
        return property;
    }

    public GraphConstant getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "values('" + property.getVertexPropertyName() + "')";
    }


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
