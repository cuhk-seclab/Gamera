package org.gdbtesting.common.schema;

public class AbstractGraphVertexIndex {

    private final String vertexIndexName;

    protected AbstractGraphVertexIndex(String vertexIndexName) {
        this.vertexIndexName = vertexIndexName;
    }

    public String getIndexName() {
        return vertexIndexName;
    }

    @Override
    public String toString() {
        return vertexIndexName;
    }
}
