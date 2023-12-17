package org.gdbtesting.common.schema;

public class AbstractGraphEdgeIndex {

    private final String edgeIndexName;

    protected AbstractGraphEdgeIndex(String indexName) {
        this.edgeIndexName = indexName;
    }

    public static AbstractGraphEdgeIndex create(String indexName) {
        return new AbstractGraphEdgeIndex(indexName);
    }

    public String getIndexName() {
        return edgeIndexName;
    }

    @Override
    public String toString() {
        return edgeIndexName;
    }
}
