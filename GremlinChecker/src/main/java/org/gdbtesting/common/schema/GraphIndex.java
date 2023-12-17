package org.gdbtesting.common.schema;

public class GraphIndex {

    private final String indexName;

    protected GraphIndex(String indexName) {
        this.indexName = indexName;
    }

    public static GraphIndex create(String indexName) {
        return new GraphIndex(indexName);
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public String toString() {
        return indexName;
    }
}
