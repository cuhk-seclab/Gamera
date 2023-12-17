package org.gdbtesting.gremlin;

public interface AbstractAction<G> {
    // TODO
    //Query<?> getQuery(G globalState) throws Exception;
    String getQuery(G globalState) throws Exception;
}
