package ch.cypherchecker.common;

import ch.cypherchecker.neo4j.Neo4JConnection;

public interface Generator<C extends Connection> {

    void generate(GlobalState<C> globalState);

    void generateSimple(GlobalState<C> globalState);

    void generateCustomized(GlobalState<C> globalState);
}
