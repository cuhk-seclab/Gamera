package ch.cypherchecker.memgraph.schema;

import ch.cypherchecker.util.Randomization;

public enum MemgraphType {

    INTEGER,
    FLOAT,
    STRING,
    BOOLEAN,
    DURATION,
    DATE,
    LOCAL_TIME;

    public static MemgraphType getRandom() {
        return Randomization.fromOptions(MemgraphType.values());
    }
}
