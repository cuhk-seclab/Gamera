package ch.cypherchecker.neo4j.schema;

import ch.cypherchecker.util.Randomization;

public enum Neo4JType {

    INTEGER,
    FLOAT,
    STRING,
    BOOLEAN,
    DURATION,
    DATE,
    LOCAL_TIME,
    POINT;

    public static Neo4JType getRandom() {
        return Randomization.fromOptions(Neo4JType.values());
    }

}
