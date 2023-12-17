package ch.cypherchecker.neo4j.oracle;

import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.Oracle;
import ch.cypherchecker.common.OracleFactory;
import ch.cypherchecker.common.OracleType;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.neo4j.Neo4JConnection;
import ch.cypherchecker.neo4j.schema.Neo4JType;

public class Neo4JOracleFactory implements OracleFactory<Neo4JConnection, Neo4JType> {

    @Override
    public Oracle createOracle(OracleType type, GlobalState<Neo4JConnection> state, Schema<Neo4JType> schema) {
        return switch (type) {
            case EMPTY_RESULT -> new Neo4JEmptyResultOracle(state, schema);
            case NON_EMPTY_RESULT -> new Neo4JNonEmptyResultOracle(state, schema);
            case PARTITION -> new Neo4JPartitionOracle(state, schema);
            case PATH -> new Neo4JPathOracle(state, schema);
            case NODE -> new Neo4JNodeOracle(state, schema);
            case EDGE -> new Neo4JEdgeOracle(state, schema);
        };
    }

}
