package ch.cypherchecker.memgraph.oracle;

import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.Oracle;
import ch.cypherchecker.common.OracleFactory;
import ch.cypherchecker.common.OracleType;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.memgraph.MemgraphConnection;
import ch.cypherchecker.memgraph.schema.MemgraphType;
import ch.cypherchecker.neo4j.oracle.Neo4JNodeOracle;
import ch.cypherchecker.neo4j.oracle.Neo4JPathOracle;

public class MemgraphOracleFactory implements OracleFactory<MemgraphConnection, MemgraphType> {

    @Override
    public Oracle createOracle(OracleType type, GlobalState<MemgraphConnection> state, Schema<MemgraphType> schema) {
        return switch (type) {
            case EMPTY_RESULT -> null;
            case NON_EMPTY_RESULT -> null;
            case PARTITION -> null;
            case PATH -> new MemgraphPathOracle(state, schema);
            case NODE -> new MemgraphNodeOracle(state, schema);
        };
    }
}
