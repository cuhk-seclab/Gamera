package ch.cypherchecker.memgraph;

import ch.cypherchecker.common.Generator;
import ch.cypherchecker.common.OracleFactory;
import ch.cypherchecker.common.Provider;
import ch.cypherchecker.common.QueryReplay;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.memgraph.oracle.MemgraphOracleFactory;
import ch.cypherchecker.memgraph.schema.MemgraphType;

import java.util.Set;

public class MemgraphProvider implements Provider<MemgraphConnection, MemgraphType> {

    @Override
    public MemgraphConnection getConnection() {
        return new MemgraphConnection();
    }

    @Override
    public Schema<MemgraphType> getSchema() {
        return Schema.generateRandomSchema(Set.of(MemgraphType.values()));
    }

    @Override
    public Generator<MemgraphConnection> getGenerator(Schema<MemgraphType> schema) {
        return new MemgraphGenerator(schema);
    }

    @Override
    public OracleFactory<MemgraphConnection, MemgraphType> getOracleFactory() {
        return new MemgraphOracleFactory();
    }

    @Override
    public QueryReplay getQueryReplay() {
        return null;
    }
}
