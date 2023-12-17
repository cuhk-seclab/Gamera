package ch.cypherchecker.neo4j;

import ch.cypherchecker.common.Generator;
import ch.cypherchecker.common.OracleFactory;
import ch.cypherchecker.common.Provider;
import ch.cypherchecker.common.QueryReplay;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.neo4j.oracle.Neo4JOracleFactory;
import ch.cypherchecker.neo4j.schema.Neo4JType;

import java.util.Set;

public class Neo4JProvider implements Provider<Neo4JConnection, Neo4JType> {

    @Override
    public Neo4JConnection getConnection() {
        return new Neo4JConnection();
    }

    @Override
    public Schema<Neo4JType> getSchema() {
        return Schema.generateRandomSchema(Set.of(Neo4JType.values()));
    }

    @Override
    public Generator<Neo4JConnection> getGenerator(Schema<Neo4JType> schema) {
        return new Neo4JGenerator(schema);
    }

    @Override
    public OracleFactory<Neo4JConnection, Neo4JType> getOracleFactory() {
        return new Neo4JOracleFactory();
    }

    @Override
    public QueryReplay getQueryReplay() {
        return new Neo4JQueryReplay();
    }

}
