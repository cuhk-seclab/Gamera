package ch.cypherchecker.neo4j.gen;

import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.neo4j.schema.Neo4JType;

import java.util.Set;

class Neo4JSchemaGenerator {

    Schema<Neo4JType> makeSchema() {
        return Schema.generateRandomSchema(Set.of(Neo4JType.values()));
    }

}
