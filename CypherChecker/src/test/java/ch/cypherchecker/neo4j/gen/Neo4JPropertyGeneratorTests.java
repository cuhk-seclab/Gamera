package ch.cypherchecker.neo4j.gen;

import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.neo4j.schema.Neo4JType;
import ch.cypherchecker.util.IgnoreMeException;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class Neo4JPropertyGeneratorTests extends Neo4JSchemaGenerator {

    @Test
    void testGenerateProperties() {
        while (true) {
            try {
                Entity<Neo4JType> entity = Entity.generateRandomEntity(Set.of(Neo4JType.values()), new HashSet<>());

                String query = new Neo4JPropertyGenerator(entity).generateProperties();
                assertNotNull(query);

                if (!query.isEmpty()) {
                    assertTrue(query.startsWith("{"));
                    assertTrue(query.endsWith("}"));
                }

                break;
            } catch (IgnoreMeException ignored) {}
        }
    }

}
