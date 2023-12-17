package ch.cypherchecker.cypher.gen;

import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.neo4j.schema.Neo4JType;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CypherReturnGeneratorTests {

    @Test
    void testReturnEntities() {
        Entity<Neo4JType> entity = Entity.generateRandomEntity(Set.of(Neo4JType.values()), new HashSet<>());
        Map<String, Entity<Neo4JType>> entities = Map.of("n", entity);
        String query = CypherReturnGenerator.returnEntities(entities);

        assertNotNull(query);
        assertTrue(query.startsWith("RETURN"));
    }

}
