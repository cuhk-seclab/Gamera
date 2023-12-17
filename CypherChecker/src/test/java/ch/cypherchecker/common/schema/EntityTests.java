package ch.cypherchecker.common.schema;

import ch.cypherchecker.neo4j.schema.Neo4JType;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class EntityTests {

    @Test
    void testGenerateRandomEntity() {
        Entity<Neo4JType> entity = Entity.generateRandomEntity(Set.of(Neo4JType.values()), new HashSet<>());
        Map<String, Neo4JType> availableProperties = entity.availableProperties();

        assertNotNull(entity);
        assertNotNull(availableProperties);
        assertFalse(availableProperties.isEmpty());
    }

    @Test
    void testGenerateRandomEntityNotSamePropertyName() {
        HashSet<String> names = new HashSet<>();
        Entity<Neo4JType> entity = Entity.generateRandomEntity(Set.of(Neo4JType.values()), names);
        Entity<Neo4JType> entity2 = Entity.generateRandomEntity(Set.of(Neo4JType.values()), names);

        for (String key : entity.availableProperties().keySet()) {
            if (entity2.availableProperties().containsKey(key)) {
                fail(String.format("The name %s is contained in two entities", key));
            }
        }
    }

}
