package ch.cypherchecker.neo4j.gen;

import ch.cypherchecker.common.Query;
import ch.cypherchecker.util.IgnoreMeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Neo4JDeleteGeneratorTests extends Neo4JSchemaGenerator {

    @Test
    void testDeleteNodes() {
        while (true) {
            try {
                Query<?> query = Neo4JDeleteGenerator.deleteNodes(makeSchema());

                assertNotNull(query);
                assertTrue(query.getQuery().startsWith("MATCH "));
                assertTrue(query.getQuery().contains(" DELETE n"));
                break;
            } catch (IgnoreMeException ignored) {}
        }
    }

}
