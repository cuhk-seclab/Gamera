package ch.cypherchecker.neo4j.gen;

import ch.cypherchecker.common.Query;
import ch.cypherchecker.util.IgnoreMeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Neo4JDropIndexGeneratorTests extends Neo4JSchemaGenerator {

    @Test
    void testDropIndex() {
        while (true) {
            try {
                Query<?> query = Neo4JDropIndexGenerator.dropIndex(makeSchema());

                assertNotNull(query);
                assertTrue(query.getQuery().startsWith("DROP INDEX"));
                break;
            } catch (IgnoreMeException ignored) {}
        }
    }

}
