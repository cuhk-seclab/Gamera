package ch.cypherchecker.neo4j.gen;

import ch.cypherchecker.common.Query;
import ch.cypherchecker.util.IgnoreMeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Neo4JCreateIndexGeneratorTests extends Neo4JSchemaGenerator {

    @Test
    void testCreateIndex() {
        while (true) {
            try {
                Query<?> query = Neo4JCreateIndexGenerator.createIndex(makeSchema());

                assertNotNull(query);
                assertTrue(query.getQuery().startsWith("CREATE"));
                break;
            } catch (IgnoreMeException ignored) {}
        }
    }

}
