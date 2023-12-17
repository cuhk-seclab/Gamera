package ch.cypherchecker.redis.gen;

import ch.cypherchecker.common.Query;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.redis.schema.RedisType;
import ch.cypherchecker.util.IgnoreMeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RedisDeleteGeneratorTests extends RedisSchemaGenerator {

    @Test
    void testDeleteNodes() {
        while (true) {
            try {
                Schema<RedisType> schema = makeSchema();
                Query<?> query = RedisDeleteGenerator.deleteNodes(schema);

                assertNotNull(query);
                assertTrue(query.getQuery().startsWith("MATCH "));
                assertTrue(query.getQuery().contains(" DELETE n"));
                break;
            } catch (IgnoreMeException ignored) {}
        }
    }

}
