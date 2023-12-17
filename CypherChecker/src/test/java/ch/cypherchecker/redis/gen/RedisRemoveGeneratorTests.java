package ch.cypherchecker.redis.gen;

import ch.cypherchecker.common.Query;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.redis.schema.RedisType;
import ch.cypherchecker.util.IgnoreMeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RedisRemoveGeneratorTests extends RedisSchemaGenerator {

    @Test
    void testRemoveProperties() {
        while (true) {
            try {
                Schema<RedisType> schema = makeSchema();
                Query<?> query = RedisRemoveGenerator.removeProperties(schema);

                assertNotNull(query);
                assertTrue(query.getQuery().startsWith("MATCH "));
                assertTrue(query.getQuery().contains(" SET n."));
                assertTrue(query.getQuery().contains(" = NULL"));
                break;
            } catch (IgnoreMeException ignored) {}
        }
    }

}
