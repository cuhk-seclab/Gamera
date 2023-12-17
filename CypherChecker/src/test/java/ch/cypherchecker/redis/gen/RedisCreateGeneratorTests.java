package ch.cypherchecker.redis.gen;

import ch.cypherchecker.common.Query;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.redis.schema.RedisType;
import ch.cypherchecker.util.IgnoreMeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RedisCreateGeneratorTests extends RedisSchemaGenerator {

    @Test
    void testCreateEntities() {
        while (true) {
            try {
                Schema<RedisType> schema = makeSchema();
                Query<?> query = RedisCreateGenerator.createEntities(schema);

                assertNotNull(query);
                assertTrue(query.getQuery().startsWith("CREATE"));
                break;
            } catch (IgnoreMeException ignored) {}
        }
    }

}
