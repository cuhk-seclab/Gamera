package ch.cypherchecker.redis.gen;

import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.redis.schema.RedisType;
import ch.cypherchecker.util.IgnoreMeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RedisCreateIndexGeneratorTests extends RedisSchemaGenerator {

    @Test
    void testCreateIndex() {
        while (true) {
            try {
                Schema<RedisType> schema = makeSchema();
                assertNotNull(RedisCreateIndexGenerator.createIndex(schema));
                break;
            } catch (IgnoreMeException ignored) {}
        }
    }

}
