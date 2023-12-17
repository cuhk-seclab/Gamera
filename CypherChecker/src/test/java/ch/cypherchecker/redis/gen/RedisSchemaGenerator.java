package ch.cypherchecker.redis.gen;

import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.redis.schema.RedisType;

import java.util.Set;

public class RedisSchemaGenerator {

    Schema<RedisType> makeSchema() {
        return Schema.generateRandomSchema(Set.of(RedisType.values()));
    }

}
