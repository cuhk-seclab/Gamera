package ch.cypherchecker.redis;

import ch.cypherchecker.common.Generator;
import ch.cypherchecker.common.OracleFactory;
import ch.cypherchecker.common.Provider;
import ch.cypherchecker.common.QueryReplay;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.redis.oracle.RedisOracleFactory;
import ch.cypherchecker.redis.schema.RedisType;

import java.util.Set;

public class RedisProvider implements Provider<RedisConnection, RedisType> {

    @Override
    public RedisConnection getConnection() {
        return new RedisConnection();
    }

    @Override
    public Schema<RedisType> getSchema() {
        return Schema.generateRandomSchema(Set.of(RedisType.values()));
    }

    @Override
    public Generator<RedisConnection> getGenerator(Schema<RedisType> schema) {
        return new RedisGenerator(schema);
    }

    @Override
    public OracleFactory<RedisConnection, RedisType> getOracleFactory() {
        return new RedisOracleFactory();
    }

    @Override
    public QueryReplay getQueryReplay() {
        return new RedisQueryReplay();
    }
}
