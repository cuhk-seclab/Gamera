package ch.cypherchecker.redis.oracle;

import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.Oracle;
import ch.cypherchecker.common.OracleFactory;
import ch.cypherchecker.common.OracleType;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.redis.RedisConnection;
import ch.cypherchecker.redis.schema.RedisType;

public class RedisOracleFactory implements OracleFactory<RedisConnection, RedisType> {

    @Override
    public Oracle createOracle(OracleType type, GlobalState<RedisConnection> state, Schema<RedisType> schema) {
        return switch (type) {
            case EMPTY_RESULT -> new RedisEmptyResultOracle(state, schema);
            case NON_EMPTY_RESULT -> new RedisNonEmptyResultOracle(state, schema);
            case PARTITION -> new RedisPartitionOracle(state, schema);
            case PATH -> new RedisPathOracle(state, schema);
            case NODE -> new RedisNodeOracle(state, schema);
            case EDGE -> new RedisEdgeOracle(state, schema);
        };
    }

}
