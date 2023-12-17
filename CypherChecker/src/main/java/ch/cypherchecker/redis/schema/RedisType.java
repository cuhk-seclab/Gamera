package ch.cypherchecker.redis.schema;

import ch.cypherchecker.util.Randomization;

public enum RedisType {

    INTEGER,
    FLOAT,
    STRING,
    BOOLEAN,
    POINT;

    public static RedisType getRandom() {
        return Randomization.fromOptions(RedisType.values());
    }

}
