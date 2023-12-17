package ch.cypherchecker.redis.ast;

import ch.cypherchecker.cypher.ast.CypherConstant;

public class RedisPointConstant extends CypherConstant {

    private final double longitude;
    private final double latitude;

    public RedisPointConstant(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    @Override
    public String getTextRepresentation() {
        return String.format("point({ longitude: %f, latitude: %f })", longitude, latitude);
    }
}
