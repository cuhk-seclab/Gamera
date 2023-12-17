package ch.cypherchecker.redis.gen;

import ch.cypherchecker.cypher.gen.CypherPropertyGenerator;
import ch.cypherchecker.cypher.ast.CypherExpression;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.redis.ast.RedisExpressionGenerator;
import ch.cypherchecker.redis.schema.RedisType;

import java.util.Collections;
import java.util.Map;

public class RedisPropertyGenerator extends CypherPropertyGenerator<RedisType> {

    protected RedisPropertyGenerator(Entity<RedisType> entity) {
        super(entity);
    }

    protected RedisPropertyGenerator(Entity<RedisType> entity, Map<String, Entity<RedisType>> variables) {
        super(entity, variables);
    }

    @Override
    protected CypherExpression generateConstant(RedisType type) {
        return RedisExpressionGenerator.generateConstant(type);
    }

    @Override
    protected CypherExpression generateExpression(Map<String, Entity<RedisType>> variables, RedisType type) {
        // Accessing properties of nodes created in the same query is not supported by redis
        // See: https://github.com/RedisGraph/RedisGraph/pull/1495
        return RedisExpressionGenerator.generateExpression(Collections.emptyMap(), type);
    }

}
