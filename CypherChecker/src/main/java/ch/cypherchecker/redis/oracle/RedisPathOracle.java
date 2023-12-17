package ch.cypherchecker.redis.oracle;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.Query;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.cypher.ast.CypherExpression;
import ch.cypherchecker.cypher.oracle.CypherPathOracle;
import ch.cypherchecker.redis.RedisConnection;
import ch.cypherchecker.redis.RedisQuery;
import ch.cypherchecker.redis.RedisUtil;
import ch.cypherchecker.redis.ast.RedisExpressionGenerator;
import ch.cypherchecker.redis.schema.RedisType;

import java.util.Map;

public class RedisPathOracle extends CypherPathOracle<RedisConnection, RedisType> {

    private final ExpectedErrors errors = new ExpectedErrors();

    public RedisPathOracle(GlobalState<RedisConnection> state, Schema<RedisType> schema) {
        super(state, schema);

        RedisUtil.addFunctionErrors(errors);
        RedisUtil.addArithmeticErrors(errors);
    }

    @Override
    protected CypherExpression getWhereClause(Entity<RedisType> entity) {
        return RedisExpressionGenerator.generateExpression(Map.of("n", entity), RedisType.BOOLEAN);
    }

    @Override
    protected Query<RedisConnection> makeQuery(String query) {
        return new RedisQuery(query, errors);
    }
}
