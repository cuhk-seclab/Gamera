package ch.cypherchecker.redis.oracle;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.Query;
import ch.cypherchecker.cypher.ast.CypherVisitor;
import ch.cypherchecker.cypher.oracle.CypherEmptyResultOracle;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.redis.RedisConnection;
import ch.cypherchecker.redis.RedisQuery;
import ch.cypherchecker.redis.RedisUtil;
import ch.cypherchecker.redis.ast.RedisExpressionGenerator;
import ch.cypherchecker.redis.schema.RedisType;

import java.util.Map;

public class RedisEmptyResultOracle extends CypherEmptyResultOracle<RedisConnection, RedisType> {

    public RedisEmptyResultOracle(GlobalState<RedisConnection> state, Schema<RedisType> schema) {
        super(state, schema);
    }

    @Override
    protected Query<RedisConnection> getIdQuery() {
        return new RedisQuery("MATCH (n) RETURN id(n)");
    }

    @Override
    protected Query<RedisConnection> getInitialQuery(String label, Entity<RedisType> entity) {
        ExpectedErrors errors = new ExpectedErrors();
        RedisUtil.addFunctionErrors(errors);
        RedisUtil.addArithmeticErrors(errors);

        String query = String.format("MATCH (n:%s) WHERE %s RETURN n",
                label,
                CypherVisitor.asString(RedisExpressionGenerator.generateExpression(Map.of("n", entity), RedisType.BOOLEAN)));
        return new RedisQuery(query, errors);
    }

    @Override
    protected Query<RedisConnection> getDeleteQuery(Long id) {
        return new RedisQuery(String.format("MATCH (n) WHERE id(n) = %d DETACH DELETE n", id));
    }

}
