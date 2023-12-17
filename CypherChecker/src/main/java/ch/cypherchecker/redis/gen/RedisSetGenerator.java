package ch.cypherchecker.redis.gen;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.cypher.ast.CypherExpression;
import ch.cypherchecker.cypher.ast.CypherVisitor;
import ch.cypherchecker.cypher.gen.CypherSetGenerator;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.redis.RedisQuery;
import ch.cypherchecker.redis.RedisUtil;
import ch.cypherchecker.redis.ast.RedisExpressionGenerator;
import ch.cypherchecker.redis.schema.RedisType;

import java.util.Map;

public class RedisSetGenerator extends CypherSetGenerator<RedisType> {

    public RedisSetGenerator(Schema<RedisType> schema) {
        super(schema);
    }

    public static RedisQuery setProperties(Schema<RedisType> schema) {
        ExpectedErrors errors = new ExpectedErrors();

        RedisUtil.addFunctionErrors(errors);
        RedisUtil.addArithmeticErrors(errors);


        RedisSetGenerator generator = new RedisSetGenerator(schema);
        generator.generateSet();
        return new RedisQuery(generator.query.toString(), errors);
    }

    @Override
    protected String generateWhereClause(Entity<RedisType> entity) {
        return CypherVisitor.asString(RedisExpressionGenerator.generateExpression(Map.of("n", entity), RedisType.BOOLEAN));
    }

    @Override
    protected CypherExpression generateConstant(RedisType type) {
        return RedisExpressionGenerator.generateConstant(type);
    }

    @Override
    protected CypherExpression generateExpression(RedisType type) {
        return RedisExpressionGenerator.generateExpression(type);
    }

}
