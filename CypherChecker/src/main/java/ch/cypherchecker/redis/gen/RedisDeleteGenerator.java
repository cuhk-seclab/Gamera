package ch.cypherchecker.redis.gen;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.cypher.ast.CypherVisitor;
import ch.cypherchecker.cypher.gen.CypherDeleteGenerator;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.redis.RedisQuery;
import ch.cypherchecker.redis.RedisUtil;
import ch.cypherchecker.redis.ast.RedisExpressionGenerator;
import ch.cypherchecker.redis.schema.RedisType;

import java.util.Map;

public class RedisDeleteGenerator extends CypherDeleteGenerator<RedisType> {

    public RedisDeleteGenerator(Schema<RedisType> schema) {
        super(schema);
    }

    public static RedisQuery deleteNodes(Schema<RedisType> schema) {
        RedisDeleteGenerator generator = new RedisDeleteGenerator(schema);
        generator.generateDelete();

        ExpectedErrors errors = new ExpectedErrors();

        RedisUtil.addFunctionErrors(errors);
        RedisUtil.addArithmeticErrors(errors);

        return new RedisQuery(generator.query.toString(), errors);
    }

    @Override
    protected String generateWhereClause(Entity<RedisType> entity) {
        return CypherVisitor.asString(RedisExpressionGenerator.generateExpression(Map.of("n", entity), RedisType.BOOLEAN));
    }

    @Override
    protected void onNonDetachDelete() {}

}