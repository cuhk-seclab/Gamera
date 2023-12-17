package ch.cypherchecker.redis.gen;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.cypher.ast.CypherVisitor;
import ch.cypherchecker.cypher.gen.CypherRemoveGenerator;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.redis.RedisQuery;
import ch.cypherchecker.redis.RedisUtil;
import ch.cypherchecker.redis.ast.RedisExpressionGenerator;
import ch.cypherchecker.redis.schema.RedisType;

import java.util.Map;

public class RedisRemoveGenerator extends CypherRemoveGenerator<RedisType> {

    public RedisRemoveGenerator(Schema<RedisType> schema) {
        super(schema);
    }

    public static RedisQuery removeProperties(Schema<RedisType> schema) {
        RedisRemoveGenerator generator = new RedisRemoveGenerator(schema);
        generator.generateRemove();

        ExpectedErrors errors = new ExpectedErrors();
        RedisUtil.addFunctionErrors(errors);
        RedisUtil.addArithmeticErrors(errors);

        return new RedisQuery(generator.query.toString(), errors);
    }

    @Override
    protected String generateWhereClause(Entity<RedisType> entity) {
        return CypherVisitor.asString(RedisExpressionGenerator.generateExpression(Map.of("n", entity), RedisType.BOOLEAN));
    }

    /**
     * RedisGraph does not support REMOVE on a property.
     * Instead, we just set the property to NULL.
     */
    @Override
    protected String generateRemoveClause(String property) {
        return String.format(" SET n.%s = NULL", property);
    }


}
