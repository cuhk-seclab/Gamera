package ch.cypherchecker.redis.gen;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.cypher.gen.CypherCreateGenerator;
import ch.cypherchecker.cypher.gen.CypherPropertyGenerator;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.neo4j.Neo4JQuery;
import ch.cypherchecker.neo4j.Neo4JUtil;
import ch.cypherchecker.neo4j.gen.Neo4JCreateGenerator;
import ch.cypherchecker.neo4j.schema.Neo4JType;
import ch.cypherchecker.redis.RedisQuery;
import ch.cypherchecker.redis.RedisUtil;
import ch.cypherchecker.redis.schema.RedisType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RedisCreateGenerator extends CypherCreateGenerator<RedisType> {

    public RedisCreateGenerator(Schema<RedisType> schema) {
        super(schema);
    }

    @Override
    protected CypherPropertyGenerator<RedisType> getPropertyGenerator(Entity<RedisType> entity, Map<String, Entity<RedisType>> variables) {
        return new RedisPropertyGenerator(entity, variables);
    }

    public static RedisQuery createEntities(Schema<RedisType> schema) {
        RedisCreateGenerator generator = new RedisCreateGenerator(schema);
        generator.generateCreate();

        ExpectedErrors errors = new ExpectedErrors();
        RedisUtil.addFunctionErrors(errors);
        RedisUtil.addArithmeticErrors(errors);

        return new RedisQuery(generator.query.toString(), errors);
    }

    public static List<RedisQuery> createEntitiesList(Schema<RedisType> schema) {
        RedisCreateGenerator generator = new RedisCreateGenerator(schema);

        ExpectedErrors errors = new ExpectedErrors();
        RedisUtil.addFunctionErrors(errors);
        RedisUtil.addArithmeticErrors(errors);

        // Create multiple queries
        List<String> queryList = new ArrayList<>();
        List<RedisQuery> redisQueryList = new ArrayList<>();
        queryList = generator.generateCustomizedCreate();
        for (String query : queryList) {
            redisQueryList.add(new RedisQuery(query, errors));
        }
        return redisQueryList;
    }

}
