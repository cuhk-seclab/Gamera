package ch.cypherchecker.neo4j.gen;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.cypher.gen.CypherCreateGenerator;
import ch.cypherchecker.cypher.gen.CypherPropertyGenerator;
import ch.cypherchecker.neo4j.Neo4JQuery;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.neo4j.Neo4JUtil;
import ch.cypherchecker.neo4j.schema.Neo4JType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Neo4JCreateGenerator extends CypherCreateGenerator<Neo4JType> {

    public Neo4JCreateGenerator(Schema<Neo4JType> schema) {
        super(schema);
    }

    @Override
    protected CypherPropertyGenerator<Neo4JType> getPropertyGenerator(Entity<Neo4JType> entity, Map<String, Entity<Neo4JType>> variables) {
        return new Neo4JPropertyGenerator(entity, variables);
    }

    public static Neo4JQuery createEntities(Schema<Neo4JType> schema) {
        Neo4JCreateGenerator generator = new Neo4JCreateGenerator(schema);

        ExpectedErrors errors = new ExpectedErrors();
        Neo4JUtil.addRegexErrors(errors);
        Neo4JUtil.addArithmeticErrors(errors);
        Neo4JUtil.addFunctionErrors(errors);

        // Create single query
        generator.generateCreate();
        return new Neo4JQuery(generator.query.toString(), errors);
    }

    public static Neo4JQuery createSimpleEntities(Schema<Neo4JType> schema) {
        Neo4JCreateGenerator generator = new Neo4JCreateGenerator(schema);

        ExpectedErrors errors = new ExpectedErrors();
        Neo4JUtil.addRegexErrors(errors);
        Neo4JUtil.addArithmeticErrors(errors);
        Neo4JUtil.addFunctionErrors(errors);

        // Create single query
        generator.generateCreateSimple();
        return new Neo4JQuery(generator.query.toString(), errors);
    }

    public static List<Neo4JQuery> createEntitiesList(Schema<Neo4JType> schema) {
        Neo4JCreateGenerator generator = new Neo4JCreateGenerator(schema);

        ExpectedErrors errors = new ExpectedErrors();
        Neo4JUtil.addRegexErrors(errors);
        Neo4JUtil.addArithmeticErrors(errors);
        Neo4JUtil.addFunctionErrors(errors);

        // Create single query
//        generator.generateCreate();
//        return new Neo4JQuery(generator.query.toString(), errors);

        // Create multiple queries
        List<String> queryList = new ArrayList<>();
        List<Neo4JQuery> neo4JQueryList = new ArrayList<>();
        queryList = generator.generateCustomizedCreate();
        for (String query : queryList) {
            neo4JQueryList.add(new Neo4JQuery(query, errors));
        }
        return neo4JQueryList;
    }
}
