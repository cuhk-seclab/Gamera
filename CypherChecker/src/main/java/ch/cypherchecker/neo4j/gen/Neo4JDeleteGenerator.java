package ch.cypherchecker.neo4j.gen;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.cypher.ast.CypherVisitor;
import ch.cypherchecker.cypher.gen.CypherDeleteGenerator;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.neo4j.Neo4JQuery;
import ch.cypherchecker.neo4j.Neo4JUtil;
import ch.cypherchecker.neo4j.ast.Neo4JExpressionGenerator;
import ch.cypherchecker.neo4j.schema.Neo4JType;

import java.util.Map;

public class Neo4JDeleteGenerator extends CypherDeleteGenerator<Neo4JType> {

    private final ExpectedErrors errors = new ExpectedErrors();

    public Neo4JDeleteGenerator(Schema<Neo4JType> schema) {
        super(schema);
    }

    public static Neo4JQuery deleteNodes(Schema<Neo4JType> schema) {
        Neo4JDeleteGenerator generator = new Neo4JDeleteGenerator(schema);
        generator.generateDelete();

        Neo4JUtil.addRegexErrors(generator.errors);
        Neo4JUtil.addArithmeticErrors(generator.errors);
        Neo4JUtil.addFunctionErrors(generator.errors);

        return new Neo4JQuery(generator.query.toString(), generator.errors);
    }

    public static Neo4JQuery deleteSimpleNodes(Schema<Neo4JType> schema) {
        Neo4JDeleteGenerator generator = new Neo4JDeleteGenerator(schema);
        generator.generateSimpleDelete();

        Neo4JUtil.addRegexErrors(generator.errors);
        Neo4JUtil.addArithmeticErrors(generator.errors);
        Neo4JUtil.addFunctionErrors(generator.errors);

        return new Neo4JQuery(generator.query.toString(), generator.errors);
    }

    @Override
    protected String generateWhereClause(Entity<Neo4JType> entity) {
        return CypherVisitor.asString(Neo4JExpressionGenerator.generateExpression(Map.of("n", entity), Neo4JType.BOOLEAN));
    }

    @Override
    protected void onNonDetachDelete() {
        errors.addRegex("Cannot delete node<\\d+>, because it still has relationships. To delete this node, you must first delete its relationships.");
    }

}
