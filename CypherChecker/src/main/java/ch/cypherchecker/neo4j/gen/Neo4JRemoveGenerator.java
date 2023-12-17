package ch.cypherchecker.neo4j.gen;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.cypher.ast.CypherVisitor;
import ch.cypherchecker.cypher.gen.CypherRemoveGenerator;
import ch.cypherchecker.neo4j.Neo4JQuery;
import ch.cypherchecker.neo4j.Neo4JUtil;
import ch.cypherchecker.neo4j.ast.Neo4JExpressionGenerator;
import ch.cypherchecker.neo4j.schema.Neo4JType;

import java.util.Map;

public class Neo4JRemoveGenerator extends CypherRemoveGenerator<Neo4JType> {

    public Neo4JRemoveGenerator(Schema<Neo4JType> schema) {
        super(schema);
    }

    public static Neo4JQuery removeProperties(Schema<Neo4JType> schema) {
        Neo4JRemoveGenerator generator = new Neo4JRemoveGenerator(schema);
        generator.generateRemove();

        ExpectedErrors errors = new ExpectedErrors();
        Neo4JUtil.addRegexErrors(errors);
        Neo4JUtil.addArithmeticErrors(errors);
        Neo4JUtil.addFunctionErrors(errors);

        return new Neo4JQuery(generator.query.toString(), errors);
    }

    public static Neo4JQuery removeSimpleProperties(Schema<Neo4JType> schema) {
        Neo4JRemoveGenerator generator = new Neo4JRemoveGenerator(schema);
        generator.generateSimpleRemove();

        ExpectedErrors errors = new ExpectedErrors();
        Neo4JUtil.addRegexErrors(errors);
        Neo4JUtil.addArithmeticErrors(errors);
        Neo4JUtil.addFunctionErrors(errors);

        return new Neo4JQuery(generator.query.toString(), errors);
    }

    @Override
    protected String generateWhereClause(Entity<Neo4JType> entity) {
        return CypherVisitor.asString(Neo4JExpressionGenerator.generateExpression(Map.of("n", entity), Neo4JType.BOOLEAN));
    }

    @Override
    protected String generateRemoveClause(String property) {
        return String.format(" REMOVE n.%s", property);
    }

}
