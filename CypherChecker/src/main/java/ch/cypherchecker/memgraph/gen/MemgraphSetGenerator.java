package ch.cypherchecker.memgraph.gen;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.cypher.ast.CypherExpression;
import ch.cypherchecker.cypher.ast.CypherVisitor;
import ch.cypherchecker.cypher.gen.CypherSetGenerator;
import ch.cypherchecker.memgraph.MemgraphQuery;
import ch.cypherchecker.memgraph.MemgraphUtil;
import ch.cypherchecker.memgraph.ast.MemgraphExpressionGenerator;
import ch.cypherchecker.memgraph.schema.MemgraphType;

import java.util.Map;

public class MemgraphSetGenerator extends CypherSetGenerator<MemgraphType> {

    public MemgraphSetGenerator(Schema<MemgraphType> schema) {
        super(schema);
    }

    public static MemgraphQuery setProperties(Schema<MemgraphType> schema) {
        ExpectedErrors errors = new ExpectedErrors();

        MemgraphUtil.addRegexErrors(errors);
        MemgraphUtil.addArithmeticErrors(errors);
        MemgraphUtil.addFunctionErrors(errors);


        MemgraphSetGenerator generator = new MemgraphSetGenerator(schema);
        generator.generateSet();
        return new MemgraphQuery(generator.query.toString(), errors);
    }

    public static MemgraphQuery setSimpleProperties(Schema<MemgraphType> schema) {
        ExpectedErrors errors = new ExpectedErrors();

        MemgraphUtil.addRegexErrors(errors);
        MemgraphUtil.addArithmeticErrors(errors);
        MemgraphUtil.addFunctionErrors(errors);

        MemgraphSetGenerator generator = new MemgraphSetGenerator(schema);
        generator.generateSimpleSet();
        return new MemgraphQuery(generator.query.toString(), errors);
    }

    @Override
    protected String generateWhereClause(Entity<MemgraphType> entity) {
        return CypherVisitor.asString(MemgraphExpressionGenerator.generateExpression(Map.of("n", entity), MemgraphType.BOOLEAN));
    }

    @Override
    protected CypherExpression generateConstant(MemgraphType type) {
        return MemgraphExpressionGenerator.generateConstant(type);
    }

    // TODO: Use n as a variable here
    @Override
    protected CypherExpression generateExpression(MemgraphType type) {
        return MemgraphExpressionGenerator.generateExpression(type);
    }
}
