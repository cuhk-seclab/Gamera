package ch.cypherchecker.memgraph.gen;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.cypher.ast.CypherVisitor;
import ch.cypherchecker.cypher.gen.CypherRemoveGenerator;
import ch.cypherchecker.memgraph.MemgraphQuery;
import ch.cypherchecker.memgraph.MemgraphUtil;
import ch.cypherchecker.memgraph.ast.MemgraphExpressionGenerator;
import ch.cypherchecker.memgraph.schema.MemgraphType;

import java.util.Map;

public class MemgraphRemoveGenerator extends CypherRemoveGenerator<MemgraphType> {

    public MemgraphRemoveGenerator(Schema<MemgraphType> schema) {
        super(schema);
    }

    public static MemgraphQuery removeProperties(Schema<MemgraphType> schema) {
        MemgraphRemoveGenerator generator = new MemgraphRemoveGenerator(schema);
        generator.generateRemove();

        ExpectedErrors errors = new ExpectedErrors();
        MemgraphUtil.addRegexErrors(errors);
        MemgraphUtil.addArithmeticErrors(errors);
        MemgraphUtil.addFunctionErrors(errors);

        return new MemgraphQuery(generator.query.toString(), errors);
    }

    public static MemgraphQuery removeSimpleProperties(Schema<MemgraphType> schema) {
        MemgraphRemoveGenerator generator = new MemgraphRemoveGenerator(schema);
        generator.generateSimpleRemove();

        ExpectedErrors errors = new ExpectedErrors();
        MemgraphUtil.addRegexErrors(errors);
        MemgraphUtil.addArithmeticErrors(errors);
        MemgraphUtil.addFunctionErrors(errors);

        return new MemgraphQuery(generator.query.toString(), errors);
    }

    @Override
    protected String generateWhereClause(Entity<MemgraphType> entity) {
        return CypherVisitor.asString(MemgraphExpressionGenerator.generateExpression(Map.of("n", entity), MemgraphType.BOOLEAN));
    }

    @Override
    protected String generateRemoveClause(String property) {
        return String.format(" REMOVE n.%s", property);
    }

}
