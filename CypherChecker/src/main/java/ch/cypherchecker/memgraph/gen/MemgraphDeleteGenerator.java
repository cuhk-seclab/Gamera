package ch.cypherchecker.memgraph.gen;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.cypher.ast.CypherVisitor;
import ch.cypherchecker.cypher.gen.CypherDeleteGenerator;
import ch.cypherchecker.memgraph.MemgraphQuery;
import ch.cypherchecker.memgraph.MemgraphUtil;
import ch.cypherchecker.memgraph.ast.MemgraphExpressionGenerator;
import ch.cypherchecker.memgraph.schema.MemgraphType;

import java.util.Map;

public class MemgraphDeleteGenerator extends CypherDeleteGenerator<MemgraphType> {

    private final ExpectedErrors errors = new ExpectedErrors();

    public MemgraphDeleteGenerator(Schema<MemgraphType> schema) {
        super(schema);
    }

    public static MemgraphQuery deleteNodes(Schema<MemgraphType> schema) {
        MemgraphDeleteGenerator generator = new MemgraphDeleteGenerator(schema);
        generator.generateDelete();

        MemgraphUtil.addRegexErrors(generator.errors);
        MemgraphUtil.addArithmeticErrors(generator.errors);
        MemgraphUtil.addFunctionErrors(generator.errors);

        return new MemgraphQuery(generator.query.toString(), generator.errors);
    }

    public static MemgraphQuery deleteSimpleNodes(Schema<MemgraphType> schema) {
        MemgraphDeleteGenerator generator = new MemgraphDeleteGenerator(schema);
        generator.generateSimpleDelete();

        MemgraphUtil.addRegexErrors(generator.errors);
        MemgraphUtil.addArithmeticErrors(generator.errors);
        MemgraphUtil.addFunctionErrors(generator.errors);

        return new MemgraphQuery(generator.query.toString(), generator.errors);
    }

    @Override
    protected String generateWhereClause(Entity<MemgraphType> entity) {
        return CypherVisitor.asString(MemgraphExpressionGenerator.generateExpression(Map.of("n", entity), MemgraphType.BOOLEAN));
    }

    @Override
    protected void onNonDetachDelete() {
        errors.addRegex("Cannot delete node<\\d+>, because it still has relationships. To delete this node, you must first delete its relationships.");
    }
}
