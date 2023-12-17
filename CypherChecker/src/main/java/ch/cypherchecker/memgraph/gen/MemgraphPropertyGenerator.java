package ch.cypherchecker.memgraph.gen;

import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.cypher.ast.CypherExpression;
import ch.cypherchecker.cypher.gen.CypherPropertyGenerator;
import ch.cypherchecker.memgraph.ast.MemgraphExpressionGenerator;
import ch.cypherchecker.memgraph.schema.MemgraphType;

import java.util.Map;

public class MemgraphPropertyGenerator extends CypherPropertyGenerator<MemgraphType> {

    protected MemgraphPropertyGenerator(Entity<MemgraphType> entity) {
        super(entity);
    }

    protected MemgraphPropertyGenerator(Entity<MemgraphType> entity, Map<String, Entity<MemgraphType>> variables) {
        super(entity, variables);
    }

    @Override
    protected CypherExpression generateConstant(MemgraphType type) {
        return MemgraphExpressionGenerator.generateConstant(type);
    }

    @Override
    protected CypherExpression generateExpression(Map<String, Entity<MemgraphType>> variables, MemgraphType type) {
        return MemgraphExpressionGenerator.generateExpression(variables, type);
    }
}
