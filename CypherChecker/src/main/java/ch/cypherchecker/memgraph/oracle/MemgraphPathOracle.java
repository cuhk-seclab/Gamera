package ch.cypherchecker.memgraph.oracle;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.Query;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.cypher.ast.CypherExpression;
import ch.cypherchecker.cypher.oracle.CypherPathOracle;
import ch.cypherchecker.memgraph.MemgraphConnection;
import ch.cypherchecker.memgraph.MemgraphQuery;
import ch.cypherchecker.memgraph.MemgraphUtil;
import ch.cypherchecker.memgraph.ast.MemgraphExpressionGenerator;
import ch.cypherchecker.memgraph.schema.MemgraphType;

import java.util.Map;

public class MemgraphPathOracle extends CypherPathOracle<MemgraphConnection, MemgraphType> {

    private final ExpectedErrors errors = new ExpectedErrors();

    public MemgraphPathOracle(GlobalState<MemgraphConnection> state, Schema<MemgraphType> schema) {
        super(state, schema);

        MemgraphUtil.addRegexErrors(errors);
        MemgraphUtil.addArithmeticErrors(errors);
        MemgraphUtil.addFunctionErrors(errors);
    }

    @Override
    protected CypherExpression getWhereClause(Entity<MemgraphType> entity) {
        return MemgraphExpressionGenerator.generateExpression(Map.of("n", entity), MemgraphType.BOOLEAN);
    }

    @Override
    protected Query<MemgraphConnection> makeQuery(String query) {
        return new MemgraphQuery(query, errors);
    }
}
