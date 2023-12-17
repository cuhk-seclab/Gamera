package ch.cypherchecker.memgraph.oracle;

import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.Query;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.cypher.ast.CypherExpression;
import ch.cypherchecker.cypher.oracle.CypherNodeOracle;
import ch.cypherchecker.memgraph.MemgraphConnection;
import ch.cypherchecker.memgraph.schema.MemgraphType;

public class MemgraphNodeOracle extends CypherNodeOracle<MemgraphConnection, MemgraphType> {

    public MemgraphNodeOracle(GlobalState<MemgraphConnection> state, Schema<MemgraphType> schema) {
        super(state, schema);
    }

    @Override
    protected CypherExpression getWhereClause(Entity<MemgraphType> entity) {
        return null;
    }

    @Override
    protected Query<MemgraphConnection> makeQuery(String query) {
        return null;
    }
}
