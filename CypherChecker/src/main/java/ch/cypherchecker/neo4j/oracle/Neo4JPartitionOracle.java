package ch.cypherchecker.neo4j.oracle;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.Query;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.cypher.ast.CypherExpression;
import ch.cypherchecker.cypher.oracle.CypherPartitionOracle;
import ch.cypherchecker.neo4j.Neo4JBugs;
import ch.cypherchecker.neo4j.Neo4JConnection;
import ch.cypherchecker.neo4j.Neo4JQuery;
import ch.cypherchecker.neo4j.Neo4JUtil;
import ch.cypherchecker.neo4j.ast.Neo4JExpressionGenerator;
import ch.cypherchecker.neo4j.schema.Neo4JType;

import java.util.Map;

public class Neo4JPartitionOracle extends CypherPartitionOracle<Neo4JConnection, Neo4JType> {

    private final ExpectedErrors errors = new ExpectedErrors();

    public Neo4JPartitionOracle(GlobalState<Neo4JConnection> state, Schema<Neo4JType> schema) {
        super(state, schema);

        Neo4JUtil.addRegexErrors(errors);
        Neo4JUtil.addArithmeticErrors(errors);
        Neo4JUtil.addFunctionErrors(errors);
    }

    @Override
    protected CypherExpression getWhereClause(Entity<Neo4JType> entity) {
        return Neo4JExpressionGenerator.generateExpression(Map.of("n", entity), Neo4JType.BOOLEAN);
    }

    @Override
    protected Query<Neo4JConnection> makeQuery(String query) {
        return new Neo4JQuery(query, errors);
    }

    @Override
    public void onStart() {
        Neo4JBugs.PartitionOracleSpecific.enableAll();
    }

    @Override
    public void onComplete() {
        Neo4JBugs.PartitionOracleSpecific.disableAll();
    }

}
