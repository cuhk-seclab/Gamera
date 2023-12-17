package ch.cypherchecker.cypher.oracle;

import ch.cypherchecker.common.Connection;
import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.Oracle;
import ch.cypherchecker.common.Query;
import ch.cypherchecker.cypher.ast.CypherExpression;
import ch.cypherchecker.cypher.ast.CypherPrefixOperation;
import ch.cypherchecker.cypher.ast.CypherVisitor;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.util.IgnoreMeException;

import java.util.List;
import java.util.Map;

public abstract class CypherPartitionOracle<C extends Connection, T> implements Oracle {

    private final GlobalState<C> state;
    private final Schema<T> schema;

    public CypherPartitionOracle(GlobalState<C> state, Schema<T> schema) {
        this.state = state;
        this.schema = schema;
    }

    @Override
    public void check() {
        int exceptions = 0;

        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);

        Query<C> initialQuery = makeQuery(String.format("MATCH (n:%s) RETURN COUNT(n)", label));
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        Long expectedTotal;

        if (result != null) {
            expectedTotal = (Long) result.get(0).get("COUNT(n)");
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        CypherExpression whereCondition = getWhereClause(entity);

        Query<C> firstQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN COUNT(n)", label, CypherVisitor.asString(whereCondition)));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery());
        Long first = 0L;

        if (result != null) {
            first = (Long) result.get(0).get("COUNT(n)");
        } else {
            exceptions++;
        }

        CypherExpression negatedWhereCondition = new CypherPrefixOperation(whereCondition, CypherPrefixOperation.PrefixOperator.NOT);
        Query<C> secondQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN COUNT(n)", label, CypherVisitor.asString(negatedWhereCondition)));

        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        Long second = 0L;

        if (result != null) {
            second = (Long) result.get(0).get("COUNT(n)");
        } else {
            exceptions++;
        }

        Query<C> thirdQuery = makeQuery(String.format("MATCH (n:%s) WHERE (%s) IS NULL RETURN COUNT(n)", label, CypherVisitor.asString(whereCondition)));
        result = thirdQuery.executeAndGet(state);
        System.out.println("4." + thirdQuery.getQuery());
        Long third = 0L;

        if (result != null) {
            third = (Long) result.get(0).get("COUNT(n)");
        } else {
            exceptions++;
        }

        if (exceptions > 0) {
            throw new IgnoreMeException();
        }

        if (first + second + third != expectedTotal) {
            throw new AssertionError(String.format("%d + %d + %d is not equal to %d", first, second, third, expectedTotal));
        }
    }

    protected abstract CypherExpression getWhereClause(Entity<T> entity);

    protected abstract Query<C> makeQuery(String query);

}
