package ch.cypherchecker.cypher.oracle;

import ch.cypherchecker.common.Connection;
import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.Oracle;
import ch.cypherchecker.common.Query;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.cypher.ast.CypherExpression;
import ch.cypherchecker.cypher.ast.CypherPrefixOperation;
import ch.cypherchecker.cypher.ast.CypherVisitor;
import ch.cypherchecker.util.IgnoreMeException;
import ch.cypherchecker.util.Randomization;
import redis.clients.jedis.graph.entities.Node;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public abstract class CypherEdgeOracle<C extends Connection, T> implements Oracle {

    private final GlobalState<C> state;
    private final Schema<T> schema;

    public CypherEdgeOracle(GlobalState<C> state, Schema<T> schema) {
        this.state = state;
        this.schema = schema;
    }

    // delete edges by id
    @Override
    public void check() {
        int exceptions = 0;
        String type = schema.getRandomType();
        Entity<T> entity = schema.getEntityByType(type);
        List<String> EIds = new ArrayList<>();

        // Query 1
        CypherExpression whereCondition = getWhereClause(entity);
        Query<C> initialQuery = makeQuery(String.format("MATCH ()-[r:%s]->() WHERE %s RETURN ID(r)", type, CypherVisitor.asString(whereCondition)));
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
//                Node t = (Node) r.get("n");
//                VIdsOne.add(String.valueOf(t.getId()));
                // Neo4j
                EIds.add(r.get("ID(r)").toString());
            }
        } else {
            System.out.println("Trigger exception");
            exceptions++;
        }

        // Query 2
        for (String id : EIds) {
            Query<C> firstQuery = makeQuery(String.format("MATCH ()-[r]->() WHERE ID(r)=%s DETACH DELETE r", id));
            result = firstQuery.executeAndGet(state);
            System.out.println("2." + firstQuery.getQuery());
        }

        // Query 3
        Query<C> secondQuery = makeQuery(String.format("MATCH ()-[r:%s]->() WHERE %s RETURN ID(r)", type, CypherVisitor.asString(whereCondition)));
        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        if (result != null) {
            if (result.size() != 0) {
                System.out.println("Deletion false\n");
                throw new AssertionError("Deletion false");
            } else {
                System.out.println("Deletion true\n");
            }
        }
    }

    // find two hop edges and add
    public void check6() {
        int exceptions = 0;
        String type = schema.getRandomType();
        Entity<T> entity = schema.getEntityByType(type);
        List<String> EAllIds = new ArrayList<>();
        List<String> EIdsTwo = new ArrayList<>();
        Long count1 = 0L, count2 = 0L;

        // Query 1
        Query<C> initialQuery = makeQuery("MATCH ()-[r]->() RETURN ID(r)");
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            for (int i = 0; i < result.size(); i++) {
                EAllIds.add(String.valueOf(i));
            }
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        String eId1 = "", eId2 = "", eId3 = "";
        int k = Randomization.nextInt(1, 5);
        eId1 = EAllIds.get(Randomization.nextInt(0, EAllIds.size()-1));
        Query<C> firstQuery = makeQuery(String.format("MATCH (a)-[*%s]->(b) WHERE ID(a)=%s AND a<>b AND length(shortestPath((a)-[*]->(b)))=%s RETURN ID(b)", k, eId1, k));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
                EIdsTwo.add(String.valueOf(r.get("ID(b)")));
            }
        } else {
            System.out.println("2. Trigger exception");
            exceptions++;
        }

        if (EIdsTwo.size() == 0)
            return;
        // Query 3
        if (EIdsTwo.size() == 1) {
            eId2 = EIdsTwo.get(0);
        } else {
            eId2 = EIdsTwo.get(Randomization.nextInt(0, EIdsTwo.size()-1));
        }

        Query<C> secondQuery = makeQuery("CREATE (addNode:addLabel {nodeKey: 'nodeValue'}) RETURN ID(addNode)");
        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        if (result != null) {
            eId3 = String.valueOf(result.get(0).get("ID(addNode)"));
        } else {
            System.out.println("3. Trigger exception");
            exceptions++;
        }
        Query<C> thirdQuery = makeQuery(String.format("MATCH (a),(b) WHERE ID(a)=%s AND ID(b)=%s CREATE (a)-[:EDGE]->(b)", eId2, eId3));
        result = thirdQuery.executeAndGet(state);
        System.out.println("4." + thirdQuery.getQuery());

        // Query 4
        Query<C> fourQuery = makeQuery(String.format("MATCH path=shortestPath((a)-[*]->(b)) WHERE ID(a)=%s AND ID(b)=%s RETURN length(path)", eId1, eId3));
        result = fourQuery.executeAndGet(state);
        System.out.println("5." + fourQuery.getQuery());
        if (result != null) {
            count2 = (Long) result.get(0).get("length(path)");

            if (count2 != (k+1)) {
                System.out.println(String.format("Distance false. count2 is: %s. k is %s.\n", count2, k));
                throw new AssertionError(String.format("Distance false. count2 is: %s. k is %s.\n", count2, k));
            } else {
                System.out.println(String.format("Distance true. count2 is: %s. k is %s.\n", count2, k));
            }
        } else {
            System.out.println("5. Trigger exception");
            exceptions++;
        }

    }

    // where predicate with A or !A or A IS NULL
    public void check5() {
        int exceptions = 0;
        String type = schema.getRandomType();
        Entity<T> entity = schema.getEntityByType(type);

        // Query 1
        Query<C> initialQuery = makeQuery(String.format("MATCH ()-[r:%s]->() RETURN COUNT(r)", type));
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        Long expectedTotal;
        if (result != null) {
            expectedTotal = (Long) result.get(0).get("COUNT(r)");
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        CypherExpression whereCondition = getWhereClause(entity);
        Query<C> firstQuery = makeQuery(String.format("MATCH ()-[r:%s]->() WHERE %s RETURN COUNT(r)", type, CypherVisitor.asString(whereCondition)));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery());
        Long first = 0L;
        if (result != null) {
            first = (Long) result.get(0).get("COUNT(r)");
        } else {
            System.out.println("2. Trigger exception");
            exceptions++;
        }

        // Query 3
        Query<C> secondQuery = makeQuery(String.format("MATCH ()-[r:%s]->() WHERE %s OR NOT(%s) OR (%s) IS NULL RETURN COUNT(r)", type, CypherVisitor.asString(whereCondition), CypherVisitor.asString(whereCondition), CypherVisitor.asString(whereCondition)));
        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        Long second = 0L;
        if (result != null) {
            second = (Long) result.get(0).get("COUNT(r)");
        } else {
            System.out.println("3. Trigger exception");
            exceptions++;
        }

        if (exceptions > 0) {
            throw new IgnoreMeException();
        }

        if (second != expectedTotal) {
            System.out.println(String.format("A|!A is not equal to all. Second: %s != ExpectedTotal: %s", second, expectedTotal));
            throw new AssertionError(String.format("A|!A is not equal to all. Second: %s != ExpectedTotal: %s", second, expectedTotal));
        }
    }

    // where predicate with A and !A
    public void check4() {
        int exceptions = 0;
        String type = schema.getRandomType();
        Entity<T> entity = schema.getEntityByType(type);

        // Query 1
        Query<C> initialQuery = makeQuery(String.format("MATCH ()-[r:%s]->() RETURN COUNT(r)", type));
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        Long expectedTotal;
        if (result != null) {
            expectedTotal = (Long) result.get(0).get("COUNT(r)");
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        CypherExpression whereCondition = getWhereClause(entity);
        Query<C> firstQuery = makeQuery(String.format("MATCH ()-[r:%s]->() WHERE %s RETURN COUNT(r)", type, CypherVisitor.asString(whereCondition)));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery());
        Long first = 0L;
        if (result != null) {
            first = (Long) result.get(0).get("COUNT(r)");
        } else {
            System.out.println("2. Trigger exception");
            exceptions++;
        }

        // Query 3
        Query<C> secondQuery = makeQuery(String.format("MATCH ()-[r:%s]->() WHERE (%s) AND (NOT(%s)) RETURN COUNT(r)", type, CypherVisitor.asString(whereCondition), CypherVisitor.asString(whereCondition)));
        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        Long second = 0L;
        if (result != null) {
            second = (Long) result.get(0).get("COUNT(r)");
        } else {
            System.out.println("3. Trigger exception");
            exceptions++;
        }

        if (exceptions > 0) {
            throw new IgnoreMeException();
        }

        if (second != 0) {
            System.out.println("A&!A is not equal to 0");
            throw new AssertionError("A&!A is not equal to 0");
        }
    }

    // where predicate with or operators, union
    public void check3() {
        int exceptions = 0;
        String type = schema.getRandomType();
        Entity<T> entity = schema.getEntityByType(type);
        List<String> EAllIds = new ArrayList<>();
        List<String> EIdsOne = new ArrayList<>();
        List<String> EIdsTwo = new ArrayList<>();
        List<String> EIdsThree = new ArrayList<>();

        // Query 1
        Query<C> initialQuery = makeQuery("MATCH ()-[r]->() RETURN ID(r)");
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            for (int i = 0; i < result.size(); i++) {
                EAllIds.add(String.valueOf(i));
            }
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        CypherExpression whereCondition1 = getWhereClause(entity);
        CypherExpression negatedWhereCondition1 = new CypherPrefixOperation(whereCondition1, CypherPrefixOperation.PrefixOperator.NOT);
        Query<C> firstQuery = makeQuery(String.format("MATCH ()-[r:%s]->() WHERE %s RETURN ID(r)", type, CypherVisitor.asString(whereCondition1)));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
//                Node t = (Node) r.get("n");
//                VIdsOne.add(String.valueOf(t.getId()));
                EIdsOne.add(r.get("ID(r)").toString());
            }
        } else {
            System.out.println("2. Trigger exception");
            exceptions++;
        }

        // Query 3
        CypherExpression whereCondition2 = getWhereClause(entity);
        CypherExpression negatedWhereCondition2 = new CypherPrefixOperation(whereCondition2, CypherPrefixOperation.PrefixOperator.NOT);
        Query<C> secondQuery = makeQuery(String.format("MATCH ()-[r:%s]->() WHERE %s RETURN ID(r)", type, CypherVisitor.asString(whereCondition2)));
        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
//                Node t = (Node) r.get("n");
//                VIdsTwo.add(String.valueOf(t.getId()));
                EIdsTwo.add(r.get("ID(r)").toString());
            }
        } else {
            System.out.println("3. Trigger exception");
            exceptions++;
        }

        // Query 4
        Query<C> thirdQuery = makeQuery(String.format("MATCH ()-[r:%s]->() WHERE (%s) OR (%s) RETURN ID(r)", type, CypherVisitor.asString(whereCondition1), CypherVisitor.asString(whereCondition2)));
        result = thirdQuery.executeAndGet(state);
        System.out.println("4." + thirdQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
//                Node t = (Node) r.get("n");
//                VIdsThree.add(String.valueOf(t.getId()));
                EIdsThree.add(r.get("ID(r)").toString());
            }
        } else {
            System.out.println("4. Trigger exception");
            exceptions++;
        }

        if (exceptions > 0) {
            throw new IgnoreMeException();
        }

        EIdsOne.sort(Comparator.naturalOrder());
        EIdsTwo.sort(Comparator.naturalOrder());
        EIdsThree.sort(Comparator.naturalOrder());
        List<String> union = new ArrayList<>(EIdsOne);
        union.removeAll(EIdsTwo);
        union.addAll(EIdsTwo);
        union.sort(Comparator.naturalOrder());
        if (!union.equals(EIdsThree)) {
            System.out.println("Comparison false\n");
            System.out.println("VIdsThree results: " + EIdsThree + "\n");
            System.out.println("Union results: " + union + "\n");
            throw new AssertionError(String.format("%s (VIdsThree) are not equal to %s (Union).", EIdsThree, union));
        }
    }

    // where predicate with and operators, intersection
    public void check2() {
        int exceptions = 0;
        String type = schema.getRandomType();
        Entity<T> entity = schema.getEntityByType(type);
        List<String> EAllIds = new ArrayList<>();
        List<String> EIdsOne = new ArrayList<>();
        List<String> EIdsTwo = new ArrayList<>();
        List<String> EIdsThree = new ArrayList<>();

        // Query 1
        Query<C> initialQuery = makeQuery("MATCH ()-[r]->() RETURN ID(r)");
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            for (int i = 0; i < result.size(); i++) {
                EAllIds.add(String.valueOf(i));
            }
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        CypherExpression whereCondition1 = getWhereClause(entity);
        CypherExpression negatedWhereCondition1 = new CypherPrefixOperation(whereCondition1, CypherPrefixOperation.PrefixOperator.NOT);
        Query<C> firstQuery = makeQuery(String.format("MATCH ()-[r:%s]->() WHERE %s RETURN ID(r)", type, CypherVisitor.asString(whereCondition1)));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
//                Node t = (Node) r.get("n");
//                VIdsOne.add(String.valueOf(t.getId()));
                // Neo4j
                EIdsOne.add(r.get("ID(r)").toString());
            }
        } else {
            System.out.println("2. Trigger exception");
            exceptions++;
        }

        // Query 3
        CypherExpression whereCondition2 = getWhereClause(entity);
        CypherExpression negatedWhereCondition2 = new CypherPrefixOperation(whereCondition2, CypherPrefixOperation.PrefixOperator.NOT);
        Query<C> secondQuery = makeQuery(String.format("MATCH ()-[r:%s]->() WHERE %s RETURN ID(r)", type, CypherVisitor.asString(whereCondition2)));
        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
//                Node t = (Node) r.get("n");
//                VIdsTwo.add(String.valueOf(t.getId()));
                EIdsTwo.add(r.get("ID(r)").toString());
            }
        } else {
            System.out.println("3. Trigger exception");
            exceptions++;
        }

        // Query 4
        Query<C> thirdQuery = makeQuery(String.format("MATCH ()-[r:%s]->() WHERE (%s) AND (%s) RETURN ID(r)", type, CypherVisitor.asString(whereCondition1), CypherVisitor.asString(whereCondition2)));
        result = thirdQuery.executeAndGet(state);
        System.out.println("4." + thirdQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
//                Node t = (Node) r.get("n");
//                VIdsThree.add(String.valueOf(t.getId()));
                EIdsThree.add(r.get("ID(r)").toString());
            }
        } else {
            System.out.println("4. Trigger exception");
            exceptions++;
        }

        if (exceptions > 0) {
            throw new IgnoreMeException();
        }

        EIdsOne.sort(Comparator.naturalOrder());
        EIdsTwo.sort(Comparator.naturalOrder());
        EIdsThree.sort(Comparator.naturalOrder());
        List<String> intersection = new ArrayList<>(EIdsOne);
        intersection.retainAll(EIdsTwo);
        intersection.sort(Comparator.naturalOrder());
        if (!intersection.equals(EIdsThree)) {
            System.out.println("Comparison false\n");
            System.out.println("EIdsThree results: " + EIdsThree + "\n");
            System.out.println("Intersection results: " + intersection + "\n");
            throw new AssertionError(String.format("%s (EIdsThree) are not equal to %s (Intersection).", EIdsThree, intersection));
        }
    }

    // where predicate with query partitioning              YES
    public void check1() {
        int exceptions = 0;

        String type = schema.getRandomType();
        Entity<T> entity = schema.getEntityByType(type);

        Query<C> initialQuery = makeQuery(String.format("MATCH ()-[r:%s]-() RETURN COUNT(r)", type));
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        Long expectedTotal;

        if (result != null) {
            expectedTotal = (Long) result.get(0).get("COUNT(r)");
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        CypherExpression whereCondition = getWhereClause(entity);

        Query<C> firstQuery = makeQuery(String.format("MATCH ()-[r:%s]-() WHERE %s RETURN COUNT(r)", type, CypherVisitor.asString(whereCondition)));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery());
        Long first = 0L;

        if (result != null) {
            first = (Long) result.get(0).get("COUNT(r)");
        } else {
            exceptions++;
        }

        CypherExpression negatedWhereCondition = new CypherPrefixOperation(whereCondition, CypherPrefixOperation.PrefixOperator.NOT);
        Query<C> secondQuery = makeQuery(String.format("MATCH ()-[r:%s]-() WHERE %s RETURN COUNT(r)", type, CypherVisitor.asString(negatedWhereCondition)));

        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        Long second = 0L;

        if (result != null) {
            second = (Long) result.get(0).get("COUNT(r)");
        } else {
            exceptions++;
        }

        Query<C> thirdQuery = makeQuery(String.format("MATCH ()-[r:%s]-() WHERE (%s) IS NULL RETURN COUNT(r)", type, CypherVisitor.asString(whereCondition)));
        result = thirdQuery.executeAndGet(state);
        System.out.println("4." + thirdQuery.getQuery());
        Long third = 0L;

        if (result != null) {
            third = (Long) result.get(0).get("COUNT(r)");
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
