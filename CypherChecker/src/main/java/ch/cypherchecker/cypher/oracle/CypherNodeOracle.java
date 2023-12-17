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

import java.util.*;

public abstract class CypherNodeOracle<C extends Connection, T> implements Oracle {

    private final GlobalState<C> state;
    private final Schema<T> schema;

    public CypherNodeOracle(GlobalState<C> state, Schema<T> schema) {
        this.state = state;
        this.schema = schema;
    }

    // delete one node inside result set
    @Override
    public void check() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VIds = new ArrayList<>();
        Long count1 = 0L, count2 = 0L;

        // Query 1
        CypherExpression whereCondition = getWhereClause(entity);
        Query<C> initialQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN COUNT(n)", label, CypherVisitor.asString(whereCondition)));
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            count1 = (Long) result.get(0).get("COUNT(n)");
        } else {
            System.out.println("Trigger exception");
            exceptions++;
        }

        // Query 2
        Query<C> firstQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s WITH collect(n) AS nodes UNWIND nodes as node DETACH DELETE node", label, CypherVisitor.asString(whereCondition)));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery());

        // Query 3
        Query<C> secondQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN COUNT(n)", label, CypherVisitor.asString(whereCondition)));
        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        if (result != null) {
            count2 = (Long) result.get(0).get("COUNT(n)");
        } else {
            System.out.println("3. Trigger exception");
            exceptions++;
        }

        if (count1 != 0) {
            if (count2 != 0) {
                System.out.println("Addition false\n");
                throw new AssertionError(String.format("Addition false. count1 is %s, count2 is %s.\n", count1, count2));
            } else {
                System.out.println(String.format("Redundant addition true. count1 is %s, count2 is %s.\n", count1, count2));
            }
        }
    }

    // add one node inside result set                       YES
    public void check16() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VIds = new ArrayList<>();
        Long count1 = 0L, count2 = 0L;

        // Query 1
        CypherExpression whereCondition = getWhereClause(entity);
        Query<C> initialQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN COUNT(n)", label, CypherVisitor.asString(whereCondition)));
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            count1 = (Long) result.get(0).get("COUNT(n)");
        } else {
            System.out.println("Trigger exception");
            exceptions++;
        }

        // Query 2
        Query<C> firstQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s WITH collect(n) AS nodes UNWIND nodes as node CREATE (new:%s) SET new=node", label, CypherVisitor.asString(whereCondition), label));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery());

        // Query 3
        Query<C> secondQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN COUNT(n)", label, CypherVisitor.asString(whereCondition)));
        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        if (result != null) {
            count2 = (Long) result.get(0).get("COUNT(n)");
        } else {
            System.out.println("3. Trigger exception");
            exceptions++;
        }

        if (count1 != 0) {
            if ((count1*2) != count2) {
                System.out.println("Addition false\n");
                throw new AssertionError(String.format("Addition false. count1 is %s, count2 is %s.\n", count1, count2));
            } else {
                System.out.println(String.format("Redundant addition true. count1 is %s, count2 is %s.\n", count1, count2));
            }
        }

    }

    // add deprecated nodes and edges                       NO
    public void check15() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VIds = new ArrayList<>();
        Long count1 = 0L, count2 = 0L;

        // Query 1
        CypherExpression whereCondition = getWhereClause(entity);
        Query<C> initialQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN COUNT(n)", label, CypherVisitor.asString(whereCondition)));
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            count1 = (Long) result.get(0).get("COUNT(n)");
        } else {
            System.out.println("Trigger exception");
            exceptions++;
        }

        // Query 2
        for (int i = 0; i < 30; i++) {
            StringBuilder addQuery = new StringBuilder();
            addQuery.append(String.format("CREATE (%s:%s {nodeKey: 'nodeValue'}), (%s:%s {nodeKey: 'nodeValue'})", "nodeName"+i, "nodeLabel"+i, "nodeName"+(i+1), "nodeLabel"+(i+1)));
            if (Randomization.getBoolean()) {
                addQuery.append(String.format(", (%s)-[%s:%s]->(%s)", "nodeName"+i, "edgeName"+i, "edgeLabel"+i, "nodeName"+(i+1)));
            }
            Query<C> secondQuery = makeQuery(addQuery.toString());
            result = secondQuery.executeAndGet(state);
            System.out.println("3." + secondQuery.getQuery());
            if (result == null) {
                System.out.println("3. Trigger exception");
                exceptions++;
            }
        }

        // Query 3
        result = initialQuery.executeAndGet(state);
        System.out.println("4." + initialQuery.getQuery());
        if (result != null) {
            count2 = (Long) result.get(0).get("COUNT(n)");
        } else {
            System.out.println("4. Trigger exception");
            exceptions++;
        }

        // Check result
        if (count1 != count2) {
            System.out.println("Redundant addition false\n");
            throw new AssertionError(String.format("Redundant addition false. count1 is %s, count2 is %s.\n", count1, count2));
        } else {
            System.out.println(String.format("Redundant addition true. count1 is %s, count2 is %s.\n", count1, count2));
        }
    }

    // match and where clauses mutation                     Under implementation
    public void check14() {
        int exceptions = 0;

        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        CypherExpression whereCondition = getWhereClause(entity);

        Query<C> firstQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN COUNT(n)", label, CypherVisitor.asString(whereCondition)));
        List<Map<String, Object>> result = firstQuery.executeAndGet(state);
        System.out.println("1." + firstQuery.getQuery());
        Long total1 = 0L;
        if (result != null) {
            total1 = (Long) result.get(0).get("COUNT(n)");
        } else {
            exceptions++;
        }

        Query<C> secondQuery = makeQuery(String.format("MATCH (n:%s) AND (%s) RETURN COUNT(n)", label, CypherVisitor.asString(whereCondition)));
        result = secondQuery.executeAndGet(state);
        System.out.println("1." + secondQuery.getQuery());
        Long total2 = 0L;
        if (result != null) {
            total2 = (Long) result.get(0).get("COUNT(n)");
        } else {
            exceptions++;
        }

        System.out.println("DEBUG");

    }

    // delete nodes by id                                   NO
    public void check13() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VIds = new ArrayList<>();

        // Query 1
        CypherExpression whereCondition = getWhereClause(entity);
        Query<C> initialQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN n", label, CypherVisitor.asString(whereCondition)));
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r : result) {
                Node t = (Node) r.get("n");
                VIds.add(String.valueOf(t.getId()));
//                VIds.add(r.get("n").toString().replaceAll("[^0-9]", ""));
            }
        } else {
            System.out.println("Trigger exception");
            exceptions++;
        }

        // Query 2
        for (String id : VIds) {
            Query<C> firstQuery = makeQuery(String.format("MATCH (n) WHERE ID(n)=%s DETACH DELETE n", id));
            result = firstQuery.executeAndGet(state);
            System.out.println("2." + firstQuery.getQuery());
        }

        // Query 3
        Query<C> secondQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN n", label, CypherVisitor.asString(whereCondition)));
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

    // ancestors/descendants and/or k-hop
    public void check12() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();

        // Query 1
        Query<C> initialQuery = makeQuery("MATCH (n) RETURN n");
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
                Node t = (Node) r.get("n");
                VAllIds.add(String.valueOf(t.getId()));
//                VAllIds.add(r.get("n").toString().replaceAll("[^0-9]", ""));
            }
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        for (String id : VAllIds) {
            List<String> VDescendants = new ArrayList<>();
            Query<C> firstQuery = makeQuery(String.format("MATCH (a) MATCH path=(a)-[*1..]->(b) WHERE (a<>b) AND (ID(a)=%s) RETURN DISTINCT b", id));
            result = firstQuery.executeAndGet(state);
            System.out.println("2." + firstQuery.getQuery());
            if (result != null && result.size() != 0) {
                for (Map<String, Object> r : result) {
                    Node t = (Node) r.get("b");
                    VDescendants.add(String.valueOf(t.getId()));
//                    VDescendants.add(r.get("b").toString().replaceAll("[^0-9]", ""));
                }
            } else {
                System.out.println("2. Trigger exception");
                exceptions++;
            }

            // Query 3
            int k = Randomization.nextInt(1, 6);
            List<String> VKHop = new ArrayList<>();
            Query<C> secondQuery = makeQuery(String.format("MATCH (a) MATCH path=(a)-[*%s..%s]->(b) WHERE (a<>b) AND (ID(a)=%s) RETURN DISTINCT b", k, k, id));
            result = secondQuery.executeAndGet(state);
            System.out.println("3." + secondQuery.getQuery());
            if (result != null && result.size() != 0) {
                for (Map<String, Object> r : result) {
                    Node t = (Node) r.get("b");
                    VKHop.add(String.valueOf(t.getId()));
//                    VKHop.add(r.get("b").toString().replaceAll("[^0-9]", ""));
                }
            } else {
                System.out.println("3. Trigger exception");
                exceptions++;
            }

            // Query 4
            List<String> And = new ArrayList<>();
            Query<C> thirdQuery = makeQuery(String.format("MATCH path=(a)-[*1..]->(b) WHERE (a<>b) AND (ID(a)=%s) RETURN DISTINCT b UNION MATCH path=(a)-[*%s..%s]->(b) WHERE (a<>b) AND (ID(a)=%s) RETURN DISTINCT b", id, k, k, id));
            result = thirdQuery.executeAndGet(state);
            System.out.println("4." + thirdQuery.getQuery());
            if (result != null && result.size() != 0) {
                for (Map<String, Object> r : result) {
                    Node t = (Node) r.get("b");
                    And.add(String.valueOf(t.getId()));
//                    And.add(r.get("b").toString().replaceAll("[^0-9]", ""));
                }
            } else {
                System.out.println("4. Trigger exception");
                exceptions++;
            }

            // Query 5
            List<String> Or = new ArrayList<>();
            Query<C> fourQuery = makeQuery(String.format("MATCH path=(a)-[*1..]->(b) WHERE (a<>b) AND (ID(a)=%s) MATCH path=(a)-[*%s..%s]->(b) WHERE (a<>b) AND (ID(a)=%s) RETURN DISTINCT b", id, k, k, id));
            result = fourQuery.executeAndGet(state);
            System.out.println("5." + fourQuery.getQuery());
            if (result != null && result.size() != 0) {
                for (Map<String, Object> r : result) {
                    Node t = (Node) r.get("b");
                    Or.add(String.valueOf(t.getId()));
//                    Or.add(r.get("b").toString().replaceAll("[^0-9]", ""));
                }
            } else {
                System.out.println("5. Trigger exception");
                exceptions++;
            }

/*            VDescendants.sort(Comparator.naturalOrder());
            VKHop.sort(Comparator.naturalOrder());
            Or.sort(Comparator.naturalOrder());
            List<String> intersection = new ArrayList<>(VDescendants);
            intersection.retainAll(VKHop);
            intersection.sort(Comparator.naturalOrder());
            if (!intersection.equals(Or)) {
                System.out.println("Comparison false\n");
                System.out.println("Or results: " + Or + "\n");
                System.out.println("Intersection results: " + intersection + "\n");
                throw new AssertionError(String.format("%s (Or) are not equal to %s (Intersection).", Or, intersection));
            }*/

            VDescendants.sort(Comparator.naturalOrder());
            VKHop.sort(Comparator.naturalOrder());
            And.sort(Comparator.naturalOrder());
            List<String> union = new ArrayList<>(VDescendants);
            union.removeAll(VKHop);
            union.addAll(VKHop);
            union.sort(Comparator.naturalOrder());
            if (!union.equals(And)) {
                System.out.println("Comparison false\n");
                System.out.println("And results: " + And + "\n");
                System.out.println("Union results: " + union + "\n");
                throw new AssertionError(String.format("%s (And) are not equal to %s (Union).", And, union));
            }
        }
    }

    // spouses and/or ancestors/descendants
    public void check11() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();

        // Query 1
        Query<C> initialQuery = makeQuery("MATCH (n) RETURN n");
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
                Node t = (Node) r.get("n");
                VAllIds.add(String.valueOf(t.getId()));
//                VAllIds.add(r.get("n").toString().replaceAll("[^0-9]", ""));
            }
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        for (String id : VAllIds) {
            List<String> VSpouses = new ArrayList<>();
            Query<C> firstQuery = makeQuery(String.format("MATCH (a)-->(b)<--(c) WHERE ID(a)=%s RETURN DISTINCT c", id));
            result = firstQuery.executeAndGet(state);
            System.out.println("2." + firstQuery.getQuery());
            if (result != null && result.size() != 0) {
                for (Map<String, Object> r : result) {
                    Node t = (Node) r.get("c");
                    VSpouses.add(String.valueOf(t.getId()));
//                    VSpouses.add(r.get("c").toString().replaceAll("[^0-9]", ""));
                }
            } else {
                System.out.println("2. Trigger exception");
                exceptions++;
            }

            // Query 3
            List<String> VDescendants = new ArrayList<>();
            Query<C> secondQuery = makeQuery(String.format("MATCH (a) MATCH path=(a)-[*1..]->(b) WHERE (a<>b) AND (ID(a)=%s) RETURN DISTINCT b", id));
            result = secondQuery.executeAndGet(state);
            System.out.println("3." + secondQuery.getQuery());
            if (result != null && result.size() != 0) {
                for (Map<String, Object> r : result) {
                    Node t = (Node) r.get("b");
                    VDescendants.add(String.valueOf(t.getId()));
//                    VDescendants.add(r.get("b").toString().replaceAll("[^0-9]", ""));
                }
            } else {
                System.out.println("3. Trigger exception");
                exceptions++;
            }

            // Query 4
            List<String> And = new ArrayList<>();
            Query<C> thirdQuery = makeQuery(String.format("MATCH (a)-->(b)<--(c) WHERE ID(a)=%s RETURN DISTINCT c UNION MATCH path=(a)-[*1..]->(c) WHERE (a<>c) AND (ID(a)=%s) RETURN DISTINCT c", id, id));
            result = thirdQuery.executeAndGet(state);
            System.out.println("4." + thirdQuery.getQuery());
            if (result != null && result.size() != 0) {
                for (Map<String, Object> r : result) {
                    Node t = (Node) r.get("c");
                    And.add(String.valueOf(t.getId()));
//                    And.add(r.get("c").toString().replaceAll("[^0-9]", ""));
                }
            } else {
                System.out.println("4. Trigger exception");
                exceptions++;
            }

            // Query 5
            List<String> Or = new ArrayList<>();
            Query<C> fourQuery = makeQuery(String.format("MATCH (a)-->(b)<--(c) WHERE ID(a)=%s MATCH path=(a)-[*1..]->(c) WHERE (a<>c) AND (ID(a)=%s) RETURN DISTINCT c", id, id));
            result = fourQuery.executeAndGet(state);
            System.out.println("5." + fourQuery.getQuery());
            if (result != null && result.size() != 0) {
                for (Map<String, Object> r : result) {
                    Node t = (Node) r.get("c");
                    Or.add(String.valueOf(t.getId()));
//                    Or.add(r.get("c").toString().replaceAll("[^0-9]", ""));
                }
            } else {
                System.out.println("5. Trigger exception");
                exceptions++;
            }

            VSpouses.sort(Comparator.naturalOrder());
            VDescendants.sort(Comparator.naturalOrder());
            Or.sort(Comparator.naturalOrder());
            List<String> intersection = new ArrayList<>(VSpouses);
            intersection.retainAll(VDescendants);
            intersection.sort(Comparator.naturalOrder());
            if (!intersection.equals(Or)) {
                System.out.println("Comparison false\n");
                System.out.println("Or results: " + Or + "\n");
                System.out.println("Intersection results: " + intersection + "\n");
                throw new AssertionError(String.format("%s (Or) are not equal to %s (Intersection).", Or, intersection));
            }

/*            VSpouses.sort(Comparator.naturalOrder());
            VDescendants.sort(Comparator.naturalOrder());
            And.sort(Comparator.naturalOrder());
            List<String> union = new ArrayList<>(VSpouses);
            union.removeAll(VDescendants);
            union.addAll(VDescendants);
            union.sort(Comparator.naturalOrder());
            if (!union.equals(And)) {
                System.out.println("Comparison false\n");
                System.out.println("And results: " + And + "\n");
                System.out.println("Union results: " + union + "\n");
                throw new AssertionError(String.format("%s (And) are not equal to %s (Union).", And, union));
            }*/
        }
    }

    // spouses and/or k-hop
    public void check10() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();

        // Query 1
        Query<C> initialQuery = makeQuery("MATCH (n) RETURN n");
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
                Node t = (Node) r.get("n");
                VAllIds.add(String.valueOf(t.getId()));
//                VAllIds.add(r.get("n").toString().replaceAll("[^0-9]", ""));
            }
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        for (String id : VAllIds) {
            List<String> VSpouses = new ArrayList<>();
            Query<C> firstQuery = makeQuery(String.format("MATCH (a)-->(b)<--(c) WHERE ID(a)=%s RETURN DISTINCT c", id));
            result = firstQuery.executeAndGet(state);
            System.out.println("2." + firstQuery.getQuery());
            if (result != null && result.size() != 0) {
                for (Map<String, Object> r : result) {
                    Node t = (Node) r.get("c");
                    VSpouses.add(String.valueOf(t.getId()));
//                    VSpouses.add(r.get("c").toString().replaceAll("[^0-9]", ""));
                }
            } else {
                System.out.println("2. Trigger exception");
                exceptions++;
            }

            // Query 3
            int k = Randomization.nextInt(1, 6);
            List<String> VKHop = new ArrayList<>();
            Query<C> secondQuery = makeQuery(String.format("MATCH (a) MATCH path=(a)-[*%s..%s]->(b) WHERE (a<>b) AND (ID(a)=%s) RETURN DISTINCT b", k, k, id));
            result = secondQuery.executeAndGet(state);
            System.out.println("3." + secondQuery.getQuery());
            if (result != null && result.size() != 0) {
                for (Map<String, Object> r : result) {
                    Node t = (Node) r.get("b");
                    VKHop.add(String.valueOf(t.getId()));
//                    VKHop.add(r.get("b").toString().replaceAll("[^0-9]", ""));
                }
            } else {
                System.out.println("3. Trigger exception");
                exceptions++;
            }

            // Query 4
            List<String> And = new ArrayList<>();
            Query<C> thirdQuery = makeQuery(String.format("MATCH (a)-->(b)<--(c) WHERE ID(a)=%s RETURN DISTINCT c UNION MATCH path=(a)-[*%s..%s]->(c) WHERE (a<>c) AND (ID(a)=%s) RETURN DISTINCT c", id, k, k, id));
            result = thirdQuery.executeAndGet(state);
            System.out.println("4." + thirdQuery.getQuery());
            if (result != null && result.size() != 0) {
                for (Map<String, Object> r : result) {
                    Node t = (Node) r.get("c");
                    And.add(String.valueOf(t.getId()));
//                    And.add(r.get("c").toString().replaceAll("[^0-9]", ""));
                }
            } else {
                System.out.println("4. Trigger exception");
                exceptions++;
            }

            // Query 5
            List<String> Or = new ArrayList<>();
            Query<C> fourQuery = makeQuery(String.format("MATCH (a)-->(b)<--(c) WHERE ID(a)=%s MATCH path=(a)-[*%s..%s]->(c) WHERE (a<>c) AND (ID(a)=%s) RETURN DISTINCT c", id, k, k, id));
            result = fourQuery.executeAndGet(state);
            System.out.println("5." + fourQuery.getQuery());
            if (result != null && result.size() != 0) {
                for (Map<String, Object> r : result) {
                    Node t = (Node) r.get("c");
                    Or.add(String.valueOf(t.getId()));
//                    Or.add(r.get("c").toString().replaceAll("[^0-9]", ""));
                }
            } else {
                System.out.println("5. Trigger exception");
                exceptions++;
            }

            VSpouses.sort(Comparator.naturalOrder());
            VKHop.sort(Comparator.naturalOrder());
            Or.sort(Comparator.naturalOrder());
            List<String> intersection = new ArrayList<>(VSpouses);
            intersection.retainAll(VKHop);
            intersection.sort(Comparator.naturalOrder());
            if (!intersection.equals(Or)) {
                System.out.println("Comparison false\n");
                System.out.println("Or results: " + Or + "\n");
                System.out.println("Intersection results: " + intersection + "\n");
                throw new AssertionError(String.format("%s (Or) are not equal to %s (Intersection).", Or, intersection));
            }

/*            VSpouses.sort(Comparator.naturalOrder());
            VKHop.sort(Comparator.naturalOrder());
            And.sort(Comparator.naturalOrder());
            List<String> union = new ArrayList<>(VSpouses);
            union.removeAll(VKHop);
            union.addAll(VKHop);
            union.sort(Comparator.naturalOrder());
            if (!union.equals(And)) {
                System.out.println("Comparison false\n");
                System.out.println("And results: " + And + "\n");
                System.out.println("Union results: " + union + "\n");
                throw new AssertionError(String.format("%s (And) are not equal to %s (Union).", And, union));
            }*/
        }
    }

    // k-hop
    public void check9() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();

        // Query 1
        Query<C> initialQuery = makeQuery("MATCH (n) RETURN n");
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
                Node t = (Node) r.get("n");
                VAllIds.add(String.valueOf(t.getId()));
//                VAllIds.add(r.get("n").toString().replaceAll("[^0-9]", ""));
            }
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        for (String id : VAllIds) {
            int k = Randomization.nextInt(1, 6);
            List<String> VKHop1 = new ArrayList<>();
            Query<C> firstQuery = makeQuery(String.format("MATCH (a) MATCH path=(a)-[*%s..%s]->(b) WHERE (a<>b) AND (ID(a)=%s) RETURN DISTINCT b", k, k, id));
            result = firstQuery.executeAndGet(state);
            System.out.println("2." + firstQuery.getQuery());
            if (result != null && result.size() != 0) {
                for (Map<String, Object> r : result) {
                    Node t = (Node) r.get("b");
                    VKHop1.add(String.valueOf(t.getId()));
//                    VKHop1.add(r.get("b").toString().replaceAll("[^0-9]", ""));
                }
            } else {
                System.out.println("2. Trigger exception");
                exceptions++;
            }

            for (String id2 : VKHop1) {
                List<String> VKHop2 = new ArrayList<>();
                Query<C> secondQuery = makeQuery(String.format("MATCH (a) MATCH path=(a)<-[*%s..%s]-(b) WHERE (a<>b) AND (ID(a)=%s) RETURN DISTINCT b", k, k, id2));
                List<Map<String, Object>> result2 = secondQuery.executeAndGet(state);
                System.out.println("3." + secondQuery.getQuery());
                if (result2 != null && result2.size() != 0) {
                    for (Map<String, Object> r : result2) {
                        Node t = (Node) r.get("b");
                        VKHop2.add(String.valueOf(t.getId()));
//                        VKHop2.add(r.get("b").toString().replaceAll("[^0-9]", ""));
                    }

                    if (!VKHop2.contains(id)) {
                        System.out.println("Find spouses false\n");
                        System.out.println("Results are: " + id + ", " + id2 + "\n");
                        throw new AssertionError(String.format("Results are: %s, %s", id, id2));
                    }
                } else {
                    System.out.println("3. Trigger exception");
                    exceptions++;
                }
            }
        }
    }

    // ancestors/descendants                                NO
    public void check8() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();

        // Query 1
        Query<C> initialQuery = makeQuery("MATCH (n) RETURN n");
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
                Node t = (Node) r.get("n");
                VAllIds.add(String.valueOf(t.getId()));
//                VAllIds.add(r.get("n").toString().replaceAll("[^0-9]", ""));
            }
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        for (String id : VAllIds) {
            List<String> VDescendants = new ArrayList<>();
            Query<C> firstQuery = makeQuery(String.format("MATCH (a) MATCH path=(a)-[*1..]->(b) WHERE (a<>b) AND (ID(a)=%s) RETURN DISTINCT b", id));
            result = firstQuery.executeAndGet(state);
            System.out.println("2." + firstQuery.getQuery());
            if (result != null && result.size() != 0) {
                for (Map<String, Object> r : result) {
                    Node t = (Node) r.get("b");
                    VDescendants.add(String.valueOf(t.getId()));
//                    VDescendants.add(r.get("b").toString().replaceAll("[^0-9]", ""));
                }
            } else {
                System.out.println("2. Trigger exception");
                exceptions++;
            }

            for (String id2 : VDescendants) {
                List<String> VAncestors = new ArrayList<>();
                Query<C> secondQuery = makeQuery(String.format("MATCH (a) MATCH path=(a)<-[*1..]-(b) WHERE (a<>b) AND (ID(a)=%s) RETURN DISTINCT b", id2));
                List<Map<String, Object>> result2 = secondQuery.executeAndGet(state);
                System.out.println("3." + secondQuery.getQuery());
                if (result2 != null && result2.size() != 0) {
                    for (Map<String, Object> r : result2) {
                        Node t = (Node) r.get("b");
                        VAncestors.add(String.valueOf(t.getId()));
//                        VAncestors.add(r.get("b").toString().replaceAll("[^0-9]", ""));
                    }

                    if (!VAncestors.contains(id)) {
                        System.out.println("Find spouses false\n");
                        System.out.println("Results are: " + id + ", " + id2 + "\n");
                        throw new AssertionError(String.format("Results are: %s, %s", id, id2));
                    }
                } else {
                    System.out.println("3. Trigger exception");
                    exceptions++;
                }
            }
        }
    }

    // spouses                                              NO
    public void check7() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();

        // Query 1
        Query<C> initialQuery = makeQuery("MATCH (n) RETURN n");
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
                Node t = (Node) r.get("n");
                VAllIds.add(String.valueOf(t.getId()));
//                VAllIds.add(r.get("n").toString().replaceAll("[^0-9]", ""));
            }
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        for (String id : VAllIds) {
            List<String> VSpouses = new ArrayList<>();
            Query<C> firstQuery = makeQuery(String.format("MATCH (a)-->(b)<--(c) WHERE ID(a)=%s RETURN DISTINCT c", id));
            result = firstQuery.executeAndGet(state);
            System.out.println("2." + firstQuery.getQuery());
            if (result != null && result.size() != 0) {
                for (Map<String, Object> r: result) {
                    Node t = (Node) r.get("c");
                    VSpouses.add(String.valueOf(t.getId()));
//                    VSpouses.add(r.get("c").toString().replaceAll("[^0-9]", ""));
                }
            } else {
                System.out.println("2. Trigger exception");
                exceptions++;
            }

            // Query 3
            for (String id2 : VSpouses) {
                List<String> VSpouses2 = new ArrayList<>();
                Query<C> secondQuery = makeQuery(String.format("MATCH (a)-->(b)<--(c) WHERE ID(a)=%s RETURN DISTINCT c", id2));
                List<Map<String, Object>> result2 = secondQuery.executeAndGet(state);
                System.out.println("3." + secondQuery.getQuery());
                if (result2 != null && result2.size() != 0) {
                    for (Map<String, Object> r: result2) {
                        Node t = (Node) r.get("c");
                        VSpouses2.add(String.valueOf(t.getId()));
//                        VSpouses2.add(r.get("c").toString().replaceAll("[^0-9]", ""));
                    }

                    if (!VSpouses2.contains(id)) {
                        System.out.println("Find spouses false\n");
                        System.out.println("Results are: " + id + ", " + id2 + "\n");
                        throw new AssertionError(String.format("Results are: %s, %s", id, id2));
                    }
                } else {
                    System.out.println("3. Trigger exception");
                    exceptions++;
                }
            }
        }
    }

    // where predicate with A or !A or A IS NULL            NO
    public void check6() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);

        // Query 1
        Query<C> initialQuery = makeQuery(String.format("MATCH (n:%s) RETURN COUNT(n)", label));
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        Long expectedTotal;
        if (result != null) {
            expectedTotal = (Long) result.get(0).get("COUNT(n)");
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        CypherExpression whereCondition = getWhereClause(entity);
        Query<C> firstQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN COUNT(n)", label, CypherVisitor.asString(whereCondition)));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery());
        Long first = 0L;
        if (result != null) {
            first = (Long) result.get(0).get("COUNT(n)");
        } else {
            System.out.println("2. Trigger exception");
            exceptions++;
        }

        // Query 3
        Query<C> secondQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s OR NOT(%s) OR (%s) IS NULL RETURN COUNT(n)", label, CypherVisitor.asString(whereCondition), CypherVisitor.asString(whereCondition), CypherVisitor.asString(whereCondition)));
        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        Long second = 0L;
        if (result != null) {
            second = (Long) result.get(0).get("COUNT(n)");
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

    // where predicate with A and !A                        YES, SAME AS 1
    public void check5() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);

        // Query 1
        Query<C> initialQuery = makeQuery(String.format("MATCH (n:%s) RETURN COUNT(n)", label));
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        Long expectedTotal;
        if (result != null) {
            expectedTotal = (Long) result.get(0).get("COUNT(n)");
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        CypherExpression whereCondition = getWhereClause(entity);
        Query<C> firstQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN COUNT(n)", label, CypherVisitor.asString(whereCondition)));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery());
        Long first = 0L;
        if (result != null) {
            first = (Long) result.get(0).get("COUNT(n)");
        } else {
            System.out.println("2. Trigger exception");
            exceptions++;
        }

        // Query 3
        Query<C> secondQuery = makeQuery(String.format("MATCH (n:%s) WHERE (%s) AND (NOT(%s)) RETURN COUNT(n)", label, CypherVisitor.asString(whereCondition), CypherVisitor.asString(whereCondition)));
        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        Long second = 0L;
        if (result != null) {
            second = (Long) result.get(0).get("COUNT(n)");
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

    // where predicate with A + not(A)                      DESIGN WRONGLY, should A and !A and A IS NULL
    public void check4() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);

        // Query 1
        Query<C> initialQuery = makeQuery(String.format("MATCH (n:%s) RETURN COUNT(n)", label));
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        Long expectedTotal;
        if (result != null) {
            expectedTotal = (Long) result.get(0).get("COUNT(n)");
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        CypherExpression whereCondition1 = getWhereClause(entity);
        Query<C> firstQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN COUNT(n)", label, CypherVisitor.asString(whereCondition1)));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery());
        Long first = 0L;
        if (result != null) {
            first = (Long) result.get(0).get("COUNT(n)");
        } else {
            System.out.println("2. Trigger exception");
            exceptions++;
        }

        // Query 3
        CypherExpression negatedWhereCondition1 = new CypherPrefixOperation(whereCondition1, CypherPrefixOperation.PrefixOperator.NOT);
        Query<C> secondQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN COUNT(n)", label, CypherVisitor.asString(negatedWhereCondition1)));
        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        Long second = 0L;
        if (result != null) {
            second = (Long) result.get(0).get("COUNT(n)");
        } else {
            System.out.println("3. Trigger exception");
            exceptions++;
        }

        if (exceptions > 0) {
            throw new IgnoreMeException();
        }

        if (first + second != expectedTotal) {
            System.out.println(String.format("%d + %d is not equal to %d", first, second, expectedTotal));
            throw new AssertionError(String.format("%d + %d is not equal to %d", first, second, expectedTotal));
        }

    }

    // where predicate with or operators, union             YES
    public void check3() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();
        List<String> VIdsOne = new ArrayList<>();
        List<String> VIdsTwo = new ArrayList<>();
        List<String> VIdsThree = new ArrayList<>();

        // Query 1
        Query<C> initialQuery = makeQuery("MATCH (n) RETURN n");
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            for (int i = 0; i < result.size(); i++) {
                VAllIds.add(String.valueOf(i));
            }
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        CypherExpression whereCondition1 = getWhereClause(entity);
        CypherExpression negatedWhereCondition1 = new CypherPrefixOperation(whereCondition1, CypherPrefixOperation.PrefixOperator.NOT);
        Query<C> firstQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN n", label, CypherVisitor.asString(whereCondition1)));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
                Node t = (Node) r.get("n");
                VIdsOne.add(String.valueOf(t.getId()));
//                VIdsOne.add(r.get("n").toString().replaceAll("[^0-9]", ""));
            }
        } else {
            System.out.println("2. Trigger exception");
            exceptions++;
        }

        // Query 3
        CypherExpression whereCondition2 = getWhereClause(entity);
        CypherExpression negatedWhereCondition2 = new CypherPrefixOperation(whereCondition2, CypherPrefixOperation.PrefixOperator.NOT);
        Query<C> secondQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN n", label, CypherVisitor.asString(whereCondition2)));
        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
                Node t = (Node) r.get("n");
                VIdsTwo.add(String.valueOf(t.getId()));
//                VIdsTwo.add(r.get("n").toString().replaceAll("[^0-9]", ""));
            }
        } else {
            System.out.println("3. Trigger exception");
            exceptions++;
        }

        // Query 4
        Query<C> thirdQuery = makeQuery(String.format("MATCH (n:%s) WHERE (%s) OR (%s) RETURN n", label, CypherVisitor.asString(whereCondition1), CypherVisitor.asString(whereCondition2)));
        result = thirdQuery.executeAndGet(state);
        System.out.println("4." + thirdQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
                Node t = (Node) r.get("n");
                VIdsThree.add(String.valueOf(t.getId()));
//                VIdsThree.add(r.get("n").toString().replaceAll("[^0-9]", ""));
            }
        } else {
            System.out.println("4. Trigger exception");
            exceptions++;
        }

        if (exceptions > 0) {
            throw new IgnoreMeException();
        }

        VIdsOne.sort(Comparator.naturalOrder());
        VIdsTwo.sort(Comparator.naturalOrder());
        VIdsThree.sort(Comparator.naturalOrder());
        List<String> union = new ArrayList<>(VIdsOne);
        union.removeAll(VIdsTwo);
        union.addAll(VIdsTwo);
        union.sort(Comparator.naturalOrder());
        if (!union.equals(VIdsThree)) {
            System.out.println("Comparison false\n");
            System.out.println("VIdsThree results: " + VIdsThree + "\n");
            System.out.println("Union results: " + union + "\n");
            throw new AssertionError(String.format("%s (VIdsThree) are not equal to %s (Union).", VIdsThree, union));
        }

    }

    // where predicate with and operators, intersection     YES
    public void check2() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();
        List<String> VIdsOne = new ArrayList<>();
        List<String> VIdsTwo = new ArrayList<>();
        List<String> VIdsThree = new ArrayList<>();

        // Query 1
        Query<C> initialQuery = makeQuery("MATCH (n) RETURN n");
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            for (int i = 0; i < result.size(); i++) {
                VAllIds.add(String.valueOf(i));
            }
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        CypherExpression whereCondition1 = getWhereClause(entity);
        CypherExpression negatedWhereCondition1 = new CypherPrefixOperation(whereCondition1, CypherPrefixOperation.PrefixOperator.NOT);
        Query<C> firstQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN n", label, CypherVisitor.asString(whereCondition1)));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
                Node t = (Node) r.get("n");
                VIdsOne.add(String.valueOf(t.getId()));
                // Neo4j
//                VIdsOne.add(r.get("n").toString().replaceAll("[^0-9]", ""));
            }
        } else {
            System.out.println("2. Trigger exception");
            exceptions++;
        }

        // Query 3
        CypherExpression whereCondition2 = getWhereClause(entity);
        CypherExpression negatedWhereCondition2 = new CypherPrefixOperation(whereCondition2, CypherPrefixOperation.PrefixOperator.NOT);
        Query<C> secondQuery = makeQuery(String.format("MATCH (n:%s) WHERE %s RETURN n", label, CypherVisitor.asString(whereCondition2)));
        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
                Node t = (Node) r.get("n");
                VIdsTwo.add(String.valueOf(t.getId()));
//                VIdsTwo.add(r.get("n").toString().replaceAll("[^0-9]", ""));
            }
        } else {
            System.out.println("3. Trigger exception");
            exceptions++;
        }

        // Query 4
        Query<C> thirdQuery = makeQuery(String.format("MATCH (n:%s) WHERE (%s) AND (%s) RETURN n", label, CypherVisitor.asString(whereCondition1), CypherVisitor.asString(whereCondition2)));
        result = thirdQuery.executeAndGet(state);
        System.out.println("4." + thirdQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
                Node t = (Node) r.get("n");
                VIdsThree.add(String.valueOf(t.getId()));
//                VIdsThree.add(r.get("n").toString().replaceAll("[^0-9]", ""));
            }
        } else {
            System.out.println("4. Trigger exception");
            exceptions++;
        }

        if (exceptions > 0) {
            throw new IgnoreMeException();
        }

        VIdsOne.sort(Comparator.naturalOrder());
        VIdsTwo.sort(Comparator.naturalOrder());
        VIdsThree.sort(Comparator.naturalOrder());
        List<String> intersection = new ArrayList<>(VIdsOne);
        intersection.retainAll(VIdsTwo);
        intersection.sort(Comparator.naturalOrder());
        if (!intersection.equals(VIdsThree)) {
            System.out.println("Comparison false\n");
            System.out.println("VIdsThree results: " + VIdsThree + "\n");
            System.out.println("Intersection results: " + intersection + "\n");
            throw new AssertionError(String.format("%s (VIdsThree) are not equal to %s (Intersection).", VIdsThree, intersection));
        }

    }

    // where predicate with query partitioning              YES
    public void check1() {
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
