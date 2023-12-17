package ch.cypherchecker.cypher.oracle;

import ch.cypherchecker.common.Connection;
import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.Oracle;
import ch.cypherchecker.common.Query;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.cypher.ast.CypherExpression;
import ch.cypherchecker.util.Randomization;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.DefaultValueMapper.*;
import redis.clients.jedis.graph.entities.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class CypherPathOracle<C extends Connection, T> implements Oracle {

    private final GlobalState<C> state;
    private final Schema<T> schema;

    public CypherPathOracle(GlobalState<C> state, Schema<T> schema) {
        this.state = state;
        this.schema = schema;
    }

    // delete nodes or edges in only one path           NO
    @Override
    public void check() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();
        Long count1 = 0L, count2 = 0L;

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
        for(int id1 = 0; id1 < VAllIds.size(); id1++) {
            for (int id2 = 0; id2 < VAllIds.size(); id2++) {
                if (id2 == id1) continue;
                String vId1 = VAllIds.get(id1), vId2 = VAllIds.get(id2);
                Query<C> firstQuery = makeQuery(String.format("MATCH path=(a)-[*]->(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId2));
                result = firstQuery.executeAndGet(state);
                System.out.println("2." + firstQuery.getQuery());
                if (result != null) {
                    count1 = (Long) result.get(0).get("COUNT(path)");
                    if (count1 == 1) {

                        // Query 3
                        Query<C> secondQuery = makeQuery(String.format("MATCH path=(a)-[*]->(b) WHERE ID(a)=%s AND ID(b)=%s FOREACH (rel IN relationships(path) | DELETE rel)", vId1, vId2));
                        List<Map<String, Object>> result2 = secondQuery.executeAndGet(state);
                        System.out.println("3." + secondQuery.getQuery());

                        // Query 4
                        Query<C> thirdQuery = makeQuery(String.format("MATCH path=(a)-[*]->(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId2));
                        List<Map<String, Object>> result3 = thirdQuery.executeAndGet(state);
                        System.out.println("4." + thirdQuery.getQuery());
                        if (result3 != null) {
                            count2 = (Long) result3.get(0).get("COUNT(path)");

                            // Check result
                            if (count2 != 0) {
                                System.out.println(String.format("Deletion false. count1 is %s, count2 is %s. count2 != 0.\n", count1, count2));
                                throw new AssertionError(String.format("Deletion false. count1 is %s, count2 is %s. count2 != 0.\n", count1, count2));
                            } else {
                                System.out.println(String.format("Deletion true. count1 is %s, count2 is %s. count2 == 0.\n", count1, count2));
                                id1 = VAllIds.size();
                                break;
                            }

                        } else {
                            System.out.println("3. Trigger exception");
                            exceptions++;
                        }
                    }
                } else {
                    System.out.println("2. Trigger exception");
                    exceptions++;
                }
            }
        }
    }

    // delete all the edges between nodes   NO
    public void check9() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();
        Long count1 = 0L, count2 = 0L;

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
        for(int id1 = 0; id1 < VAllIds.size(); id1++) {
            for (int id2 = 0; id2 < VAllIds.size(); id2++) {
                if (id2 == id1) continue;
                String vId1 = VAllIds.get(id1), vId2 = VAllIds.get(id2);
                Query<C> firstQuery = makeQuery(String.format("MATCH (a)-[r]->(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(r)", vId1, vId2));
                result = firstQuery.executeAndGet(state);
                System.out.println("2." + firstQuery.getQuery());
                if (result != null) {
                    count1 = (Long) result.get(0).get("COUNT(r)");
                    if (count1 == 0)
                        continue;

                    // Query 3
                    Query<C> secondQuery = makeQuery(String.format("MATCH (a)-[r]->(b) WHERE ID(a)=%s AND ID(b)=%s DELETE r", vId1, vId2));
                    List<Map<String, Object>> result2 = secondQuery.executeAndGet(state);
                    System.out.println("3." + secondQuery.getQuery());

                    // Query 4
                    Query<C> thirdQuery = makeQuery(String.format("MATCH (a)-[r]->(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(r)", vId1, vId2));
                    List<Map<String, Object>> result3 = thirdQuery.executeAndGet(state);
                    System.out.println("4." + thirdQuery.getQuery());
                    if (result3 != null) {
                        count2 = (Long) result3.get(0).get("COUNT(r)");

                        // Check result
                        if (count2 != 0) {
                            System.out.println(String.format("Deletion false. count1 is %s, count2 is %s. count2 != 0.\n", count1, count2));
                            throw new AssertionError(String.format("Deletion false. count1 is %s, count2 is %s. count2 != 0.\n", count1, count2));
                        } else {
                            System.out.println(String.format("Deletion true. count1 is %s, count2 is %s. count2 == 0.\n", count1, count2));
                            id1 = VAllIds.size();
                            break;
                        }
                    } else {
                        System.out.println("4. Trigger exception");
                        exceptions++;
                    }

                } else {
                    System.out.println("2. Trigger exception");
                    exceptions++;
                }

            }
        }
    }

    // add a redundant node in the end      NO
    public void check8() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();
        Long count1 = 0L, count2 = 0L;

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
        for(int id1 = 0; id1 < VAllIds.size(); id1++) {
            for (int id2 = 0; id2 < VAllIds.size(); id2++) {
                if (id2 == id1) continue;
                String vId1 = VAllIds.get(id1), vId2 = VAllIds.get(id2), vId3 = "";
                Query<C> firstQuery = makeQuery(String.format("MATCH path=(a)-[*]->(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId2));
                result = firstQuery.executeAndGet(state);
                System.out.println("2." + firstQuery.getQuery());
                if (result != null) {
                    count1 = (Long) result.get(0).get("COUNT(path)");
                    if (count1 == 0)
                        continue;
                } else {
                    System.out.println("2. Trigger exception");
                    exceptions++;
                }

                // Query 3
                Query<C> secondQuery = makeQuery("CREATE (n:redundantNode {nodeKey: 'nodeValue'}) RETURN ID(n)");
                List<Map<String, Object>> result2 = secondQuery.executeAndGet(state);
                System.out.println("3." + secondQuery.getQuery());
                if (result2 != null) {
                    vId3 = String.valueOf(result2.get(0).get("ID(n)"));
                } else {
                    System.out.println("3. Trigger exception");
                    exceptions++;
                }

                // Query 4
                Query<C> thirdQuery = makeQuery(String.format("MATCH (a),(b) WHERE ID(a)=%s AND ID(b)=%s CREATE (a)-[:Edge]->(b)", vId2, vId3));
                List<Map<String, Object>> result3 = thirdQuery.executeAndGet(state);
                System.out.println("4." + thirdQuery.getQuery());

                // Query 5
                Query<C> fourQuery = makeQuery(String.format("MATCH path=(a)-[*]->(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId3));
                List<Map<String, Object>> result4 = fourQuery.executeAndGet(state);
                System.out.println("5." + fourQuery.getQuery());
                if (result4 != null) {
                    count2 = (Long) result4.get(0).get("COUNT(path)");
                } else {
                    System.out.println("5. Trigger exception");
                    exceptions++;
                }

                // Check result
                if (count1 != count2) {
                    System.out.println(String.format("Addition false. count1 is %s, count2 is %s.\n", count1, count2));
                    throw new AssertionError(String.format("Addition false. count1 is %s, count2 is %s.\n", count1, count2));
                } else {
                    System.out.println(String.format("Addition true. count1 is %s, count2 is %s.\n", count1, count2));
                    id1 = VAllIds.size();
                    break;
                }
            }
        }
    }

    // add a ring in the end node           YES?
    public void check7() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();
        Long count1 = 0L, count2 = 0L;

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
        for(int id1 = 0; id1 < VAllIds.size(); id1++) {
            for (int id2 = 0; id2 < VAllIds.size(); id2++) {
                if (id2 == id1) continue;
                String vId1 = VAllIds.get(id1), vId2 = VAllIds.get(id2);
                Query<C> firstQuery = makeQuery(String.format("MATCH path=(a)-[*]->(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId2));
                result = firstQuery.executeAndGet(state);
                System.out.println("2." + firstQuery.getQuery());
                if (result != null) {
                    count1 = (Long) result.get(0).get("COUNT(path)");
                    if (count1 == 0)
                        continue;
                } else {
                    System.out.println("2. Trigger exception");
                    exceptions++;
                }

                // Query 3
                Query<C> secondQuery = makeQuery(String.format("MATCH (a),(b) WHERE ID(a)=%s AND ID(b)=%s CREATE (a)-[:Edge_Ring]->(b)", vId2, vId2));
                List<Map<String, Object>> result2 = secondQuery.executeAndGet(state);
                System.out.println("3." + secondQuery.getQuery());

                // Query 4
                Query<C> thirdQuery = makeQuery(String.format("MATCH path=(a)-[*]->(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId2));
                List<Map<String, Object>> result3 = thirdQuery.executeAndGet(state);
                System.out.println("4." + thirdQuery.getQuery());
                if (result3 != null) {
                    count2 = (Long) result3.get(0).get("COUNT(path)");
                } else {
                    System.out.println("4. Trigger exception");
                    exceptions++;
                }

                // Check result
                if (count1*2 != count2) {
                    System.out.println(String.format("Addition false. count1 is %s, count2 is %s.\n", count1, count2));
                    throw new AssertionError(String.format("Addition false. count1 is %s, count2 is %s.\n", count1, count2));
                } else {
                    System.out.println(String.format("Addition true. count1 is %s, count2 is %s.\n", count1, count2));
                    id1 = VAllIds.size();
                    break;
                }
            }
        }
    }

    // add one edge between nodes in shortest path      NO, the code format doesn't support
    public void check6() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();
        Long count1 = 0L, count2 = 0L;

        // Query 1
        Query<C> initialQuery = makeQuery("MATCH (n) RETURN n");
        List<Map<String, Object>> result = initialQuery.executeAndGet(state);
        System.out.println("1." + initialQuery.getQuery());
        if (result != null) {
            for (Map<String, Object> r: result) {
//                Node t = (Node) r.get("n");
//                VAllIds.add(String.valueOf(t.getId()));
                VAllIds.add(r.get("n").toString().replaceAll("[^0-9]", ""));
            }
        } else {
            throw new AssertionError("Unexpected exception when fetching total");
        }

        // Query 2
        for(int id1 = 0; id1 < VAllIds.size(); id1++) {
            for (int id2 = 0; id2 < VAllIds.size(); id2++) {
                if (id2 == id1) continue;
                String vId1 = VAllIds.get(id1), vId2 = VAllIds.get(id2);
                Query<C> firstQuery = makeQuery(String.format("MATCH path=shortestPath((a)-[*]-(b)) WHERE ID(a)=%s AND ID(b)=%s RETURN path", vId1, vId2));
                result = firstQuery.executeAndGet(state);
                System.out.println("2." + firstQuery.getQuery());
                if (result != null) {
                    if (result.size() == 0)
                        continue;
                } else {
                    System.out.println("2. Trigger exception");
                    exceptions++;
                }
            }
        }
    }

    // add one edge between nodes           NO, exist loops from node1, not MR
    public void check5() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();
        Long count1 = 0L, count2 = 0L;

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
        int id1 = 0, id2 = 0;
        do {
            id1 = Randomization.nextInt(0, VAllIds.size() - 1);
            id2 = Randomization.nextInt(0, VAllIds.size() - 1);
        } while (id1 == id2);
        String vId1 = VAllIds.get(id1), vId2 = VAllIds.get(id2);
        Query<C> firstQuery = makeQuery(String.format("MATCH path=(a)-[*]->(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId2));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery() + "result is: " + result.get(0).get("COUNT(path)"));
        if (result != null) {
            count1 = (Long) result.get(0).get("COUNT(path)");
        } else {
            System.out.println("2. Trigger exception");
            exceptions++;
        }

        // Query 3, add one more edge
        Query<C> secondQuery = makeQuery(String.format("MATCH (a),(b) WHERE ID(a)=%s AND ID(b)=%s CREATE (a)-[:OneMoreEdge]->(b)", vId1, vId2));
        result = secondQuery.executeAndGet(state);
        System.out.println("3." + secondQuery.getQuery());
        if (result == null) {
            System.out.println("3. Trigger exception");
            exceptions++;
        }

        // Query 4
        Query<C> thirdQuery = makeQuery(String.format("MATCH path=(a)-[*]->(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId2));
        result = thirdQuery.executeAndGet(state);
        System.out.println("4." + thirdQuery.getQuery() + "result is: " + result.get(0).get("COUNT(path)"));
        if (result != null) {
            count2 = (Long) result.get(0).get("COUNT(path)");
        } else {
            System.out.println("4. Trigger exception");
            exceptions++;
        }

        if ((count1 + 1) != count2) {
            System.out.println("Addition false\n");
            throw new AssertionError(String.format("Addition false. count1 is %s, count2 is %s.\n", count1, count2));
        } else {
            System.out.println(String.format("Addition true. count1 is %s, count2 is %s.\n", count1, count2));
        }

    }

    // add deprecated nodes and edges       NO
    public void check4() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();
        Long count1 = 0L, count2 = 0L;

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
        int id1 = 0, id2 = 0;
        do {
            id1 = Randomization.nextInt(0, VAllIds.size() - 1);
            id2 = Randomization.nextInt(0, VAllIds.size() - 1);
        } while (id1 == id2);
        String vId1 = VAllIds.get(id1), vId2 = VAllIds.get(id2);
        Query<C> firstQuery = makeQuery(String.format("MATCH path=(a)-[*]-(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId2));
        result = firstQuery.executeAndGet(state);
        System.out.println("2." + firstQuery.getQuery() + "result is: " + result.get(0).get("COUNT(path)"));
        if (result != null) {
            count1 = (Long) result.get(0).get("COUNT(path)");
        } else {
            System.out.println("2. Trigger exception");
            exceptions++;
        }

        // Query 3, add deprecated nodes and edges
        for (int i = 0; i < 10; i++) {
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

        // Query 4
        Query<C> thirdQuery = makeQuery(String.format("MATCH path=(a)-[*]-(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId2));
        result = thirdQuery.executeAndGet(state);
        System.out.println("3." + thirdQuery.getQuery() + "result is: " + result.get(0).get("COUNT(path)"));
        if (result != null) {
            count2 = (Long) result.get(0).get("COUNT(path)");
        } else {
            System.out.println("3. Trigger exception");
            exceptions++;
        }

        if (count1 != count2) {
            System.out.println("Redundant addition false\n");
            throw new AssertionError(String.format("Redundant addition false. count1 is %s, count2 is %s.\n", count1, count2));
        } else {
            System.out.println(String.format("Redundant addition true. count1 is %s, count2 is %s.\n", count1, count2));
        }
    }

    // multiply counts                      YES
    public void check3() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();

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

        Long count1 = 0L, count2 = 0L, count3 = 0L;
        // Query 2
        for(int id1 = 0; id1 < VAllIds.size(); id1++) {
            for (int id2 = 0; id2 < VAllIds.size(); id2++) {
                if (id2 == id1) continue;
                String vId1 = VAllIds.get(id1), vId2 = VAllIds.get(id2), vId3 = "";
                Query<C> firstQuery = makeQuery(String.format("MATCH path=(a)-[*]-(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId2));
                result = firstQuery.executeAndGet(state);
                System.out.println("2." + firstQuery.getQuery());
                if (result != null) {
                    count1 = (Long) result.get(0).get("COUNT(path)");
                } else {
                    System.out.println("2. Trigger exception");
                    exceptions++;
                }

                for (int id3 = 0; id3 < VAllIds.size(); id3++) {
                    if (id3 == id2 || id3 == id1) continue;
                    vId3 = VAllIds.get(id3);

                    Query<C> secondQuery = makeQuery(String.format("MATCH path=(a)-[*]-(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId2, vId3));
                    List<Map<String, Object>> result2 = secondQuery.executeAndGet(state);
                    System.out.println("3." + secondQuery.getQuery());
                    if (result2 != null) {
                        count2 = (Long) result2.get(0).get("COUNT(path)");
                    } else {
                        System.out.println("3. Trigger exception");
                        exceptions++;
                    }

                    Query<C> thirdQuery = makeQuery(String.format("MATCH path=(a)-[*]-(b)-[*]-(c) WHERE ID(a)=%s AND ID(b)=%s AND ID(c)=%s RETURN COUNT(path)", vId1, vId2, vId3));
                    List<Map<String, Object>> result3 = thirdQuery.executeAndGet(state);
                    System.out.println("4." + thirdQuery.getQuery());
                    if (result3 != null) {
                        count3 = (Long) result2.get(0).get("COUNT(path)");
                    } else {
                        System.out.println("4. Trigger exception");
                        exceptions++;
                    }

                    if (count1 * count2 != count3) {
                        System.out.println(String.format("Multiply false. %s * %s != %s. id1 is %s, id2 is %s, id3 is %s.\n", count1, count2, count3, vId1, vId2, vId3));
                        throw new AssertionError(String.format("Multiply false. %s * %s != %s. id1 is %s, id2 is %s, id3 is %s.\n", count1, count2, count3, vId1, vId2, vId3));
                    }
                }
            }
        }

    }

    // node a, b, c whether are connected   NO
    public void check2() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();

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
        for(int id1 = 0; id1 < VAllIds.size(); id1++) {
            for (int id2 = 0; id2 < VAllIds.size(); id2++) {
                if (id2 == id1) continue;
                String vId1 = VAllIds.get(id1), vId2 = VAllIds.get(id2), vId3 = "";
                Query<C> firstQuery = makeQuery(String.format("MATCH path=(a)-[*]->(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId2));
                result = firstQuery.executeAndGet(state);
                System.out.println("2." + firstQuery.getQuery());
                if (result != null) {
                    if ((Long) result.get(0).get("COUNT(path)") == 0) {

                        for (int id3 = 0; id3 < VAllIds.size(); id3++) {
                            if (id3 == id2 || id3 == id1) continue;
                            vId3 = VAllIds.get(id3);
                            Query<C> secondQuery = makeQuery(String.format("MATCH path=(a)-[*]->(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId2, vId3));
                            List<Map<String, Object>> result2 = secondQuery.executeAndGet(state);
                            System.out.println("3." + secondQuery.getQuery());
                            if (result2 != null) {
                                if ((Long) result2.get(0).get("COUNT(path)") == 0)
                                    continue;

                                Query<C> thirdQuery = makeQuery(String.format("MATCH path=(a)-[*]->(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId3));
                                List<Map<String, Object>> result3 = thirdQuery.executeAndGet(state);
                                System.out.println("4." + thirdQuery.getQuery());
                                if (result3 != null) {
                                    if ((Long) result3.get(0).get("COUNT(path)") == 0) {
                                        // Add edge
                                        Query<C> fourQuery = makeQuery(String.format("MATCH (a),(b) WHERE ID(a)=%s AND ID(b)=%s CREATE (a)-[:EDGE]->(b)", vId1, vId2));
                                        List<Map<String, Object>> result4 = fourQuery.executeAndGet(state);
                                        System.out.println("5." + fourQuery.getQuery());

                                        // Check result
                                        Query<C> fiveQuery = makeQuery(String.format("MATCH path=(a)-[*]->(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId3));
                                        List<Map<String, Object>> result5 = fiveQuery.executeAndGet(state);
                                        System.out.println("5." + fiveQuery.getQuery());
                                        if (result5 == null) {
                                            System.out.println("Connection false\n");
                                            throw new AssertionError(String.format("Connection false. id1 is %s, id2 is %s, id3 is %s. The respective results are: %s, %s, %s.\n",
                                                    vId1, vId2, vId3, result.get(0).get("COUNT(path)"), result2.get(0).get("COUNT(path)"), result5.get(0).get("COUNT(path)")));
                                        } else {
                                            if ((Long) result5.get(0).get("COUNT(path)") == 0) {
                                                System.out.println("Connection false\n");
                                                throw new AssertionError(String.format("Connection false. id1 is %s, id2 is %s, id3 is %s. The respective results are: %s, %s, %s.\n",
                                                        vId1, vId2, vId3, result.get(0).get("COUNT(path)"), result2.get(0).get("COUNT(path)"), result5.get(0).get("COUNT(path)")));
                                            }
                                        }
                                    }
                                } else {
                                    System.out.println("4. Trigger exception");
                                    exceptions++;
                                }
                            } else {
                                System.out.println("3. Trigger exception");
                                exceptions++;
                            }
                        }
                    }
                } else {
                    System.out.println("2. Trigger exception");
                    exceptions++;
                }
            }
        }
    }

    // node a, b, c connection              NO
    public void check1() {
        int exceptions = 0;
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);
        List<String> VAllIds = new ArrayList<>();

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
        for(int id1 = 0; id1 < VAllIds.size(); id1++) {
            for (int id2 = 0; id2 < VAllIds.size(); id2++) {
                if (id2 == id1) continue;
                String vId1 = VAllIds.get(id1), vId2 = VAllIds.get(id2), vId3 = "";
                Query<C> firstQuery = makeQuery(String.format("MATCH path=(a)-[*]-(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId2));
                result = firstQuery.executeAndGet(state);
                System.out.println("2." + firstQuery.getQuery() + "result is: " + result.get(0).get("COUNT(path)"));
                if (result != null) {
                    if ((Long) result.get(0).get("COUNT(path)") == 0)
                        continue;
                    for (int id3 = 0; id3 < VAllIds.size(); id3++) {
                        if (id3 == id2 || id3 == id1) continue;
                        vId3 = VAllIds.get(id3);
                        Query<C> secondQuery = makeQuery(String.format("MATCH path=(a)-[*]-(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId2, vId3));
                        List<Map<String, Object>> result2 = secondQuery.executeAndGet(state);
                        System.out.println("3." + secondQuery.getQuery() + "result is: " + result2.get(0).get("COUNT(path)"));
                        if (result2 != null) {
                            if ((Long) result2.get(0).get("COUNT(path)") == 0)
                                continue;

                            // Check
                            Query<C> thirdQuery = makeQuery(String.format("MATCH path=(a)-[*]-(b) WHERE ID(a)=%s AND ID(b)=%s RETURN COUNT(path)", vId1, vId3));
                            List<Map<String, Object>> result3 = thirdQuery.executeAndGet(state);
                            System.out.println("4." + thirdQuery.getQuery() + "result is: " + result3.get(0).get("COUNT(path)"));
                            if (result3 == null) {
                                System.out.println("Connection false\n");
                                throw new AssertionError(String.format("Connection false. id1 is %s, id2 is %s, id3 is %s. id1 is not connected to id3. The respective results are: %s, %s, %s.\n",
                                        vId1, vId2, vId3, result.get(0).get("COUNT(path)"), result2.get(0).get("COUNT(path)"), result3.get(0).get("COUNT(path)")));
                            } else {
                                if ((Long) result3.get(0).get("COUNT(path)") == 0) {
                                    System.out.println("Connection false\n");
                                    throw new AssertionError(String.format("Connection false. id1 is %s, id2 is %s, id3 is %s. id1 is not connected to id3. The respective results are: %s, %s, %s.\n",
                                            vId1, vId2, vId3, result.get(0).get("COUNT(path)"), result2.get(0).get("COUNT(path)"), result3.get(0).get("COUNT(path)")));
                                }
                            }
                        } else {
                            System.out.println("3." + secondQuery.getQuery());
                        }
                    }
                } else {
                    System.out.println("2." + firstQuery.getQuery());
                }
            }
        }
    }

    protected abstract CypherExpression getWhereClause(Entity<T> entity);

    protected abstract Query<C> makeQuery(String query);
}
