package ch.cypherchecker.cypher.gen;

import ch.cypherchecker.cypher.CypherUtil;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.cypher.ast.CypherExpression;
import ch.cypherchecker.cypher.ast.CypherVisitor;
import ch.cypherchecker.util.IgnoreMeException;
import ch.cypherchecker.util.Randomization;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// Add create data with nodes/edges number
public abstract class CypherCreateGenerator<T> {

    private final Schema<T> schema;
    protected final StringBuilder query = new StringBuilder();

    private final Map<String, Entity<T>> nodeVariables = new HashMap<>();
    private final Map<String, Entity<T>> relationshipVariables = new HashMap<>();
    private final Set<String> aliasVariables = new HashSet<>();

    private boolean usesDistinctReturn = false;
    private final Set<String> distinctReturnedExpression = new HashSet<>();

    public CypherCreateGenerator(Schema<T> schema) {
        this.schema = schema;
    }

    protected void generateCreate() {
        query.append("CREATE ");

        int numberOfNodes = Randomization.nextInt(1, 6);
        String separator = "";

        for (int i = 0; i < numberOfNodes; i++) {
            query.append(separator);
            generateNode();
            separator = ", ";
        }

        for (String from : nodeVariables.keySet()) {
            for (String to : nodeVariables.keySet()) {
                if (Randomization.getBoolean()) {
                    query.append(", ");
                    generateRelationship(from, to);
                }
            }
        }

        if (Randomization.getBoolean()) {
            generateReturn();

            if (Randomization.getBoolean()) {
                generateOrderBy();
            }

            if (Randomization.getBoolean()) {
                generateLimit();
            }
        }
    }

    protected void generateCreateSimple() {
        query.append("CREATE ");

        int numberOfNodes = Randomization.nextInt(1, 6);
        String separator = "";

        for (int i = 0; i < numberOfNodes; i++) {
            query.append(separator);
            generateNode();
            separator = ", ";
        }

        for (String from : nodeVariables.keySet()) {
            for (String to : nodeVariables.keySet()) {
                if (Randomization.getBoolean()) {
                    query.append(", ");
                    generateRelationship(from, to);
                }
            }
        }
    }

    protected List<String> generateCustomizedCreate() {
        List<String> createQueries = new ArrayList<>();

        // Nodes/Edges number
        int nodeNum = 50;
        int edgeNum = 100;

        // Add nodes
        for (int i = 0; i < nodeNum; i++) {
            String query = "CREATE ";
            String separator = "";
            query += separator;

            String label = schema.getRandomLabel();
            Entity<T> nodeSchema = schema.getEntityByLabel(label);
            String name = getUniqueVariableName();

            query += String.format("(%s:%s ", name, label);
            query += getPropertyGenerator(nodeSchema, getAllVariables()).generatePropertiesForString();
            query += ")";

            // Add variable at this point so that we don't reference it before
            nodeVariables.put(name, nodeSchema);

            createQueries.add(query);
        }

        // Add edges
        for (String from : nodeVariables.keySet()) {
            for (String to : nodeVariables.keySet()) {
                if (Randomization.getBoolean()) {
                    String query = "CREATE ";

                    String type = schema.getRandomType();
                    Entity<T> relationshipSchema = schema.getEntityByType(type);
                    String name = getUniqueVariableName();

                    query += String.format("(%s)-[%s:%s ", from, name, type);
                    String properties = "";
                    do {
                        try {
                            properties = getPropertyGenerator(relationshipSchema, getAllVariables()).generatePropertiesForString();
                        } catch (Exception e) {
                            System.out.println("Property generator for edges cause ignored exception.");
                        }
                    } while (properties.equals(""));
                    query += properties;
                    query += String.format("]->(%s)", to);

                    // Add variable at this point so that we don't reference it before
                    relationshipVariables.put(name, relationshipSchema);

                    createQueries.add(query);
                }
            }
        }

        return createQueries;
    }

    private void generateReturn() {
        if (Randomization.getBoolean()) {
            query.append(" RETURN * ");
            return;
        }

        query.append(" RETURN ");

        if (Randomization.getBoolean()) {
            query.append("DISTINCT ");
            usesDistinctReturn = true;
        }

        Map<String, Entity<T>> allVariables = getAllVariables();
        Set<String> variables = Randomization.nonEmptySubset(allVariables.keySet());
        String separator = "";

        for (String variable : variables) {
            query.append(separator);
            query.append(variable);

            if (Randomization.getBoolean()) {
                Entity<T> entity = allVariables.get(variable);
                String property = Randomization.fromSet(entity.availableProperties().keySet());

                query.append(".");
                query.append(property);
                query.append(" ");

                if (usesDistinctReturn) {
                    distinctReturnedExpression.add(variable + "." + property);
                }

                if (Randomization.getBoolean()) {
                    String alias = getUniqueVariableName();
                    query.append(String.format("AS %s ", alias));
                    aliasVariables.add(alias);
                }
            } else {
                query.append(" ");

                if (usesDistinctReturn) {
                    distinctReturnedExpression.add(variable);
                }
            }

            separator = ", ";
        }
    }

    private void generateOrderBy() {
        query.append("ORDER BY ");

        if (usesDistinctReturn) {
            Set<String> expressions = Randomization.nonEmptySubset(distinctReturnedExpression);
            String separator = "";

            for (String expression : expressions) {
                query.append(separator);
                query.append(expression);
                query.append(" ");

                if (Randomization.getBoolean()) {
                    if (Randomization.getBoolean()) {
                        query.append("DESC ");
                    } else {
                        query.append("DESCENDING ");
                    }
                }

                separator = ", ";
            }

            return;
        }

        Map<String, Entity<T>> allVariables = getAllVariables();
        Set<String> variables = Randomization.nonEmptySubset(allVariables.keySet());
        String separator = "";

        for (String variable : variables) {
            query.append(separator);
            query.append(variable);

            if (Randomization.getBoolean()) {
                Entity<T> entity = allVariables.get(variable);
                String property = Randomization.fromSet(entity.availableProperties().keySet());

                query.append(".");
                query.append(property);
                query.append(" ");

                if (Randomization.getBoolean()) {
                    if (Randomization.getBoolean()) {
                        query.append("DESC ");
                    } else {
                        query.append("DESCENDING ");
                    }
                }
            }

            separator = ", ";
        }
    }

    private void generateLimit() {
        query.append(" LIMIT ");
        query.append(Randomization.getPositiveInteger());
    }

    private void generateRelationship(String from, String to) {
        String type = schema.getRandomType();
        Entity<T> relationshipSchema = schema.getEntityByType(type);
        String name = getUniqueVariableName();

        query.append(String.format("(%s)-[%s:%s ", from, name, type));
        query.append(getPropertyGenerator(relationshipSchema, getAllVariables()).generateProperties());
        query.append(String.format("]->(%s)", to));

        // Add variable at this point so that we don't reference it before
        relationshipVariables.put(name, relationshipSchema);
    }

    private void generateNode() {
        String label = schema.getRandomLabel();
        Entity<T> nodeSchema = schema.getEntityByLabel(label);
        String name = getUniqueVariableName();

        query.append(String.format("(%s:%s ", name, label));
        query.append(getPropertyGenerator(nodeSchema, getAllVariables()).generateProperties());
        query.append(")");

        // Add variable at this point so that we don't reference it before
        nodeVariables.put(name, nodeSchema);
    }

    private String getUniqueVariableName() {
        String name;

        do {
            name = CypherUtil.generateValidName();
        } while (nodeVariables.containsKey(name)
                || relationshipVariables.containsKey(name)
                || aliasVariables.contains(name));

        return name;
    }

    private Map<String, Entity<T>> getAllVariables() {
        return Stream.of(nodeVariables, relationshipVariables)
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    protected abstract CypherPropertyGenerator<T> getPropertyGenerator(Entity<T> entity, Map<String, Entity<T>> variables);

}
