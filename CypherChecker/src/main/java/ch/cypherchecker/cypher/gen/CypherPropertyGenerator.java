package ch.cypherchecker.cypher.gen;

import ch.cypherchecker.cypher.ast.CypherExpression;
import ch.cypherchecker.cypher.ast.CypherVisitor;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.util.Randomization;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class CypherPropertyGenerator<T> {

    private final Entity<T> entity;
    private final Map<String, Entity<T>> variables;
    private final StringBuilder query = new StringBuilder();

    protected CypherPropertyGenerator(Entity<T> entity) {
        this.entity = entity;
        this.variables = new HashMap<>();
    }

    protected CypherPropertyGenerator(Entity<T> entity, Map<String, Entity<T>> variables) {
        this.entity = entity;
        this.variables = variables;
    }

    public String generateProperties() {
        Map<String, T> availableProperties = entity.availableProperties();
        Set<String> selectedProperties = Randomization.nonEmptySubset(availableProperties.keySet());

        if (selectedProperties.isEmpty()) {
            return "";
        }

        query.append("{");
        String delimiter = "";

        for (String property : selectedProperties) {
            query.append(delimiter);
            generateProperty(property, availableProperties.get(property));
            delimiter = ", ";

        }

        query.append("}");
        return query.toString();
    }

    public String generatePropertiesForString() {
        String resStr = "";

        Map<String, T> availableProperties = entity.availableProperties();
        Set<String> selectedProperties = Randomization.nonEmptySubset(availableProperties.keySet());

        if (selectedProperties.isEmpty()) {
            return "";
        }

        resStr += "{";
        String delimiter = "";

        for (String property : selectedProperties) {
            resStr += delimiter;
            resStr += generatePropertyForString(property, availableProperties.get(property));
            delimiter = ", ";
        }

        resStr += "}";
        return resStr;
    }

    private void generateProperty(String name, T type) {
        query.append(String.format("%s:", name));

        CypherExpression expression;

        if (Randomization.getBoolean()) {
            expression = generateConstant(type);
        } else {
            expression = generateExpression(variables, type);
        }

        query.append(CypherVisitor.asString(expression));
    }

    private String generatePropertyForString(String name, T type) {
        String resStr = "";

        resStr += String.format("%s:", name);

        CypherExpression expression;

        if (Randomization.getBoolean()) {
            expression = generateConstant(type);
        } else {
            expression = generateExpression(variables, type);
        }

        resStr += CypherVisitor.asString(expression);
        return resStr;
    }

    protected abstract CypherExpression generateConstant(T type);
    protected abstract CypherExpression generateExpression(Map<String, Entity<T>> variables, T type);

}
