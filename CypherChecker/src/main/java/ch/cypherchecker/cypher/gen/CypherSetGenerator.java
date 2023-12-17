package ch.cypherchecker.cypher.gen;

import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.cypher.ast.CypherExpression;
import ch.cypherchecker.cypher.ast.CypherVisitor;
import ch.cypherchecker.util.Randomization;

import java.util.Map;
import java.util.Set;

public abstract class CypherSetGenerator<T> {

    private final Schema<T> schema;
    protected final StringBuilder query = new StringBuilder();

    public CypherSetGenerator(Schema<T> schema) {
        this.schema = schema;
    }

    protected void generateSet() {
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);

        query.append(String.format("MATCH (n:%s)", label));
        query.append(" WHERE ");
        query.append(generateWhereClause(entity));

        if (Randomization.smallBiasProbability()) {
            query.append(" SET n = {}");
        } else {
            Set<String> properties = entity.availableProperties().keySet();
            String property = Randomization.fromSet(properties);
            T type = entity.availableProperties().get(property);
            CypherExpression expression;

            if (Randomization.getBoolean()) {
                expression = generateConstant(type);
            } else {
                expression = generateExpression(type);
            }

            query.append(String.format(" SET n.%s = %s", property, CypherVisitor.asString(expression)));
        }

        if (Randomization.getBoolean()) {
            query.append(" ");
            query.append(CypherReturnGenerator.returnEntities(Map.of("n", entity)));
        }
    }

    protected void generateSimpleSet() {
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);

        query.append(String.format("MATCH (n:%s)", label));
        query.append(" WHERE ");
        query.append(generateWhereClause(entity));

        if (Randomization.smallBiasProbability()) {
            query.append(" SET n = {}");
        } else {
            Set<String> properties = entity.availableProperties().keySet();
            String property = Randomization.fromSet(properties);
            T type = entity.availableProperties().get(property);
            CypherExpression expression;

            if (Randomization.getBoolean()) {
                expression = generateConstant(type);
            } else {
                expression = generateExpression(type);
            }

            query.append(String.format(" SET n.%s = %s", property, CypherVisitor.asString(expression)));
        }
    }

    protected abstract String generateWhereClause(Entity<T> entity);
    protected abstract CypherExpression generateConstant(T type);
    protected abstract CypherExpression generateExpression(T type);


}
