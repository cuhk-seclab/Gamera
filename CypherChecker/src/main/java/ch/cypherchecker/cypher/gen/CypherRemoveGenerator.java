package ch.cypherchecker.cypher.gen;

import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.util.Randomization;

import java.util.Map;

public abstract class CypherRemoveGenerator<T> {

    private final Schema<T> schema;
    protected final StringBuilder query = new StringBuilder();

    public CypherRemoveGenerator(Schema<T> schema) {
        this.schema = schema;
    }

    protected void generateRemove() {
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);

        query.append(String.format("MATCH (n:%s) WHERE %s", label, generateWhereClause(entity)));

        String property = Randomization.fromSet(entity.availableProperties().keySet());
        query.append(generateRemoveClause(property));

        if (Randomization.getBoolean()) {
            query.append(" ");
            query.append(CypherReturnGenerator.returnEntities(Map.of("n", entity)));
        }
    }

    protected void generateSimpleRemove() {
        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);

        query.append(String.format("MATCH (n:%s) WHERE %s", label, generateWhereClause(entity)));

        String property = Randomization.fromSet(entity.availableProperties().keySet());
        query.append(generateRemoveClause(property));
    }

    protected abstract String generateWhereClause(Entity<T> entity);
    protected abstract String generateRemoveClause(String property);

}
