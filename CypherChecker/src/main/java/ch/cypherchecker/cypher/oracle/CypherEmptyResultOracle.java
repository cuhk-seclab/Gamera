package ch.cypherchecker.cypher.oracle;

import ch.cypherchecker.common.Connection;
import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.Oracle;
import ch.cypherchecker.common.Query;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.util.IgnoreMeException;
import ch.cypherchecker.util.Randomization;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class CypherEmptyResultOracle<C extends Connection, T> implements Oracle {

    private final GlobalState<C> state;
    private final Schema<T> schema;

    protected CypherEmptyResultOracle(GlobalState<C> state, Schema<T> schema) {
        this.state = state;
        this.schema = schema;
    }

    @Override
    public void check() {
        Set<Long> allIds = new HashSet<>();
        List<Map<String, Object>> idResult = getIdQuery().executeAndGet(state);

        for (Map<String, Object> properties : idResult) {
            allIds.add((Long) properties.get("id(n)"));
        }

        if (allIds.isEmpty()) {
            return;
        }

        String label = schema.getRandomLabel();
        Entity<T> entity = schema.getEntityByLabel(label);

        Query<C> initialQuery = getInitialQuery(label, entity);
        List<Map<String, Object>> initialResult = initialQuery.executeAndGet(state);

        if (initialResult == null) {
            throw new IgnoreMeException();
        }

        if (initialResult.isEmpty()) {
            for (int i = 0; i < Randomization.smallNumber() && !allIds.isEmpty(); i++) {
                Long chosenId = Randomization.fromSet(allIds);

                getDeleteQuery(chosenId).execute(state);
                allIds.remove(chosenId);
            }

            List<Map<String, Object>> result = initialQuery.executeAndGet(state);

            if (result == null) {
                throw new AssertionError("Empty oracle failed because the second query threw an exception");
            }

            if (result.size() != 0) {
                throw new AssertionError(String.format("Empty oracle failed with size: %d", result.size()));
            }
        }
    }

    protected abstract Query<C> getIdQuery();
    protected abstract Query<C> getInitialQuery(String label, Entity<T> entity);
    protected abstract Query<C> getDeleteQuery(Long id);

}
