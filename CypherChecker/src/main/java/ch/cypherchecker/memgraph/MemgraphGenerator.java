package ch.cypherchecker.memgraph;

import ch.cypherchecker.common.Generator;
import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.memgraph.gen.*;
import ch.cypherchecker.memgraph.schema.MemgraphType;

import ch.cypherchecker.util.IgnoreMeException;
import ch.cypherchecker.util.Randomization;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public record MemgraphGenerator(Schema<MemgraphType> schema) implements Generator<MemgraphConnection> {

    enum Action {
        CREATE(MemgraphCreateGenerator::createEntities),
        CREATE_INDEX(MemgraphCreateIndexGenerator::createIndex),
        DROP_INDEX(MemgraphDropIndexGenerator::dropIndex),
        DELETE(MemgraphDeleteGenerator::deleteNodes),
        SET(MemgraphSetGenerator::setProperties),
        REMOVE(MemgraphRemoveGenerator::removeProperties);

        private final Function<Schema<MemgraphType>, MemgraphQuery> generator;

        Action(Function<Schema<MemgraphType>, MemgraphQuery> generator) {
            this.generator = generator;
        }
    }

    private static int mapAction(Action action) {
        return switch (action) {
            case CREATE -> Randomization.nextInt(20, 30);
            case DELETE, SET, REMOVE -> Randomization.nextInt(0, 8);
            case CREATE_INDEX -> Randomization.nextInt(3, 10);
            case DROP_INDEX -> Randomization.nextInt(2, 5);
        };
    }

    enum SimpleAction {
        CREATE(MemgraphCreateGenerator::createSimpleEntities),
        DELETE(MemgraphDeleteGenerator::deleteSimpleNodes),
        SET(MemgraphSetGenerator::setSimpleProperties),
        REMOVE(MemgraphRemoveGenerator::removeSimpleProperties);

        private final Function<Schema<MemgraphType>, MemgraphQuery> generator;

        SimpleAction(Function<Schema<MemgraphType>, MemgraphQuery> generator) {
            this.generator = generator;
        }
    }

    private static int mapSimpleAction(SimpleAction action) {
        return switch (action) {
            case CREATE -> Randomization.nextInt(50, 70);
            case DELETE, SET, REMOVE -> Randomization.nextInt(0, 8);
        };
    }

    @Override
    public void generate(GlobalState<MemgraphConnection> globalState) {
        List<Function<Schema<MemgraphType>, MemgraphQuery>> queries = new ArrayList<>();

        // Sample the actions
        for (Action action : Action.values()) {
            int amount = mapAction(action);

            for (int i = 0; i < amount; i++) {
                queries.add(action.generator);
            }
        }

        Randomization.shuffleList(queries);

        for (Function<Schema<MemgraphType>, MemgraphQuery> queryGenerator : queries) {
            try {
                int tries = 0;
                boolean success;
                MemgraphQuery query;

                do {
                    query = queryGenerator.apply(schema);
                    success = query.execute(globalState);
                } while (!success && tries++ < 1000);

                if (success && query.couldAffectSchema()) {
                    schema.setIndices(globalState.getConnection().getIndexNames());
                }
            } catch (IgnoreMeException ignored) {
            }
        }
    }

    @Override
    public void generateSimple(GlobalState<MemgraphConnection> globalState) {
        List<Function<Schema<MemgraphType>, MemgraphQuery>> queries = new ArrayList<>();

        for (SimpleAction action : SimpleAction.values()) {
            int amount = mapSimpleAction(action);

            for (int i = 0; i < amount; i++) {
                queries.add(action.generator);
            }
        }

        Randomization.shuffleList(queries);

        for (Function<Schema<MemgraphType>, MemgraphQuery> queryGenerator : queries) {
            try {
                int tries = 0;
                boolean success;
                MemgraphQuery query;

                do {
                    query = queryGenerator.apply(schema);
                    success = query.execute(globalState);
                } while (!success && tries++ < 1000);

                if (success && query.couldAffectSchema()) {
                    schema.setIndices(globalState.getConnection().getIndexNames());
                }
            } catch (IgnoreMeException ignored) {
            }
        }
    }

    @Override
    public void generateCustomized(GlobalState<MemgraphConnection> globalState) {

    }
}
