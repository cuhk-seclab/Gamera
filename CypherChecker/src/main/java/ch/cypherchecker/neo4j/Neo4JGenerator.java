package ch.cypherchecker.neo4j;

import ch.cypherchecker.common.Generator;
import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.neo4j.gen.*;
import ch.cypherchecker.neo4j.schema.Neo4JType;
import ch.cypherchecker.util.IgnoreMeException;
import ch.cypherchecker.util.Randomization;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public record Neo4JGenerator(
        Schema<Neo4JType> schema) implements Generator<Neo4JConnection> {

    enum CreateAction {
        CREATE(Neo4JCreateGenerator::createEntitiesList);

        private final Function<Schema<Neo4JType>, List<Neo4JQuery>> generator;

        CreateAction(Function<Schema<Neo4JType>, List<Neo4JQuery>> generator) {
            this.generator = generator;
        }
    }

    private static int mapCreateAction(CreateAction action) {
        return switch (action) {
//            case CREATE -> Randomization.nextInt(1, 2);
            case CREATE -> 1;
        };
    }

    enum Action {
        CREATE(Neo4JCreateGenerator::createEntities),
        CREATE_INDEX(Neo4JCreateIndexGenerator::createIndex),
        DROP_INDEX(Neo4JDropIndexGenerator::dropIndex),
        SHOW_FUNCTIONS(Neo4JShowFunctionsGenerator::showFunctions),
        SHOW_PROCEDURES(Neo4JShowProceduresGenerator::showProcedures),
        SHOW_TRANSACTIONS(Neo4JShowTransactionsGenerator::showTransactions),
        DELETE(Neo4JDeleteGenerator::deleteNodes),
        SET(Neo4JSetGenerator::setProperties),
        REMOVE(Neo4JRemoveGenerator::removeProperties);

        private final Function<Schema<Neo4JType>, Neo4JQuery> generator;

        Action(Function<Schema<Neo4JType>, Neo4JQuery> generator) {
            this.generator = generator;
        }
    }

    private static int mapAction(Action action) {
        return switch (action) {
            case CREATE -> Randomization.nextInt(20, 30);
            case DELETE, SET, REMOVE -> Randomization.nextInt(0, 8);
            case CREATE_INDEX -> Randomization.nextInt(3, 10);
            case DROP_INDEX, SHOW_FUNCTIONS, SHOW_PROCEDURES, SHOW_TRANSACTIONS -> Randomization.nextInt(2, 5);
        };
    }

    enum SimpleAction {
        CREATE(Neo4JCreateGenerator::createSimpleEntities),
        DELETE(Neo4JDeleteGenerator::deleteSimpleNodes),
        SET(Neo4JSetGenerator::setSimpleProperties),
        REMOVE(Neo4JRemoveGenerator::removeSimpleProperties);

        private final Function<Schema<Neo4JType>, Neo4JQuery> generator;

        SimpleAction(Function<Schema<Neo4JType>, Neo4JQuery> generator) {
            this.generator = generator;
        }
    }

    private static int mapSimpleAction(SimpleAction action) {
        return switch (action) {
            case CREATE -> Randomization.nextInt(50, 70);
            case DELETE, SET, REMOVE -> Randomization.nextInt(0, 8);
        };
    }

    public void generate(GlobalState<Neo4JConnection> globalState) {
        List<Function<Schema<Neo4JType>, Neo4JQuery>> queries = new ArrayList<>();

        // Sample the actions
        for (Action action : Action.values()) {
            int amount = mapAction(action);

            for (int i = 0; i < amount; i++) {
                queries.add(action.generator);
            }
        }

        Randomization.shuffleList(queries);

        for (Function<Schema<Neo4JType>, Neo4JQuery> queryGenerator : queries) {
            try {
                int tries = 0;
                boolean success;
                Neo4JQuery query;

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
    public void generateSimple(GlobalState<Neo4JConnection> globalState) {
        List<Function<Schema<Neo4JType>, Neo4JQuery>> queries = new ArrayList<>();

        for (SimpleAction action : SimpleAction.values()) {
            int amount = mapSimpleAction(action);

            for (int i = 0; i < amount; i++) {
                queries.add(action.generator);
            }
        }

        Randomization.shuffleList(queries);

        for (Function<Schema<Neo4JType>, Neo4JQuery> queryGenerator : queries) {
            try {
                int tries = 0;
                boolean success;
                Neo4JQuery query;

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

    public void generateCustomized(GlobalState<Neo4JConnection> globalState) {
        List<Function<Schema<Neo4JType>, List<Neo4JQuery>>> createQueries = new ArrayList<>();
        List<Function<Schema<Neo4JType>, Neo4JQuery>> queries = new ArrayList<>();

        for (CreateAction createAction : CreateAction.values()) {
            int amount = mapCreateAction(createAction);

            for (int i = 0; i < amount; i++) {
                createQueries.add(createAction.generator);
            }
        }

        for (Function<Schema<Neo4JType>, List<Neo4JQuery>> queryGenerator : createQueries) {
            int tries = 0;
            boolean success;
            List<Neo4JQuery> queryList = null;

            do {
                try {
                    queryList = queryGenerator.apply(schema);
                } catch (IgnoreMeException ignored) {
                }
            } while (queryList == null);
            int successCount = 0, failCount = 0;
            for (int i = 0; i < queryList.size(); i++) {
                Neo4JQuery query = queryList.get(i);
                success = query.execute(globalState);
//                System.out.println("Execute " + success + ": " + query.getQuery());
                if (success == true) successCount++;
                else if (success == false) failCount++;

                if (success && query.couldAffectSchema()) {
                    schema.setIndices(globalState.getConnection().getIndexNames());
                }
            }
            System.out.println("Create queries succeed: " + successCount);
            System.out.println("Create queries fail: " + failCount);
        }

        for (Action action : Action.values()) {
            int amount = mapAction(action);

            for (int i = 0; i < amount; i++) {
                queries.add(action.generator);
            }
        }

        Randomization.shuffleList(queries);

        for (Function<Schema<Neo4JType>, Neo4JQuery> queryGenerator : queries) {
            try {
                int tries = 0;
                boolean success;
                Neo4JQuery query;

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

        System.out.println("DEBUG");
    }
}
