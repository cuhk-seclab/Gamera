package ch.cypherchecker.redis;

import ch.cypherchecker.common.Generator;
import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.neo4j.Neo4JGenerator;
import ch.cypherchecker.neo4j.Neo4JQuery;
import ch.cypherchecker.neo4j.gen.Neo4JCreateGenerator;
import ch.cypherchecker.neo4j.schema.Neo4JType;
import ch.cypherchecker.redis.gen.*;
import ch.cypherchecker.redis.schema.RedisType;
import ch.cypherchecker.util.IgnoreMeException;
import ch.cypherchecker.util.Randomization;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public record RedisGenerator(
        Schema<RedisType> schema) implements Generator<RedisConnection> {

    enum CreateAction {
        CREATE(RedisCreateGenerator::createEntitiesList);

        private final Function<Schema<RedisType>, List<RedisQuery>> generator;

        CreateAction(Function<Schema<RedisType>, List<RedisQuery>> generator) {
            this.generator = generator;
        }
    }

    enum Action {
        CREATE(RedisCreateGenerator::createEntities),
        REMOVE(RedisRemoveGenerator::removeProperties),
        CREATE_INDEX(RedisCreateIndexGenerator::createIndex),
        DROP_INDEX(RedisDropIndexGenerator::dropIndex),
        SET(RedisSetGenerator::setProperties),
        DELETE(RedisDeleteGenerator::deleteNodes);

        private final Function<Schema<RedisType>, RedisQuery> generator;

        Action(Function<Schema<RedisType>, RedisQuery> generator) {
            this.generator = generator;
        }
    }

    private static int mapCreateAction(RedisGenerator.CreateAction action) {
        return switch (action) {
//            case CREATE -> Randomization.nextInt(1, 2);
            case CREATE -> 1;
        };
    }

    private static int mapAction(Action action) {
        return switch (action) {
            case CREATE -> Randomization.nextInt(50, 70);
            case CREATE_INDEX -> Randomization.nextInt(3, 10);
            case REMOVE, SET, DELETE -> Randomization.nextInt(0, 8);
            case DROP_INDEX -> Randomization.nextInt(2, 5);
        };
    }

    @Override
    public void generate(GlobalState<RedisConnection> globalState) {
        List<Function<Schema<RedisType>, RedisQuery>> queries = new ArrayList<>();

        // Sample the actions
        for (Action action : Action.values()) {
            int amount = mapAction(action);

            for (int i = 0; i < amount; i++) {
                queries.add(action.generator);
            }
        }

        Randomization.shuffleList(queries);

        for (Function<Schema<RedisType>, RedisQuery> queryGenerator : queries) {
            try {
                int tries = 0;
                boolean success;
                RedisQuery query;

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
    public void generateSimple(GlobalState<RedisConnection> globalState) {

    }

    @Override
    public void generateCustomized(GlobalState<RedisConnection> globalState) {
        List<Function<Schema<RedisType>, List<RedisQuery>>> createQueries = new ArrayList<>();
        List<Function<Schema<RedisType>, RedisQuery>> queries = new ArrayList<>();

        for (RedisGenerator.CreateAction createAction : RedisGenerator.CreateAction.values()) {
            int amount = mapCreateAction(createAction);

            for (int i = 0; i < amount; i++) {
                createQueries.add(createAction.generator);
            }
        }

        for (Function<Schema<RedisType>, List<RedisQuery>> queryGenerator : createQueries) {
            int tries = 0;
            boolean success;
            List<RedisQuery> queryList = null;

            do {
                try {
                    queryList = queryGenerator.apply(schema);
                } catch (IgnoreMeException ignored) {
                }
            } while (queryList == null);
            int successCount = 0, failCount = 0;
            for (int i = 0; i < queryList.size(); i++) {
                RedisQuery query = queryList.get(i);
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

        for (RedisGenerator.Action action : RedisGenerator.Action.values()) {
            int amount = mapAction(action);

            for (int i = 0; i < amount; i++) {
                queries.add(action.generator);
            }
        }

        Randomization.shuffleList(queries);

        for (Function<Schema<RedisType>, RedisQuery> queryGenerator : queries) {
            try {
                int tries = 0;
                boolean success;
                RedisQuery query;

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

}
