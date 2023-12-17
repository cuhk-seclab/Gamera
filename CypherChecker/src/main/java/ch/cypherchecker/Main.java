package ch.cypherchecker;

import ch.cypherchecker.common.*;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.memgraph.MemgraphProvider;
import ch.cypherchecker.neo4j.Neo4JProvider;
import ch.cypherchecker.redis.RedisProvider;
import ch.cypherchecker.util.IgnoreMeException;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.nio.file.FileSystems;

public class Main {

    @Parameters(separators = "=")
    static
    class Options {

        @Parameter(required = true)
        private String databaseName;

        @Parameter(names = {"--oracle", "--method", "-o"}, description = "The oracle that should be executed on the database")
        private OracleType oracleType;

        @SuppressWarnings("FieldMayBeFinal")
        @Parameter(names = {"--reproduce", "--replay", "-r"}, description = "Whether the queries under logs/replay should be ran or not")
        private boolean reproduce = false;

        @Parameter(names = {"--verbose", "-v"}, description = "Whether all queries should be logged or not")
        private boolean verbose = false;

        @SuppressWarnings("FieldMayBeFinal")
        @Parameter(names = {"--help", "-h"}, description = "Lists all supported options", help = true)
        private boolean help = false;

    }
    public static void main(String[] args) throws Exception {

//        String[] mockArgs = {"neo4j", "--oracle", "empty_result", "--verbose"};
        String[] mockArgs = {"neo4j", "--oracle", "non_empty_result", "--verbose"};
//        String[] mockArgs = {"neo4j", "--oracle", "partition", "--verbose"};
//        String[] mockArgs = {"neo4j", "--oracle", "refinement", "--verbose"};

//        String[] mockArgs = {"neo4j", "--oracle", "path", "--verbose"};
//        String[] mockArgs = {"neo4j", "--oracle", "node", "--verbose"};
//        String[] mockArgs = {"neo4j", "--oracle", "edge", "--verbose"};

//        String[] mockArgs = {"redis", "--oracle", "path", "--verbose"};
//        String[] mockArgs = {"redis", "--oracle", "node", "--verbose"};
//        String[] mockArgs = {"redis", "--oracle", "edge", "--verbose"};

//        String[] mockArgs = {"memgraph", "--oracle", "path", "--verbose"};

        args = mockArgs;
        Options options = new Options();

        JCommander jc = JCommander.newBuilder()
                .addObject(options)
                .programName("CypherChecker")
                .build();

        jc.parse(args);

        if (options.help) {
            jc.usage();
            return;
        }

        Provider<?, ?> provider;

        switch (options.databaseName.toLowerCase()) {
            case "neo4j" -> provider = new Neo4JProvider();
            case "redis" -> provider = new RedisProvider();
            case "memgraph" -> provider = new MemgraphProvider();
            default -> {
                System.err.println("Unknown database, please use either neo4j, redis or memgraph");
                System.exit(1);
                return;
            }
        }

        System.out.printf("""
                   ______            __              ________              __           \s
                  / ____/_  ______  / /_  ___  _____/ ____/ /_  ___  _____/ /_____  _____
                 / /   / / / / __ \\/ __ \\/ _ \\/ ___/ /   / __ \\/ _ \\/ ___/ //_/ _ \\/ ___/
                / /___/ /_/ / /_/ / / / /  __/ /  / /___/ / / /  __/ /__/ ,< /  __/ /   \s
                \\____/\\__, / .___/_/ /_/\\___/_/   \\____/_/ /_/\\___/\\___/_/|_|\\___/_/    \s
                     /____/_/                                                           \s
                                Version: 1.0
                Selected Database: %s

                """, options.databaseName);

        if (options.reproduce) {
            replayQueries(provider);
        } else {
            if (options.oracleType == null) {
                System.err.println("Select an oracle to execute");
                System.exit(1);
            }

            run(provider, options.oracleType, options.verbose);
        }
    }

    private static void replayQueries(Provider<?, ?> provider) throws IOException {
        provider.getQueryReplay().replayFromFile(FileSystems.getDefault().getPath("logs/replay").toFile());
    }

    private static <C extends Connection, T> void run(Provider<C, T> provider, OracleType oracleType, boolean verbose) {
        GlobalState<C> state = new GlobalState<>();
        OracleFactory<C, T> factory = provider.getOracleFactory();

        while (true) {
            state.clearLog();

            try (C connection = provider.getConnection()) {
                connection.connect();
                state.setConnection(connection);

                // 1. Graph schema
                Schema<T> schema = provider.getSchema();
                Oracle oracle = factory.createOracle(oracleType, state, schema);
                oracle.onGenerate();

                // 2. Graph data
                // Do twice, both generate(), generateSimple() and generateCustomized()
                provider.getGenerator(schema).generate(state);
//                provider.getGenerator(schema).generateSimple(state);
//                provider.getGenerator(schema).generateCustomized(state);

                try {
                    oracle.onStart();

                    for (int i = 0; i < 100; i++) {
                        try {
                            System.out.println("Check oracle " + i);
                            oracle.check();     // Here's where the Metamorphic Testing for inconsistency happens.
                        } catch (IgnoreMeException ignored) {
                        }
                    }
                } finally {
                    oracle.onComplete();
                }

                if (verbose) {
                    state.logCurrentExecution();
                }
            } catch (Throwable throwable) {
                // Throw an exception then break
                state.appendToLog(ExceptionUtils.getStackTrace(throwable));
                state.logCurrentExecution();
                break;
            }
        }
    }

}
