package ch.cypherchecker.neo4j;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.QueryReplay;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Neo4JQueryReplay extends QueryReplay {

    @Override
    protected void executeQueries(List<String> queries) {
        GlobalState<Neo4JConnection> state = new GlobalState<>();

        ExpectedErrors errors = new ExpectedErrors();
        Neo4JUtil.addFunctionErrors(errors);
        Neo4JUtil.addArithmeticErrors(errors);
        Neo4JUtil.addRegexErrors(errors);

        errors.add("There already exists an index");
        errors.add("An equivalent index already exists");

        try (Neo4JConnection connection = new Neo4JConnection()) {
            connection.connect();
            state.setConnection(connection);

            for (String query : queries) {
                List<Map<String, Object>> result = new Neo4JQuery(query, errors).executeAndGet(state);
                state.appendToLog(result == null ? "null" : result.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            state.logCurrentExecution();
            state.clearLog();
        }
    }

}
