package ch.cypherchecker.redis;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.StringQuery;

import java.util.List;
import java.util.Map;

public class RedisQuery extends StringQuery<RedisConnection> {

    public RedisQuery(String query) {
        super(query);
    }

    public RedisQuery(String query, boolean couldAffectSchema) {
        super(query, couldAffectSchema);
    }

    public RedisQuery(String query, ExpectedErrors expectedErrors) {
        super(query, expectedErrors);
    }

    public RedisQuery(String query, ExpectedErrors expectedErrors, boolean couldAffectSchema) {
        super(query, expectedErrors, couldAffectSchema);
    }

    @Override
    public boolean execute(GlobalState<RedisConnection> globalState) {
        RedisConnection connection = globalState.getConnection();
        globalState.appendToLog(getQuery());
        System.out.println("INFO - " + getQuery());

        try {
            connection.execute(this);
            return true;
        } catch (Exception exception) {
            if (getQuery().startsWith("CREATE")) {
                return false;
            }
            checkException(exception);
            return false;
        }
    }

    @Override
    public List<Map<String, Object>> executeAndGet(GlobalState<RedisConnection> globalState) {
        RedisConnection connection = globalState.getConnection();
        globalState.appendToLog(getQuery());

        try {
            return connection.execute(this);
        } catch (Exception exception) {
            checkException(exception);
        }

        return null;
    }

    // Specify some expected/unexpected exceptions
    private void checkException(Exception e) throws AssertionError {
        if (!getExpectedErrors().isExpected(e)) {
            throw new AssertionError(getQuery(), e);
        }
    }
}
