package ch.cypherchecker.neo4j;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.common.GlobalState;
import ch.cypherchecker.common.StringQuery;

import java.util.List;
import java.util.Map;

public class Neo4JQuery extends StringQuery<Neo4JConnection> {

    public Neo4JQuery(String query) {
        super(query);
    }

    public Neo4JQuery(String query, boolean couldAffectSchema) {
        super(query, couldAffectSchema);
    }

    public Neo4JQuery(String query, ExpectedErrors expectedErrors) {
        super(query, expectedErrors);
    }

    public Neo4JQuery(String query, ExpectedErrors expectedErrors, boolean couldAffectSchema) {
        super(query, expectedErrors, couldAffectSchema);
    }

    @Override
    public boolean execute(GlobalState<Neo4JConnection> globalState) {
        Neo4JConnection connection = globalState.getConnection();
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
    public List<Map<String, Object>> executeAndGet(GlobalState<Neo4JConnection> globalState) {
        Neo4JConnection connection = globalState.getConnection();
        globalState.appendToLog(getQuery());

        try {
            return connection.execute(this);
        } catch (Exception exception) {
            checkException(exception);
        } catch (NoSuchMethodError error) {
            if (Neo4JBugs.bug3) {
                if (error.getMessage().equals("'int org.apache.lucene.search.IndexSearcher.getMaxClauseCount()'")) {
                    return null;
                }
            }

            throw error;
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
