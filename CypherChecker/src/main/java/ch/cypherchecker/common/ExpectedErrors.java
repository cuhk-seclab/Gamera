package ch.cypherchecker.common;

import ch.cypherchecker.neo4j.Neo4JBugs;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class ExpectedErrors {

    private final Set<String> errors = new HashSet<>();
    private final Set<String> regexErrors = new HashSet<>();

    public void add(String error) {
        errors.add(error);
    }

    public void addRegex(String error) {
        regexErrors.add(error);
    }

    /**
     * Checks if an exception is expected or not.
     * An exception is considered expected if:
     * - It can occur because of semantics of the query which cannot be avoided
     * - It is a known bug that has not been fixed yet.
     *
     * @param exception The exception to check.
     * @return          Whether the exception is expected.
     */
    public boolean isExpected(Exception exception) {
        String message = exception.getMessage();

        if (Neo4JBugs.bug2 && exception instanceof IndexOutOfBoundsException) {
            return true;
        }

        if (message == null) {
            return false;
        }

        if (exception instanceof QueryExecutionException) {
            if (Neo4JBugs.bug4) {
                if (message.startsWith("Node with id") && message.endsWith("has been deleted in this transaction")) {
                    return true;
                }
            }
        }

        for (String error : errors) {
            if (message.contains(error)) {
                return true;
            }
        }

        for (String error : regexErrors) {
            if (Pattern.matches(error, message)) {
                return true;
            }
        }

        return false;
    }

}
