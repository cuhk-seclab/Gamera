package org.gdbtesting.gremlin.exception;

public class NotExistingEexception extends RuntimeException {
    public NotExistingEexception(String key) {
        super("is not existing.");
    }
}
