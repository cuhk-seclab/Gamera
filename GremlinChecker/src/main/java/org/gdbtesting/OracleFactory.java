package org.gdbtesting;

import org.gdbtesting.common.oracle.TestOracle;

public interface OracleFactory<G> {

    TestOracle create(G globalState) throws Exception;

    /**
     * Indicates whether the test oracle requires that all tables (including views) contain at least one row.
     *
     * @return whether the test oracle requires at least one row per table
     */
    default boolean requiresAllTablesToContainRows() {
        return false;
    }

}
