package org.gdbtesting.common;

import org.gdbtesting.GraphDBConnection;

public interface GraphDBProvider<G, O, C extends GraphDBConnection> {


    Class<G> getGlobalStateClass();

    /**
     * Gets the JCommander option class.
     *
     * @return the class representing the DBMS-specific options.
     */
    Class<O> getOptionClass();

    /**
     * Generates a single database and executes a test oracle a given number of times.
     *
     * @param globalState the state created and is valid for this method call.
     * @throws Exception if creating the database fails.
     */
    void generateAndTestDatabase(G globalState) throws Exception;

    /**
     * Generates a single database and executes a test oracle a given number of times.
     *
     * @param globalState the state created and is valid for this method call.
     * @throws Exception if creating the database fails.
     */
    void generateAndTestDatabaseWithRules(G globalState) throws Exception;

    // When you need, then can add new function and also implement the related files
    // C createDatabase(G globalState) throws Exception;

    /**
     * The DBMS name is used to name the log directory and command to test the respective DBMS.
     *
     * @return the DBMS' name
     */
//    String getDBMSName();

}
