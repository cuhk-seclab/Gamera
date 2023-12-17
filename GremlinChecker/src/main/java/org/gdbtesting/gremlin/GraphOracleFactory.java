package org.gdbtesting.gremlin;

import org.gdbtesting.OracleFactory;
import org.gdbtesting.common.oracle.TestOracle;

import java.sql.SQLException;

public enum GraphOracleFactory implements OracleFactory<GraphGlobalState> {

    TLP_WHERE {
        @Override
        public TestOracle create(GraphGlobalState globalState) throws SQLException {
//                return new MySQLTLPWhereOracle(globalState);
            return null;
        }

    },
    PQS {
        @Override
        public TestOracle create(GraphGlobalState globalState) throws SQLException {
//                return new MySQLPivotedQuerySynthesisOracle(globalState);
            return null;
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }

    }
    // Add our new oracles here
}
