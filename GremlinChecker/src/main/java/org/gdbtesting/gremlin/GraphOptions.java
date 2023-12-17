package org.gdbtesting.gremlin;

import com.beust.jcommander.Parameter;

import java.util.Arrays;
import java.util.List;

public class GraphOptions {

    @Parameter(names = "--oracle")
    public List<GraphOracleFactory> oracles = Arrays.asList(GraphOracleFactory.TLP_WHERE);

    public List<GraphOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
