package org.gdbtesting;

import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GremlinGraphProvider;
import java.io.IOException;

public class Starter {

    public static void main(String[] args) throws IOException {
        // Fine tuning
        // String[] mockArgs = {"5", "100", "200", "20", "20", "100"};
        String[] mockArgs = {"5", "100", "200", "5", "5", "1000"};
        // String[] mockArgs = {"5", "100", "200", "5", "5", "300"};
        args = mockArgs;
        if (args.length != 6)
            System.out.println("Missing Parameters! 1.QueryDepth, 2.VerMaxNum, 3.EdgeMaxNum, 4.EdgeLabelNum, 5.VerLabelNum, 6.QueryNum.");
        GraphGlobalState state = new GraphGlobalState(Integer.parseInt(args[0]));

        state.setVerticesMaxNum(Integer.parseInt(args[1]));
        state.setEdgesMaxNum(Integer.parseInt(args[2]));
        state.setEdgeLabelNum(Integer.parseInt(args[3]));
        state.setVertexLabelNum(Integer.parseInt(args[4]));
        state.setQueryNum(Integer.parseInt(args[5]));
        state.setRepeatTimes(1);

        GremlinGraphProvider provider = new GremlinGraphProvider(state);

        try {
            // Comparison experiments
            // provider.generateAndTestDatabase(state);

            // Metamorphic testing
            provider.generateAndTestDatabaseWithRules(state);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}
