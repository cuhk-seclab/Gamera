package ch.cypherchecker.memgraph.gen;

import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.memgraph.MemgraphQuery;
import ch.cypherchecker.memgraph.schema.MemgraphType;
import ch.cypherchecker.util.Randomization;

public class MemgraphShowFunctionsGenerator {

    private final StringBuilder query = new StringBuilder();

    public static MemgraphQuery showFunctions(Schema<MemgraphType> ignored) {
        return new MemgraphShowFunctionsGenerator().generateShowFunctions();
    }

    private enum FunctionFilterType {
        ALL,
        BUILT_IN,
        USER_DEFINED,
        NONE
    }

    private MemgraphQuery generateShowFunctions() {
        query.append("SHOW ");

        switch (Randomization.fromOptions(MemgraphShowFunctionsGenerator.FunctionFilterType.values())) {
            case ALL:
                query.append("ALL ");
                break;
            case BUILT_IN:
                query.append("BUILT IN ");
                break;
            case USER_DEFINED:
                query.append("USER DEFINED ");
                break;
            case NONE:
                break;
            default:
                throw new AssertionError();
        }

        query.append("FUNCTION");

        if (Randomization.getBoolean()) {
            query.append("S");
        }

        return new MemgraphQuery(query.toString());
    }

}
