package ch.cypherchecker.memgraph.gen;

import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.memgraph.MemgraphQuery;
import ch.cypherchecker.memgraph.schema.MemgraphType;
import ch.cypherchecker.util.Randomization;

public class MemgraphShowProceduresGenerator {

    private final StringBuilder query = new StringBuilder();

    public static MemgraphQuery showProcedures(Schema<MemgraphType> ignored) {
        return new MemgraphShowProceduresGenerator().generateShowProcedures();
    }

    private MemgraphQuery generateShowProcedures() {
        query.append("SHOW PROCEDURE");

        if (Randomization.getBoolean()) {
            query.append("S");
        }

        return new MemgraphQuery(query.toString());
    }
}
