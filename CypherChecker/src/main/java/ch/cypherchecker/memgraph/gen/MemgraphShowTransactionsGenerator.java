package ch.cypherchecker.memgraph.gen;

import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.memgraph.MemgraphQuery;
import ch.cypherchecker.memgraph.schema.MemgraphType;
import ch.cypherchecker.util.Randomization;

public class MemgraphShowTransactionsGenerator {

    private final StringBuilder query = new StringBuilder();

    public static MemgraphQuery showTransactions(Schema<MemgraphType> ignored) {
        return new MemgraphShowTransactionsGenerator().generateShowTransactions();
    }

    private MemgraphQuery generateShowTransactions() {
        query.append("SHOW TRANSACTION");

        if (Randomization.getBoolean()) {
            query.append("S");
        }

        return new MemgraphQuery(query.toString());
    }
}
