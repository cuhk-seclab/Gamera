package ch.cypherchecker.neo4j.gen;

import ch.cypherchecker.neo4j.Neo4JQuery;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.neo4j.schema.Neo4JType;
import ch.cypherchecker.util.Randomization;

public class Neo4JShowTransactionsGenerator {

    private final StringBuilder query = new StringBuilder();

    public static Neo4JQuery showTransactions(Schema<Neo4JType> ignored) {
        return new Neo4JShowTransactionsGenerator().generateShowTransactions();
    }

    private Neo4JQuery generateShowTransactions() {
        query.append("SHOW TRANSACTION");

        if (Randomization.getBoolean()) {
            query.append("S");
        }

        return new Neo4JQuery(query.toString());
    }

}
