package ch.cypherchecker.memgraph.gen;

import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.cypher.CypherUtil;
import ch.cypherchecker.memgraph.MemgraphQuery;
import ch.cypherchecker.memgraph.schema.MemgraphType;
import ch.cypherchecker.util.IgnoreMeException;
import ch.cypherchecker.util.Randomization;

public class MemgraphDropIndexGenerator {

    private final Schema<MemgraphType> schema;

    private final StringBuilder query = new StringBuilder();

    public MemgraphDropIndexGenerator(Schema<MemgraphType> schema) {
        this.schema = schema;
    }

    public static MemgraphQuery dropIndex(Schema<MemgraphType> schema) {
        return new MemgraphDropIndexGenerator(schema).generateDropIndex();
    }

    private MemgraphQuery generateDropIndex() {
        // Drop a non-existing index
        if (Randomization.smallBiasProbability()) {
            query.append(String.format("DROP INDEX %s IF EXISTS", CypherUtil.generateValidName()));
            return new MemgraphQuery(query.toString());
        }

        if (!schema.hasIndices()) {
            throw new IgnoreMeException();
        }

        query.append(String.format("DROP INDEX %s", schema.getRandomIndex()));

        if (Randomization.getBoolean()) {
            query.append(" IF EXISTS");
        }

        return new MemgraphQuery(query.toString(), true);
    }
}
