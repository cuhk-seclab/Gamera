package ch.cypherchecker.neo4j.gen;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.cypher.CypherUtil;
import ch.cypherchecker.cypher.gen.CypherCreateIndexGenerator;
import ch.cypherchecker.common.schema.Index;
import ch.cypherchecker.neo4j.Neo4JQuery;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.neo4j.schema.Neo4JType;
import ch.cypherchecker.util.Randomization;

public class Neo4JCreateIndexGenerator extends CypherCreateIndexGenerator<Neo4JType> {

    public Neo4JCreateIndexGenerator(Schema<Neo4JType> schema) {
        super(schema, Neo4JType.STRING);
    }

    @Override
    protected void generateNodeIndex(Index index) {
        generateIndex("FOR (n:%s) ", index);
    }

    @Override
    protected void generateRelationshipIndex(Index index) {
        generateIndex("FOR ()-[n:%s]-() ", index);
    }

    @Override
    protected void generateTextIndex(Index index) {
        query.append("CREATE TEXT INDEX ");
        query.append(schema.generateRandomIndexName(CypherUtil::generateValidName));
        query.append(" ");

        // TODO: Maybe choose same name deliberately in this case?
        if (Randomization.getBoolean()) {
            query.append("IF NOT EXISTS ");
        }

        query.append(String.format("FOR (n:%s) ON (n.%s)", index.label(), index.propertyNames().toArray()[0]));
    }

    // TODO: Support point indices
    private void generateIndex(String format, Index index) {
        query.append("CREATE ");

        if (Randomization.getBoolean()) {
            query.append("RANGE ");
        }

        query.append("INDEX ");

        // TODO: Maybe add support for unnamed indices
        query.append(schema.generateRandomIndexName(CypherUtil::generateValidName));
        query.append(" ");

        // TODO: Maybe choose same name deliberately in this case?
        if (Randomization.getBoolean()) {
            query.append("IF NOT EXISTS ");
        }

        query.append(String.format(format, index.label()));
        generateOnClause(index.propertyNames());
    }

    public static Neo4JQuery createIndex(Schema<Neo4JType> schema) {
        Neo4JCreateIndexGenerator generator = new Neo4JCreateIndexGenerator(schema);
        generator.generateCreateIndex();

        ExpectedErrors errors = new ExpectedErrors();
        errors.add("There already exists an index");
        errors.add("An equivalent index already exists");

        return new Neo4JQuery(generator.query.toString(), errors, true);
    }

}
