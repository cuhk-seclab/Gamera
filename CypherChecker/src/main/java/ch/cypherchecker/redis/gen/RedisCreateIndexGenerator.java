package ch.cypherchecker.redis.gen;

import ch.cypherchecker.cypher.gen.CypherCreateIndexGenerator;
import ch.cypherchecker.common.schema.Index;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.redis.RedisQuery;
import ch.cypherchecker.redis.schema.RedisType;

public class RedisCreateIndexGenerator extends CypherCreateIndexGenerator<RedisType> {

    private RedisCreateIndexGenerator(Schema<RedisType> schema) {
        super(schema, RedisType.STRING);
    }

    public static RedisQuery createIndex(Schema<RedisType> schema) {
        RedisCreateIndexGenerator generator = new RedisCreateIndexGenerator(schema);
        generator.generateCreateIndex();

        return new RedisQuery(generator.query.toString());
    }

    @Override
    protected void generateNodeIndex(Index index) {
        query.append(String.format("CREATE INDEX FOR (n:%s) ", index.label()));
        generateOnClause(index.propertyNames());
    }

    @Override
    protected void generateRelationshipIndex(Index index) {
        query.append(String.format("CREATE INDEX FOR ()-[n:%s]-() ", index.label()));
        generateOnClause(index.propertyNames());
    }

    @Override
    protected void generateTextIndex(Index index) {
        query.append(String.format("CALL db.idx.fulltext.createNodeIndex('%s', '%s')", index.label(), index.propertyNames().toArray()[0]));
    }
}
