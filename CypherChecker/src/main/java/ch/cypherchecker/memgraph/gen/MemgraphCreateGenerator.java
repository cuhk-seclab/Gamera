package ch.cypherchecker.memgraph.gen;

import ch.cypherchecker.common.ExpectedErrors;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.common.schema.Schema;
import ch.cypherchecker.cypher.gen.CypherCreateGenerator;
import ch.cypherchecker.cypher.gen.CypherPropertyGenerator;
import ch.cypherchecker.memgraph.MemgraphQuery;
import ch.cypherchecker.memgraph.MemgraphUtil;
import ch.cypherchecker.memgraph.schema.MemgraphType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MemgraphCreateGenerator extends CypherCreateGenerator<MemgraphType> {

    public MemgraphCreateGenerator(Schema<MemgraphType> schema) {
        super(schema);
    }

    @Override
    protected CypherPropertyGenerator<MemgraphType> getPropertyGenerator(Entity<MemgraphType> entity, Map<String, Entity<MemgraphType>> variables) {
        return new MemgraphPropertyGenerator(entity, variables);
    }

    public static MemgraphQuery createEntities(Schema<MemgraphType> schema) {
        MemgraphCreateGenerator generator = new MemgraphCreateGenerator(schema);

        ExpectedErrors errors = new ExpectedErrors();
        MemgraphUtil.addRegexErrors(errors);
        MemgraphUtil.addArithmeticErrors(errors);
        MemgraphUtil.addFunctionErrors(errors);

        // Create single query
        generator.generateCreate();
        return new MemgraphQuery(generator.query.toString(), errors);
    }

    public static MemgraphQuery createSimpleEntities(Schema<MemgraphType> schema) {
        MemgraphCreateGenerator generator = new MemgraphCreateGenerator(schema);

        ExpectedErrors errors = new ExpectedErrors();
        MemgraphUtil.addRegexErrors(errors);
        MemgraphUtil.addArithmeticErrors(errors);
        MemgraphUtil.addFunctionErrors(errors);

        // Create single query
        generator.generateCreateSimple();
        return new MemgraphQuery(generator.query.toString(), errors);
    }

    public static List<MemgraphQuery> createEntitiesList(Schema<MemgraphType> schema) {
        MemgraphCreateGenerator generator = new MemgraphCreateGenerator(schema);

        ExpectedErrors errors = new ExpectedErrors();
        MemgraphUtil.addRegexErrors(errors);
        MemgraphUtil.addArithmeticErrors(errors);
        MemgraphUtil.addFunctionErrors(errors);

        // Create multiple queries
        List<String> queryList = new ArrayList<>();
        List<MemgraphQuery> memgraphQueryList = new ArrayList<>();
        queryList = generator.generateCustomizedCreate();
        for (String query : queryList) {
            memgraphQueryList.add(new MemgraphQuery(query, errors));
        }
        return memgraphQueryList;
    }
}
