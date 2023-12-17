package ch.cypherchecker.neo4j.gen;

import ch.cypherchecker.cypher.gen.CypherPropertyGenerator;
import ch.cypherchecker.cypher.ast.CypherExpression;
import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.neo4j.ast.Neo4JExpressionGenerator;
import ch.cypherchecker.neo4j.schema.Neo4JType;

import java.util.Map;

public class Neo4JPropertyGenerator extends CypherPropertyGenerator<Neo4JType>  {

    protected Neo4JPropertyGenerator(Entity<Neo4JType> entity) {
        super(entity);
    }

    protected Neo4JPropertyGenerator(Entity<Neo4JType> entity, Map<String, Entity<Neo4JType>> variables) {
        super(entity, variables);
    }

    @Override
    protected CypherExpression generateConstant(Neo4JType type) {
        return Neo4JExpressionGenerator.generateConstant(type);
    }

    @Override
    protected CypherExpression generateExpression(Map<String, Entity<Neo4JType>> variables, Neo4JType type) {
        return Neo4JExpressionGenerator.generateExpression(variables, type);
    }

}
