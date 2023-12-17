package ch.cypherchecker.cypher.ast;

public record CypherRegularExpression(CypherExpression string,
                                      CypherExpression regex) implements CypherExpression {

}
