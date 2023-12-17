package ch.cypherchecker.cypher.ast;

import ch.cypherchecker.util.Randomization;

public record CypherPostfixOperation(CypherExpression expression,
                                     PostfixOperator operator) implements CypherExpression {

    public enum PostfixOperator implements Operator {

        IS_NULL("IS NULL"),
        IS_NOT_NULL("IS NOT NULL");

        private final String representation;

        PostfixOperator(String representation) {
            this.representation = representation;
        }

        @Override
        public String getTextRepresentation() {
            return representation;
        }

        public static PostfixOperator getRandom() {
            return Randomization.fromOptions(values());
        }

    }

}
