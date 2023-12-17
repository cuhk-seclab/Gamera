package ch.cypherchecker.cypher.ast;

import ch.cypherchecker.util.Randomization;

public record CypherPrefixOperation(CypherExpression expression,
                                    PrefixOperator operator) implements CypherExpression {

    public enum PrefixOperator implements Operator {

        NOT("NOT"),
        UNARY_PLUS("+"),
        UNARY_MINUS("-");

        private final String representation;

        PrefixOperator(String representation) {
            this.representation = representation;
        }

        @Override
        public String getTextRepresentation() {
            return representation;
        }

        public static PrefixOperator getRandom() {
            return Randomization.fromOptions(values());
        }

    }

}
