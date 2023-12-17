package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;
import org.gdbtesting.common.ast.Operator;
import org.gdbtesting.common.ast.UnaryPrefixOperatorNode;

public class UnaryPrefixOperation extends UnaryPrefixOperatorNode<GraphExpression> implements GraphExpression {

    public enum EmunUnaryPrefixOperator implements Operator {
        NOT("not");

        String textRepresentation;

        EmunUnaryPrefixOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static EmunUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    public UnaryPrefixOperation(GraphExpression expr, EmunUnaryPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public String toString() {
        return getOperatorRepresentation() + "(" + getExpr() + ")";
    }
}
