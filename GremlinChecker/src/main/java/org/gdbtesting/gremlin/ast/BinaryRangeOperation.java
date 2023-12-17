package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;
import org.gdbtesting.common.ast.BinaryOperatorNode;
import org.gdbtesting.common.ast.Operator;

public class BinaryRangeOperation extends BinaryOperatorNode<GraphExpression> implements GraphExpression {

    public enum BinaryRangeOperator implements Operator {

        INSIDE("inside"),
        OUTSIDE("outside"),
        BETWEEN("between");

        String textRepresentation;

        BinaryRangeOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static BinaryRangeOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    public BinaryRangeOperation(GraphExpression left, GraphExpression right, Operator op) {
        super(left, right, op);
    }

    @Override
    public String toString() {
        return getOperatorRepresentation() + "(" + getLeft() + "," + getRight() + ")";
    }
}
