package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;
import org.gdbtesting.common.ast.BinaryOperatorNode;
import org.gdbtesting.common.ast.Operator;

public class BinaryComparisonOperation extends BinaryOperatorNode<GraphExpression> implements GraphExpression {

    public enum EnumBinaryComparisonOperator implements Operator {

        EQUAL("eq"),
        NOT_EQUAL("neq"),
        LESS("lt"),
        LESS_EQUAL("lte"),
        GREATER("gt"),
        GREATER_EQUAL("gte"),
        /*     WITHIN("within"),
             WITHOUT("without"),*/
        INSIDE("inside"),
        OUTSIDE("outside"),
        BETWEEN("between");

        String textRepresentation;

        EnumBinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static EnumBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    public BinaryComparisonOperation(GraphExpression left, GraphExpression right, EnumBinaryComparisonOperator op) {
        super(left, right, op);
    }

    @Override
    public String toString() {
        return getOperatorRepresentation() + "(" + getRight() + ")";
    }
}
