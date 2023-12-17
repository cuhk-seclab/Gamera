package org.gdbtesting.gremlin.ast;


import org.gdbtesting.Randomly;
import org.gdbtesting.common.ast.BinaryOperatorNode;
import org.gdbtesting.common.ast.Operator;

public class BinaryLogicalOperation extends BinaryOperatorNode<GraphExpression> implements GraphExpression {

    public enum EnumBinaryLogicalOperator implements Operator {
        AND("and"),
        OR("or"),
        NOT("not"),
        WHERE("where");

        String textRepresentation;

        EnumBinaryLogicalOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static EnumBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    public BinaryLogicalOperation(GraphExpression left, GraphExpression right, EnumBinaryLogicalOperator op) {
        super(left, right, op);
    }

    @Override
    public String toString() {
        return "__." + getOperatorRepresentation() + "(" + getLeft()
                + ".is(" + getRight() + "))";
    }

}
