package org.gdbtesting.common.ast;


public class UnaryPrefixOperatorNode<T> {

    protected final Operator op;
    private final T expr;

    public UnaryPrefixOperatorNode(T expr, Operator op) {
        this.expr = expr;
        this.op = op;
    }

    public String getOperatorRepresentation() {
        return op.getTextRepresentation();
    }

    public T getExpr() {
        return expr;
    }

}
