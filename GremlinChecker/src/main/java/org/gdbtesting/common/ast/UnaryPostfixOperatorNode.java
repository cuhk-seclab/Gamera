package org.gdbtesting.common.ast;


public class UnaryPostfixOperatorNode<T> implements Node<T> {

    protected final Operator op;
    private final Node<T> expr;

    public UnaryPostfixOperatorNode(Node<T> expr, Operator op) {
        this.expr = expr;
        this.op = op;
    }

    public String getOperatorRepresentation() {
        return op.getTextRepresentation();
    }

    public Node<T> getExpr() {
        return expr;
    }

}
