package org.gdbtesting.common.ast;


public class BinaryOperatorNode<T> {

    protected final Operator op;
    protected final T left;
    protected final T right;

    public BinaryOperatorNode(T left, T right, Operator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public String getOperatorRepresentation() {
        return op.getTextRepresentation();
    }

    public T getLeft() {
        return left;
    }

    public T getRight() {
        return right;
    }

}
