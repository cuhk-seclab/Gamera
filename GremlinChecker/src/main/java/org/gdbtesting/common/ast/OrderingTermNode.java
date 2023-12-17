package org.gdbtesting.common.ast;

public class OrderingTermNode<T> implements Node<T> {

    private final Node<T> expr;
    private final Operator ordering;

    public OrderingTermNode(Node<T> expr, Operator ordering) {
        this.expr = expr;
        this.ordering = ordering;
    }

    public Node<T> getExpr() {
        return expr;
    }

    public Operator getOrdering() {
        return ordering;
    }

}
