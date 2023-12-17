package org.gdbtesting.common.ast;

public class PostfixTextNode<T> implements Node<T> {

    private final Node<T> expr;
    private final String text;

    public PostfixTextNode(Node<T> expr, String text) {
        this.expr = expr;
        this.text = text;
    }

    public Node<T> getExpr() {
        return expr;
    }

    public String getText() {
        return text;
    }
}
