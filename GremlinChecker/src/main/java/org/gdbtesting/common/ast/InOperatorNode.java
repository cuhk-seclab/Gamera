package org.gdbtesting.common.ast;

import java.util.List;

public class InOperatorNode<T> implements Node<T> {

    private final Node<T> left;
    private final List<Node<T>> right;
    private final boolean isNegated;

    public InOperatorNode(Node<T> left, List<Node<T>> right, boolean isNegated) {
        this.left = left;
        this.right = right;
        this.isNegated = isNegated;
    }

    public Node<T> getLeft() {
        return left;
    }

    public List<Node<T>> getRight() {
        return right;
    }

    public boolean isNegated() {
        return isNegated;
    }

}
