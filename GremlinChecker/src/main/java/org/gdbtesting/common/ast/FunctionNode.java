package org.gdbtesting.common.ast;

import java.util.List;

public class FunctionNode<T, F> implements Node<T> {

    protected List<Node<T>> args;
    protected F func;

    public FunctionNode(List<Node<T>> args, F func) {
        this.args = args;
        this.func = func;
    }

    public List<Node<T>> getArgs() {
        return args;
    }

    public F getFunc() {
        return func;
    }

}
