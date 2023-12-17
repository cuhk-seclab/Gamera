package org.gdbtesting.gremlin.ast;

import org.gdbtesting.common.ast.SelectNode;

public class SelectOperation extends SelectNode<GraphExpression> implements GraphExpression {

    private boolean isDistinct;

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

}
