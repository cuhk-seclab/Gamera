package org.gdbtesting.common.gen;

import org.gdbtesting.Randomly;

import java.util.ArrayList;
import java.util.List;

public abstract class UntypedExpressionGenerator<E, P> implements ExpressionGenerator<E> {

    protected List<P> properties;
    protected boolean allowAggregates;


    public E generateExpression() {
        return generateExpression(0);
    }

    public abstract E generateConstant();

    protected abstract E generateExpression(int depth);

    protected abstract E generateProperties();


    @SuppressWarnings("unchecked") // unsafe
    public <U extends UntypedExpressionGenerator<E, P>> U setProperties(List<P> properties) {
        this.properties = properties;
        return (U) this;
    }

    public E generateLeafNode() {
        if (Randomly.getBoolean() && !properties.isEmpty()) {
            return generateProperties();
        } else {
            return generateConstant();
        }
    }

    public List<E> generateExpressions(int nr) {
        List<E> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression());
        }
        return expressions;
    }

    public List<E> generateExpressions(int depth, int nr) {
        List<E> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(depth));
        }
        return expressions;
    }


    // TODO
    // override this class to generate aggregate functions
    public E generateHavingClause() {
        allowAggregates = true;
        E expr = generateExpression();
        allowAggregates = false;
        return expr;
    }

    @Override
    public E generatePredicate() {
        return generateExpression();
    }

}
