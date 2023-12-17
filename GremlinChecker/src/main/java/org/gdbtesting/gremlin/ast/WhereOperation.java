package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;
import org.gdbtesting.common.ast.Operator;

public class WhereOperation implements GraphExpression {

    public enum FilterOperator implements Operator {
        where_is("where_is"),       // where for query partitioning
        where("where");
        // TODO: and, or, not

        String textRepresentation;

        FilterOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static FilterOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return where_is.toString();
        }
    }

    private final Operator op;
    private final GraphExpression left;
    private final GraphExpression right;

    /**
     * where(A.is(B))
     * where(P)
     * A: get the property or statistic to the Traversal vertices; B: construct a comparable value
     * Example:
     * A: __.in('created').values('age').mean() B: inside(30d, 35d))
     * A: values("age") B: gt(30)
     * A: __.out('created').count() B: gte(2))
     * A: __.not(out('created'))
     *
     * @param left
     * @param right
     * @param op
     */
    public WhereOperation(GraphExpression left, GraphExpression right, FilterOperator op) {
        this.op = op;
        this.left = left;
        this.right = right;
    }

    @Override
    public String toString() {
        if (op.getTextRepresentation().equals("where_is")) {
            return op + "(" + getLeft() + ".is(" + getRight() + "))";
        } else {
            return op + "(__." + getLeft() + ")";
        }
    }

    public GraphExpression getLeft() {
        return left;
    }

    public GraphExpression getRight() {
        return right;
    }

}
