package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;
import org.gdbtesting.common.ast.Operator;

public class OrderingTermOperation extends Traversal {

    public enum OrderOperator implements Operator {
        ASC("asc"), DESC("desc");

        String textRepresentation;

        OrderOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    public static OrderOperator getRandom() {
        return Randomly.fromOptions(OrderOperator.values());
    }

    public static class ASC extends OrderingTermOperation {

        public String toString() {
            return "order().by(asc)";
        }
    }

    public static class DESC extends OrderingTermOperation {

        public String toString() {
            return "order().by(desc)";
        }
    }

    public static ASC createASC() {
        return new ASC();
    }

    public static DESC createDESC() {
        return new DESC();
    }

}
