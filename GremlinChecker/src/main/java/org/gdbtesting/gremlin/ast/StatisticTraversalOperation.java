package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;

public class StatisticTraversalOperation extends Traversal {

    public enum AggregateOperator {

        /*count("count"),*/
        sum("sum"),
        mean("mean"),
        max("max"),
        min("min"),
        ;

        private String text;

        AggregateOperator(String text) {
            this.text = text;
        }

        public String getText() {
            return text;
        }
    }

    public static AggregateOperator getRandomAggregate() {
        return Randomly.fromOptions(AggregateOperator.values());
    }

    public static class Count extends StatisticTraversalOperation {

        public Count() {
        }

        public String toString() {
            return "count()";
        }
    }

    public static class Sum extends StatisticTraversalOperation {
        public Sum() {
        }

        public String toString() {
            return "sum()";
        }
    }

    public static class Mean extends StatisticTraversalOperation {
        public Mean() {
        }

        public String toString() {
            return "mean()";
        }
    }

    public static class Max extends StatisticTraversalOperation {
        public Max() {
        }

        public String toString() {
            return "max()";
        }
    }

    public static class Min extends StatisticTraversalOperation {
        public Min() {
        }

        public String toString() {
            return "min()";
        }
    }

    public static class Size extends StatisticTraversalOperation {
        public Size() {
        }

        public String toString() {
            return "size()";
        }
    }

    public static Count createCount() {
        return new Count();
    }

    public static Sum createSum() {
        return new Sum();
    }

    public static Mean createMean() {
        return new Mean();
    }

    public static Max createMax() {
        return new Max();
    }

    public static Min createMin() {
        return new Min();
    }

    public static Size createSize() {
        return new Size();
    }

}
