package org.gdbtesting.gremlin.ast;


public abstract class GraphConstant extends Traversal {

    public GraphConstant() {
    }

    abstract public Object getType();

    public static class GraphIntConstant extends GraphConstant {

        private long value;

        public GraphIntConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public long getValue() {
            return value;
        }

        public Object getType() {
            return Integer.class;
        }
    }

    public static class GraphDoubleConstant extends GraphConstant {

        private double value;

        public GraphDoubleConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public Object getType() {
            return Double.class;
        }
    }

    public static class GraphStringConstant extends GraphConstant {

        private String value;

        public GraphStringConstant(String value) {
            this.value = value;
        }

        public String getString() {
            return value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value + "'";
        }

        public Object getType() {
            return String.class;
        }

    }

    public static class GraphNullConstant extends GraphConstant {

        @Override
        public Object getType() {
            return null;
        }

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public String toString() {
            return "NULL";
        }
    }

    public static class GraphBooleanConstant extends GraphConstant {

        private boolean value;

        public GraphBooleanConstant(boolean value) {
            this.value = value;
        }

        public boolean getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public Object getType() {
            return Boolean.class;
        }
    }

    public static class GraphFloatConstant extends GraphConstant {

        private float value;

        public GraphFloatConstant(float value) {
            this.value = value;
        }

        public float getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public Object getType() {
            return Float.class;
        }
    }

    public static class GraphLongConstant extends GraphConstant {

        private long value;

        public GraphLongConstant(long value) {
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public Object getType() {
            return Long.class;
        }
    }

    public static GraphIntConstant createIntConstant(long value) {
        return new GraphIntConstant(value);
    }

    public static GraphDoubleConstant createDoubleConstant(double value) {
        return new GraphDoubleConstant(value);
    }

    public static GraphStringConstant createStringConstant(String value) {
        return new GraphStringConstant(value);
    }

    public static GraphNullConstant createNullConstant() {
        return new GraphNullConstant();
    }

    public static GraphBooleanConstant createBooleanConstant(boolean value) {
        return new GraphBooleanConstant(value);
    }

    public static GraphFloatConstant createFloatConstant(float value) {
        return new GraphFloatConstant(value);
    }

    public static GraphLongConstant createLongConstant(long value) {
        return new GraphLongConstant(value);
    }

    public boolean isNull() {
        return false;
    }

}
