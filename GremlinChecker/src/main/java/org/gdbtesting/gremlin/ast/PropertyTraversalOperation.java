package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;

import java.util.List;

public class PropertyTraversalOperation extends Traversal implements GraphExpression {

    public enum PropertyTraversal {
        //        key("key"),
//        value("value"),
//        properties("properties"),
        values("values");

        private String propertyTraversal;

        PropertyTraversal(String propertyTraversal) {
            this.propertyTraversal = propertyTraversal;
        }
    }

    public PropertyTraversal getRandomPropertyTraversal() {
        return Randomly.fromOptions(PropertyTraversal.values());
    }

    public static class Key {
        public Key() {
        }

        @Override
        public String toString() {
            return "key()";
        }

        public String getStartType() {
            return "Property";
        }

        public String getEndType() {
            return "String";
        }

    }

    public static class Value {
        public Value() {
        }

        @Override
        public String toString() {
            return "value()";
        }

        public String getStartType() {
            return "Property";
        }

        public String getEndType() {
            return "Object";
        }

    }

    public static class Properties {

        private List<String> propertyKeys;

        public Properties(List<String> propertyKeys) {
            this.propertyKeys = propertyKeys;
        }

        public String getPropertyKey() {
            String result = "";
            for (String s : propertyKeys) {
                result += "'" + s + "',";
            }
            return result.substring(0, result.length() - 1);
        }

        @Override
        public String toString() {

            return "properties(" + getPropertyKey() + ")";
        }

        public String getEndType() {
            return "Property";
        }

    }

    public static class Values {

        private List<String> propertyKeys;

        public Values(List<String> propertyKeys) {
            this.propertyKeys = propertyKeys;
        }

        public String getPropertyKey() {
            String result = "";
            if (propertyKeys == null) return result;
            for (String s : propertyKeys) {
                result += "'" + s + "',";
            }
            return result.substring(0, result.length() - 1);
        }

        @Override
        public String toString() {
            return "values(" + getPropertyKey() + ")";
        }

        public String getEndType() {
            return "Property";
        }

    }

    public static Key createKey() {
        return new Key();
    }

    public static Value createValue() {
        return new Value();
    }

    public static Properties createProperties(List<String> propertyKeys) {
        return new Properties(propertyKeys);
    }

    public static Values createValues(List<String> propertyKeys) {
        return new Values(propertyKeys);
    }


}
