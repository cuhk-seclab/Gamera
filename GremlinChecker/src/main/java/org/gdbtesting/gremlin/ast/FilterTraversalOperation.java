package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;

import java.util.List;

public class FilterTraversalOperation extends Traversal {

    public enum Filter {
        or("or"),       // or(Traversal)
        and("and"),     // and(Traversal)
        not("not"),     // not(Traversal)
        where_traversal("where_traversal"),     // where(Traversal)     where for query partitioning
        has_key_value("has_key_value"),         // has(String:key, Object:value)
        has_label_key_value("has_label_key_value"),     // has(String, String, Object)
        has_key_predicate("has_key_predicate"), // has(String:key, Predicate)
        /*has_key_traversal("has_key_traversal"),*/
        has("has"),
        hasNot("hasNot"),
        hasLabel("hasLabel"),
        /*hasId("hasId"),*/
        where_statistic_is("where_statistic_is"),
        where_value_is("where_value_is"),
        where_count_is("where_count_is");

        private String filter;

        Filter(String filter) {
            this.filter = filter;
        }

        public String getFilter() {
            return filter;
        }
    }

    public enum FilterLite {
        has("has"),
        hasNot("hasNot"),
        hasLabel("hasLabel");

        private String filterLite;

        FilterLite(String filterLite) {
            this.filterLite = filterLite;
        }

        public String getFilterLite() {
            return filterLite;
        }
    }

    /*public enum FilterPredicate{
//        is("is"), //is(Object)
//        is_predicate("is_predicate"), // is(Predicate)
//        where_predicate("where_predicate"), //where(Predicate)
        where_value_is("where_value_is"),
        where_count_is("where_count_is"),
        *//*has_label_key_predicate("has_label_key_predicate"),*//*
        //        hasLabel_predicate("hasLabel_predicate"),
        *//*hasId_predicate("hasId_predicate"),
        hasKey("hasKey"),
        hasKey_predicate("hasKey_predicate"),
        hasValue("hasValue"),
        hasValue_predicate("hasValue_predicate")*//*
        ;

        private String filter;

        FilterPredicate(String filter){this.filter = filter;}

        public String getFilterPredicate(){return filter;}
    }*/

    public static Filter getRandomFilter() {
        return Randomly.fromOptions(Filter.values());
    }

    public static FilterLite getRandomFilterLite() {
        return Randomly.fromOptions(FilterLite.values());
    }

    public static class Or extends FilterTraversalOperation {

        private Traversal traversal;

        public Or(Traversal traversal) {
            this.traversal = traversal;
        }

        public Traversal getValue() {
            return traversal;
        }

        @Override
        public String toString() {
            return "or(__." + traversal + ")";
        }

    }

    public static class And extends FilterTraversalOperation {

        private Traversal traversal;

        public And(Traversal traversal) {
            this.traversal = traversal;
        }

        public Traversal getValue() {
            return traversal;
        }

        @Override
        public String toString() {
            return "and(__." + traversal + ")";
        }

    }

    public static class Not extends FilterTraversalOperation {

        private Traversal traversal;

        public Not(Traversal traversal) {
            this.traversal = traversal;
        }

        public Traversal getValue() {
            return traversal;
        }

        @Override
        public String toString() {
            return "not(__." + traversal + ")";
        }

    }

    public static class WhereStatisticIs extends FilterTraversalOperation {
        private IsPredicate isPredicate;
        private PropertyTraversalOperation.Values properties;
        private StatisticTraversalOperation statistic;

        public WhereStatisticIs(IsPredicate isPredicate, PropertyTraversalOperation.Values properties, StatisticTraversalOperation statistic) {
            this.isPredicate = isPredicate;
            this.properties = properties;
            this.statistic = statistic;
        }

        public String toString() {
            return "where(" + properties.toString() + "." + statistic.toString() + "." + isPredicate + ")";
        }
    }

    public static class WhereCountIs extends FilterTraversalOperation {
        private IsPredicate isPredicate;
        private NeighborTraversalOperation neighbor;
        private StatisticTraversalOperation.Count count;

        public WhereCountIs(IsPredicate isPredicate, NeighborTraversalOperation neighbor, StatisticTraversalOperation.Count count) {
            this.isPredicate = isPredicate;
            this.neighbor = neighbor;
            this.count = count;
        }

        public String toString() {
            return "where(__." + neighbor.toString() + "." + count.toString() + "." + isPredicate + ")";
        }
    }

    public static class WhereValuesIs extends FilterTraversalOperation {
        private IsPredicate isPredicate;
        private NeighborTraversalOperation neighbor;
        private Traversal property;

        public WhereValuesIs(IsPredicate isPredicate, NeighborTraversalOperation neighbor, Traversal property) {
            this.isPredicate = isPredicate;
            this.neighbor = neighbor;
            this.property = property;
        }

        public String toString() {
            return "where(__." + neighbor.toString() + "." + property.toString() + "." + isPredicate + ")";
        }
    }

    public static class Is extends FilterTraversalOperation {
        private Object object;

        public Is(Object object) {
            this.object = object;
        }

        public Object getValue() {
            return object;
        }

        public String toString() {
            return "is(" + object + ")";
        }
    }

    public static class IsPredicate extends FilterTraversalOperation {
        private Predicate predicate;

        public IsPredicate(Predicate predicate) {
            this.predicate = predicate;
        }

        public Predicate getValue() {
            return predicate;
        }

        public String toString() {
            return "is(" + predicate + ")";
        }
    }

    public static class WherePredicate extends FilterTraversalOperation {
        private Predicate predicate;

        public WherePredicate(Predicate predicate) {
            this.predicate = predicate;
        }

        public Predicate getValue() {
            return predicate;
        }

        public String toString() {
            return "where(" + predicate + ")";
        }
    }

    public static class WhereTraversal extends FilterTraversalOperation {
        private Traversal traversal;

        public WhereTraversal(Traversal traversal) {
            this.traversal = traversal;
        }

        public Traversal getValue() {
            return traversal;
        }

        public String toString() {
            return "where(__." + traversal + ")";
        }
    }

    //has_key_predicate
    public static class HasKeyPredicate extends FilterTraversalOperation {
        private Predicate predicate;
        private String key;

        public HasKeyPredicate(String key, Predicate predicate) {
            this.predicate = predicate;
            this.key = key;
        }

        public Predicate getPredicate() {
            return predicate;
        }

        public String getKey() {
            return key;
        }

        public String toString() {
            return "has('" + key + "', " + predicate + ")";
        }
    }

    public static class HasNotKeyPredicate extends FilterTraversalOperation {
        private Predicate predicate;
        private String key;

        public HasNotKeyPredicate(String key, Predicate predicate) {
            this.predicate = predicate;
            this.key = key;
        }

        public Predicate getPredicate() {
            return predicate;
        }

        public String getKey() {
            return key;
        }

        public String toString() {
            return "hasNot('" + key + "', " + predicate + ")";
        }
    }

    public static class HasKeyValue extends FilterTraversalOperation {
        private Object value;
        private String key;

        public HasKeyValue(String key, Object value) {
            this.value = value;
            this.key = key;
        }

        public Object getValue() {
            return value;
        }

        public String getKey() {
            return key;
        }

        public String toString() {
            return "has('" + key + "'," + value + ")";
        }
    }

    public static class HasLabelKeyPredicate extends FilterTraversalOperation {

        private String label;
        private String key;
        private Predicate predicate;

        public HasLabelKeyPredicate(String label, String key, Predicate predicate) {
            this.key = key;
            this.label = label;
            this.predicate = predicate;
        }

        public String toString() {
            return "has('" + label + "', '" + key + "'," + predicate + ")";
        }
    }

    public static class HasLabelKeyValue extends FilterTraversalOperation {
        private String label;
        private String key;
        private Object value;

        public HasLabelKeyValue(String label, String key, Object value) {
            this.key = key;
            this.label = label;
            this.value = value;
        }

        public String toString() {
            return "has('" + label + "', '" + key + "', " + value + ")";
        }
    }

    /**
     * The traversal needs to be a property traversal.
     *  TODO： not find an example using this mode
     */
    public static class HasKeyTraversal extends FilterTraversalOperation {

        private String key;
        private Traversal traversal;

        public HasKeyTraversal(String key, Traversal traversal) {
            this.key = key;
            this.traversal = traversal;
        }

        public String toString() {
            return "has('" + key + "', " + traversal + ")";
        }

    }

    public static class HasKey extends FilterTraversalOperation {
        private String key;

        public HasKey(String key) {
            this.key = key;
        }

        public String toString() {
            return "has('" + key + "')";
        }
    }

    public static class HasNotKey extends FilterTraversalOperation {
        private String key;

        public HasNotKey(String key) {
            this.key = key;
        }

        public String toString() {
            return "hasNot('" + key + "')";
        }
    }

    public static class HasLabel extends FilterTraversalOperation {
        private List<String> label;

        public HasLabel(List<String> label) {
            this.label = label;
        }

        public String toString() {
            String labelString = "";
            for (String s : label) {
                labelString += "'" + s + "',";
            }
            return "hasLabel(" + labelString.substring(0, labelString.length() - 1) + ")";
        }

    }

    public static class HasId extends FilterTraversalOperation {
        private List<Object> id;

        public HasId(List<Object> id) {
            this.id = id;
        }

        public String toString() {
            String IdString = "";
            for (Object o : id) {
                IdString += o + ",";
            }
            return "hasId(" + IdString.substring(0, IdString.length() - 1) + ")";
        }
    }


    public static Or createOr(Traversal traversal) {
        return new Or(traversal);
    }

    public static And createAnd(Traversal traversal) {
        return new And(traversal);
    }

    public static Not createNot(Traversal traversal) {
        return new Not(traversal);
    }

    public static WhereTraversal createWhereTraversal(Traversal traversal) {
        return new WhereTraversal(traversal);
    }

    public static WherePredicate createWherePredicate(Predicate predicate) {
        return new WherePredicate(predicate);
    }

    public static Is createIs(Object object) {
        return new Is(object);
    }

    public static IsPredicate createIsPredicate(Predicate predicate) {
        return new IsPredicate(predicate);
    }

    public static HasKeyValue createHasKeyValue(String key, Object value) {
        return new HasKeyValue(key, value);
    }

    public static HasKeyPredicate createHasKeyPredicate(String key, Predicate predicate) {
        return new HasKeyPredicate(key, predicate);
    }

    public static HasNotKeyPredicate createHasNotKeyPredicate(String key, Predicate predicate) {
        return new HasNotKeyPredicate(key, predicate);
    }

    public static HasLabelKeyPredicate createHasLabelKeyPredicate(String label, String key, Predicate predicate) {
        return new HasLabelKeyPredicate(label, key, predicate);
    }

    public static HasLabelKeyValue createHasLabelKeyValue(String label, String key, Object value) {
        return new HasLabelKeyValue(label, key, value);
    }

    public static HasKeyTraversal createHasKeyTraversal(String key, Traversal traversal) {
        return new HasKeyTraversal(key, traversal);
    }

    public static HasKey createHasKey(String key) {
        return new HasKey(key);
    }

    public static HasNotKey createHasNotKey(String key) {
        return new HasNotKey(key);
    }

    public static HasLabel createHasLabel(List<String> label) {
        return new HasLabel(label);
    }

    public static HasId createHasId(List<Object> id) {
        return new HasId(id);
    }

    public static WhereValuesIs createWhereValueIs(IsPredicate isPredicate, NeighborTraversalOperation neighbor, Traversal property) {
        return new WhereValuesIs(isPredicate, neighbor, property);
    }

    public static WhereStatisticIs createWhereStatisticIs(IsPredicate isPredicate, PropertyTraversalOperation.Values properties, StatisticTraversalOperation statistic) {
        return new WhereStatisticIs(isPredicate, properties, statistic);
    }

    public static WhereCountIs createWhereCountIs(IsPredicate isPredicate, NeighborTraversalOperation neighbor, StatisticTraversalOperation.Count count) {
        return new WhereCountIs(isPredicate, neighbor, count);
    }
}
