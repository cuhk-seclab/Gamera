package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;

public class Predicate extends Traversal {

    public enum P {
        eq("eq"),
        neq("neq"),
        lt("lt"),
        lte("lte"),
        gt("gt"),
        gte("gte"),
        inside("inside"), // inside(V,V)
        outside("outside"), // outside(V,V)
        between("between"), // between(V,V)
        /*within("within"),
        without("without"),*/
        not("not"), // not(Predicate)
        and("and"), // and(Predicate)
        or("or"), // or(Predicate)
        ;

        private String predicate;

        P(String predicate) {
            this.predicate = predicate;
        }

        public String getP() {
            return predicate;
        }

    }

    public enum PP {
        and("and"), // and(Predicate)
        or("or"), // or(Predicate)*/
        ;

        private String predicate;

        PP(String predicate) {
            this.predicate = predicate;
        }

        public String getP() {
            return predicate;
        }
    }

    public static PP getPrandomPPredicate() {
        return Randomly.fromOptions(PP.values());
    }

    public static P getRandomPredicate() {
        return Randomly.fromOptions(P.values());
    }

    public static class Eq<V> extends Predicate {

        private V value;

        public Eq(V value) {
            this.value = value;
        }

        public V getValue() {
            return value;
        }

        public String toString() {
            return "eq(" + value + ")";
        }
    }

    public static class Neq<V> extends Predicate {

        private V value;

        public Neq(V value) {
            this.value = value;
        }

        public V getValue() {
            return value;
        }

        public String toString() {
            return "neq(" + value + ")";
        }
    }

    public static class Lt<V> extends Predicate {

        private V value;

        public Lt(V value) {
            this.value = value;
        }

        public V getValue() {
            return value;
        }

        public String toString() {
            return "lt(" + value + ")";
        }
    }

    public static class Lte<V> extends Predicate {

        private V value;

        public Lte(V value) {
            this.value = value;
        }

        public V getValue() {
            return value;
        }

        public String toString() {
            return "lte(" + value + ")";
        }
    }

    public static class Gt<V> extends Predicate {

        private V value;

        public Gt(V value) {
            this.value = value;
        }

        public V getValue() {
            return value;
        }

        public String toString() {
            return "gt(" + value + ")";
        }
    }

    public static class Gte<V> extends Predicate {

        private V value;

        public Gte(V value) {
            this.value = value;
        }

        public V getValue() {
            return value;
        }

        public String toString() {
            return "gte(" + value + ")";
        }
    }

    public static class Inside<V> extends Predicate {

        private V value;
        private V value2;

        public Inside(V value, V value2) {
            this.value = value;
            this.value2 = value2;
        }

        public V getValue() {
            return value;
        }

        public V getValue2() {
            return value2;
        }

        public String toString() {
            return "inside(" + value + "," + value2 + ")";
        }
    }

    public static class Outside<V> extends Predicate {

        private V value;
        private V value2;

        public Outside(V value, V value2) {
            this.value = value;
            this.value2 = value2;
        }

        public V getValue() {
            return value;
        }

        public V getValue2() {
            return value2;
        }

        public String toString() {
            return "outside(" + value + "," + value2 + ")";
        }
    }

    public static class Between<V> extends Predicate {

        private V value;
        private V value2;

        public Between(V value, V value2) {
            this.value = value;
            this.value2 = value2;
        }

        public V getValue() {
            return value;
        }

        public V getValue2() {
            return value2;
        }

        public String toString() {
            return "between(" + value + "," + value2 + ")";
        }
    }

    public static class Not extends Predicate {

        private Predicate predicate;

        public Not(Predicate predicate) {
            this.predicate = predicate;
        }

        public Predicate getValue() {
            return predicate;
        }

        public String toString() {
            return "not(" + predicate + ")";
        }
    }

    public static class And extends Predicate {

        private Predicate predicate;
        private Predicate preP;

        public And(Predicate preP, Predicate predicate) {
            this.preP = preP;
            this.predicate = predicate;
        }

        public Predicate getValue() {
            return predicate;
        }

        public String toString() {
            return preP.toString() + ".and(" + predicate + ")";
        }
    }

    public static class Or extends Predicate {

        private Predicate predicate;
        private Predicate preP;

        public Or(Predicate preP, Predicate predicate) {
            this.preP = preP;
            this.predicate = predicate;
        }

        public Predicate getValue() {
            return predicate;
        }

        public String toString() {
            return preP.toString() + ".or(" + predicate + ")";
        }
    }

    public static class NegatedPredicate extends Predicate {
        private Predicate preP;

        public NegatedPredicate(Predicate preP) {
            this.preP = preP;
        }

        public String toString() {
            return "not(" + preP.toString() + ")";
        }
    }

    public static class Dedup extends Predicate {
        public Dedup() {
        }

        public String toString() {
            return "dedup()";
        }
    }

    public static Eq createEq(Object value) {
        return new Eq(value);
    }

    public static Neq createNeq(Object value) {
        return new Neq(value);
    }

    public static Lt createLt(Object value) {
        return new Lt(value);
    }

    public static Lte createLte(Object value) {
        return new Lte(value);
    }

    public static Gt createGt(Object value) {
        return new Gt(value);
    }

    public static Gte createGte(Object value) {
        return new Gte(value);
    }

    public static Inside createInside(Object value, Object value2) {
        return new Inside(value, value2);
    }

    public static Outside createOutside(Object value, Object value2) {
        return new Outside(value, value2);
    }

    public static Between createBetween(Object value, Object value2) {
        return new Between(value, value2);
    }

    public static Not createNot(Predicate predicate) {
        return new Not(predicate);
    }

    public static And createAnd(Predicate preP, Predicate postP) {
        return new And(preP, postP);
    }

    public static Or createOr(Predicate preP, Predicate postP) {
        return new Or(preP, postP);
    }

    public static NegatedPredicate createNegatedPredicate(Predicate preP) {
        return new NegatedPredicate(preP);
    }

    public static Dedup createDedup() {
        return new Dedup();
    }
}
