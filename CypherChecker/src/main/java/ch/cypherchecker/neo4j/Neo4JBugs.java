package ch.cypherchecker.neo4j;

public class Neo4JBugs {

    public static final boolean bug1 = true;
    public static final boolean bug2 = true;
    public static final boolean bug3 = true;
    public static final boolean bug4 = true;

    static public class PartitionOracleSpecific {

        public static boolean bug5 = false;

        public static void enableAll() {
            bug5 = true;
        }

        public static void disableAll() {
            bug5 = false;
        }

    }

}
