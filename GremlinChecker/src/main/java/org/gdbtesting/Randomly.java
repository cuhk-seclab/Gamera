package org.gdbtesting;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Supplier;

public final class Randomly {

    private static StringGenerationStrategy stringGenerationStrategy = StringGenerationStrategy.SOPHISTICATED;
    private static int maxStringLength = 10;
    private static boolean useCaching = true;
    private static int cacheSize = 100;

    private static final List<Long> cachedLongs = new ArrayList<>();
    private static final List<String> cachedStrings = new ArrayList<>();
    private static final List<Double> cachedDoubles = new ArrayList<>();
    private static final List<byte[]> cachedBytes = new ArrayList<>();
    private static Supplier<String> provider;

    private static final ThreadLocal<Random> THREAD_RANDOM = new ThreadLocal<>();
    private long seed;

    private static void addToCache(long val) {
        if (useCaching && cachedLongs.size() < cacheSize && !cachedLongs.contains(val)) {
            cachedLongs.add(val);
        }
    }

    private static void addToCache(double val) {
        if (useCaching && cachedDoubles.size() < cacheSize && !cachedDoubles.contains(val)) {
            cachedDoubles.add(val);
        }
    }

    private static void addToCache(String val) {
        if (useCaching && cachedStrings.size() < cacheSize && !cachedStrings.contains(val)) {
            cachedStrings.add(val);
        }
    }

    private static Long getFromLongCache() {
        if (!useCaching || cachedLongs.isEmpty()) {
            return null;
        } else {
            return Randomly.fromList(cachedLongs);
        }
    }

    private static Double getFromDoubleCache() {
        if (!useCaching) {
            return null;
        }
        if (Randomly.getBoolean() && !cachedLongs.isEmpty()) {
            return (double) Randomly.fromList(cachedLongs);
        } else if (!cachedDoubles.isEmpty()) {
            return Randomly.fromList(cachedDoubles);
        } else {
            return null;
        }
    }

    private static String getFromStringCache() {
        if (!useCaching) {
            return null;
        }
        if (Randomly.getBoolean() && !cachedLongs.isEmpty()) {
            return String.valueOf(Randomly.fromList(cachedLongs));
        } else if (Randomly.getBoolean() && !cachedDoubles.isEmpty()) {
            return String.valueOf(Randomly.fromList(cachedDoubles));
        } else if (Randomly.getBoolean() && !cachedBytes.isEmpty()
                && stringGenerationStrategy == StringGenerationStrategy.SOPHISTICATED) {
            return new String(Randomly.fromList(cachedBytes));
        } else if (!cachedStrings.isEmpty()) {
            String randomString = Randomly.fromList(cachedStrings);
            if (Randomly.getBoolean()) {
                return randomString;
            } else {
                return stringGenerationStrategy.transformCachedString(randomString);
            }
        } else {
            return null;
        }
    }

    private static boolean cacheProbability() {
        return useCaching && getNextLong(0, 3) == 1;
    }

    // CACHING END

    public static <T> T fromList(List<T> list) {
        return list.get((int) getNextLong(0, list.size()));
    }

    @SafeVarargs
    public static <T> T fromOptions(T... options) {
        return options[getNextInt(0, options.length)];
    }

    @SafeVarargs
    public static <T> List<T> nonEmptySubset(T... options) {
        int nr = 1 + getNextInt(0, options.length);
        return extractNrRandomColumns(Arrays.asList(options), nr);
    }

    public static <T> List<T> nonEmptySubset(List<T> properties) {
        int nr = 1 + getNextInt(0, properties.size());
        return nonEmptySubset(properties, nr);
    }

    // Covert columns to properties
    public static <T> List<T> nonEmptySubset(List<T> properties, int nr) {
        if (nr > properties.size()) {
            throw new AssertionError(properties + " " + nr);
        }
        return extractNrRandomColumns(properties, nr);
    }

    public static <T> List<T> nonEmptySubList(List<T> properties) {
        List<T> arr = new ArrayList<>();
        while (arr.size() == 0) {
            for (int i = 0; i < properties.size(); i++) {
                if (Randomly.getBoolean()) {
                    arr.add(properties.get(i));
                }
            }
        }
        return arr;
    }

    public static <T> T getOneInfo(Set<String> properties) {
        return (T) nonEmptySubset(properties).get(0);
    }

    public static <T> List<T> nonEmptySubSetToList(Set<T> sets) {
        List<T> arr = new ArrayList<>();
        Object[] o = sets.toArray();
        while (arr.size() == 0) {
            for (int i = 0; i < o.length; i++) {
                if (Randomly.getBoolean()) {
                    arr.add((T) o[i]);
                }
            }
        }
        return arr;
    }

    public static <T> List<T> nonEmptySubsetPotentialDuplicates(List<T> properties) {
        List<T> arr = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            arr.add(Randomly.fromList(properties));
        }
        return arr;
    }

    public static <T> List<T> subset(List<T> columns) {
        int nr = getNextInt(0, columns.size() + 1);
        return extractNrRandomColumns(columns, nr);
    }

    public static <T> List<T> subset(int nr, @SuppressWarnings("unchecked") T... values) {
        List<T> list = new ArrayList<>();
        for (T val : values) {
            list.add(val);
        }
        return extractNrRandomColumns(list, nr);
    }

    public static <T> List<T> subset(@SuppressWarnings("unchecked") T... values) {
        List<T> list = new ArrayList<>();
        for (T val : values) {
            list.add(val);
        }
        return subset(list);
    }

    public static <T> List<T> extractNrRandomColumns(List<T> properties, int nr) {
        assert nr >= 0;
        List<T> selectedProperties = new ArrayList<>();
        List<T> remainingProperties = new ArrayList<>(properties);
        for (int i = 0; i < nr; i++) {
            selectedProperties.add(remainingProperties.remove(getNextInt(0, remainingProperties.size())));
        }
        return selectedProperties;
    }

    public static int smallNumber() {
        // no need to cache for small numbers
        return (int) (Math.abs(getThreadRandom().get().nextGaussian()) * 2);
    }

    public static boolean getBoolean() {
        return getThreadRandom().get().nextBoolean();
    }

    private static ThreadLocal<Random> getThreadRandom() {
        if (THREAD_RANDOM.get() == null) {
            // a static method has been called, before Randomly was instantiated
            THREAD_RANDOM.set(new Random());
        }
        return THREAD_RANDOM;
    }

    public static long getInteger(int bound) {
        long l = (int) (Math.random() * bound) + 1;
        return l;
    }

    public static long getInteger() {
        if (smallBiasProbability()) {
            return Randomly.fromOptions(-1L, Long.MAX_VALUE, Long.MIN_VALUE, 1L, 0L);
        } else {
            if (cacheProbability()) {
                Long l = getFromLongCache();
                if (l != null) {
                    return l;
                }
            }
            long nextLong = getThreadRandom().get().nextInt();
            addToCache(nextLong);
            return nextLong;
        }
    }

    public enum StringGenerationStrategy {

        NUMERIC {
            @Override
            public String getString() {
                return getStringOfAlphabet(NUMERIC_ALPHABET);
            }

        },
        ALPHANUMERIC {
            @Override
            public String getString() {
                return getStringOfAlphabet(ALPHANUMERIC_ALPHABET);
            }
        },
        ALPHANUMERIC_SPECIALCHAR {
            @Override
            public String getString() {
                return getStringOfAlphabet(ALPHANUMERIC_SPECIALCHAR_ALPHABET);
            }
        },
        SOPHISTICATED {

            private static final String ALPHABET = ALPHANUMERIC_SPECIALCHAR_ALPHABET;

            @Override
            public String getString() {
                if (smallBiasProbability()) {
                    return org.gdbtesting.Randomly.fromOptions("TRUE", "FALSE", "0.0", "-0.0", "1e500", "-1e500");
                }
                if (cacheProbability()) {
                    String s = org.gdbtesting.Randomly.getFromStringCache();
                    if (s != null) {
                        return s;
                    }
                }

                int n = ALPHABET.length();

                StringBuilder sb = new StringBuilder();

                int chars = getStringLength();
                for (int i = 0; i < chars; i++) {
                    if (Randomly.getBooleanWithRatherLowProbability()) {
                        char val = (char) org.gdbtesting.Randomly.getInteger();
                        if (val != 0) {
                            sb.append(val);
                        }
                    } else {
                        sb.append(ALPHABET.charAt(getNextInt(0, n)));
                    }
                }
                while (Randomly.getBooleanWithSmallProbability()) {
                    String[][] pairs = {{"{", "}"}, {"[", "]"}, {"(", ")"}};
                    int idx = (int) Randomly.getNotCachedInteger(0, pairs.length);
                    int left = (int) Randomly.getNotCachedInteger(0, sb.length() + 1);
                    sb.insert(left, pairs[idx][0]);
                    int right = (int) Randomly.getNotCachedInteger(left + 1, sb.length() + 1);
                    sb.insert(right, pairs[idx][1]);
                }
                if (Randomly.provider != null) {
                    while (Randomly.getBooleanWithSmallProbability()) {
                        if (sb.length() == 0) {
                            sb.append(Randomly.provider.get());
                        } else {
                            sb.insert((int) Randomly.getNotCachedInteger(0, sb.length()), Randomly.provider.get());
                        }
                    }
                }

                String s = sb.toString();

                org.gdbtesting.Randomly.addToCache(s);
                return s;
            }

            public String transformCachedString(Randomly r, String randomString) {
                if (Randomly.getBoolean()) {
                    return randomString.toLowerCase();
                } else if (Randomly.getBoolean()) {
                    return randomString.toUpperCase();
                } else {
                    char[] chars = randomString.toCharArray();
                    if (chars.length != 0) {
                        for (int i = 0; i < Randomly.smallNumber(); i++) {
                            chars[r.getInteger(0, chars.length)] = ALPHABET.charAt(r.getInteger(0, ALPHABET.length()));
                        }
                    }
                    return new String(chars);
                }
            }

        };

        private static final String ALPHANUMERIC_SPECIALCHAR_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#<>/.,~-+'*()[]{} ^*?%_\t\n\r|&\\";
        private static final String ALPHANUMERIC_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        private static final String NUMERIC_ALPHABET = "0123456789";

        private static int getStringLength() {
            int chars;
            if (Randomly.getBoolean()) {
                chars = Randomly.smallNumber();
            } else {
                chars = Randomly.getInteger(0, maxStringLength);
            }
            return 5;   // Configure to a fixed number?
            // return chars;
        }

        private static String getStringOfAlphabet(String alphabet) {
            int chars = getStringLength();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < chars; i++) {
                sb.append(alphabet.charAt(getNextInt(0, alphabet.length())));
            }
            return sb.toString();
        }

        public abstract String getString();

        public String transformCachedString(String s) {
            return s;
        }

    }

    public static String getString() {
        return stringGenerationStrategy.getString();
    }

    public static byte[] getBytes() {
        int size = Randomly.smallNumber();
        byte[] arr = new byte[size];
        getThreadRandom().get().nextBytes(arr);
        return arr;
    }

    public static long getNonZeroInteger() {
        long value;
        if (smallBiasProbability()) {
            return Randomly.fromOptions(-1L, Long.MAX_VALUE, Long.MIN_VALUE, 1L);
        }
        if (cacheProbability()) {
            Long l = getFromLongCache();
            if (l != null && l != 0) {
                return l;
            }
        }
        do {
            value = getInteger();
        } while (value == 0);
        assert value != 0;
        addToCache(value);
        return value;
    }

    public static long getPositiveInteger() {
        if (cacheProbability()) {
            Long value = getFromLongCache();
            if (value != null && value >= 0) {
                return value;
            }
        }
        long value;
        if (smallBiasProbability()) {
            value = Randomly.fromOptions(0L, Long.MAX_VALUE, 1L);
        } else {
            value = getNextLong(0, Long.MAX_VALUE);
        }
        addToCache(value);
        assert value >= 0;
        return value;
    }

    public static double getFiniteDouble() {
        while (true) {
            double val = getDouble();
            if (Double.isFinite(val)) {
                return val;
            }
        }
    }

    public static double getDouble() {
        if (smallBiasProbability()) {
            return Randomly.fromOptions(0.0, -0.0, Double.MAX_VALUE, -Double.MAX_VALUE, Double.POSITIVE_INFINITY,
                    Double.NEGATIVE_INFINITY);
        } else if (cacheProbability()) {
            Double d = getFromDoubleCache();
            if (d != null) {
                return d;
            }
        }
        double value = getThreadRandom().get().nextDouble();
        addToCache(value);
        return value;
    }

    private static boolean smallBiasProbability() {
        return getThreadRandom().get().nextInt(100) == 1;
    }

    public static boolean getBooleanWithRatherLowProbability() {
        return getThreadRandom().get().nextInt(10) == 1;
    }

    public static boolean getBooleanWithSmallProbability() {
        return smallBiasProbability();
    }

    // Change the function to static
    public static int getInteger(int left, int right) {
        if (left == right) {
            return left;
        }
        return (int) getLong(left, right);
    }

    public static long getLong(long left, long right) {
        if (left == right) {
            return left;
        }
        return getNextLong(left, right);
    }

    public static long getLong() {
        return new Random().nextLong();
    }

    public static float getFloat() {
        return new Random().nextFloat();
    }

    public static BigDecimal getRandomBigDecimal() {
        return new BigDecimal(getThreadRandom().get().nextDouble());
    }

    public static long getPositiveIntegerNotNull() {
        while (true) {
            long val = getPositiveInteger();
            if (val != 0) {
                return val;
            }
        }
    }

    public static long getNonCachedInteger() {
        return getThreadRandom().get().nextLong();
    }

    public static long getPositiveOrZeroNonCachedInteger() {
        return getNextLong(0, Long.MAX_VALUE);
    }

    public static long getNotCachedInteger(int lower, int upper) {
        return getNextLong(lower, upper);
    }

    public Randomly(Supplier<String> provider) {
        this.provider = provider;
    }

    public Randomly() {
        THREAD_RANDOM.set(new Random());
    }

    public Randomly(long seed) {
        this.seed = seed;
        THREAD_RANDOM.set(new Random(seed));
    }

    public static double getUncachedDouble() {
        return getThreadRandom().get().nextDouble();
    }

    public String getChar() {
        while (true) {
            String s = getString();
            if (!s.isEmpty()) {
                return s.substring(0, 1);
            }
        }
    }

    public String getAlphabeticChar() {
        while (true) {
            String s = getChar();
            if (Character.isAlphabetic(s.charAt(0))) {
                return s;
            }
        }
    }

    // see https://stackoverflow.com/a/2546158
    // uniformity does not seem to be important for us
    // org.gdbtesting previously used ThreadLocalRandom.current().nextLong(lower, upper)
    private static long getNextLong(long lower, long upper) {
        if (lower > upper) {
            throw new IllegalArgumentException(lower + " " + upper);
        }
        if (lower == upper) {
            return lower;
        }
        return (long) (getThreadRandom().get().longs(lower, upper).findFirst().getAsLong());
    }

    private static int getNextInt(int lower, int upper) {
        return (int) getNextLong(lower, upper);
    }

    public long getSeed() {
        return seed;
    }

    public static void main(String[] args) {
        Randomly randomly = new Randomly();
        System.out.println(randomly.getInteger());
        System.out.println(randomly.getString());
    }

}
