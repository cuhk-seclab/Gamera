package org.gdbtesting.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class GDBCommon {

    private static final Pattern SQLANCER_INDEX_PATTERN = Pattern.compile("^i\\d+");

    private GDBCommon() {
    }

    public static String createVertexLabelName(int nr) {
        return String.format("vl%d", nr);
    }

    public static String createEdgeLabelName(int nr) {
        return String.format("el%d", nr);
    }

    public static String createVertexPropertyName(int nr) {
        return String.format("vp%d", nr);
    }

    public static String createEdgePropertyName(int nr) {
        return String.format("ep%d", nr);
    }

    public static String createVertexIndexName(int nr) {
        return String.format("vi%d", nr);
    }

    public static String createEdgeIndexName(int nr) {
        return String.format("ei%d", nr);
    }

    public static boolean matchesIndexName(String indexName) {
        Matcher matcher = SQLANCER_INDEX_PATTERN.matcher(indexName);
        return matcher.matches();
    }

}
