package org.gdbtesting.gremlin;

import org.gdbtesting.Randomly;

public enum ConstantType {
    INTEGER, /*NULL,*/ STRING, DOUBLE, BOOLEAN, FLOAT, LONG;

    public static boolean isNumber(String type) {
        return type.equals(ConstantType.STRING.toString()) || type.equals(ConstantType.BOOLEAN.toString()) ? false : true;
    }

    public static ConstantType getRandom() {
        return Randomly.fromOptions(values());
    }
}
