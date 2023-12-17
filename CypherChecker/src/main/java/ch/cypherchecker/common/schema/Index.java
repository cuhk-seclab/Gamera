package ch.cypherchecker.common.schema;

import java.util.Set;

public record Index(String label, Set<String> propertyNames) {

}
