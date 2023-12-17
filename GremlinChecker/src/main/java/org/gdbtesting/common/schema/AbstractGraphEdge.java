package org.gdbtesting.common.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AbstractGraphEdge<EL extends AbstractGraphRelationship, EP extends AbstractGraphEdgeProperty> {

    private final List<EL> edgeLabels;
    private final List<EP> edgeProperties;

    public AbstractGraphEdge(List<EL> edgeLabels) {
        this.edgeLabels = edgeLabels;
        edgeProperties = new ArrayList<>();
        for (EL l : edgeLabels) {
            edgeProperties.addAll(l.getEdgeProperties());
        }
    }

    public List<EL> getEdgeLabels() {
        return edgeLabels;
    }

    public List<EP> getEdgeProperties() {
        return edgeProperties;
    }

    public String labelsNamesAsString() {
        return edgeLabels.stream().map(t -> t.getLabelName()).collect(Collectors.joining(", "));
    }

    public String propertyNamesAsString(Function<EP, String> function) {
        return getEdgeProperties().stream().map(function).collect(Collectors.joining(", "));
    }
}
