package org.gdbtesting.common.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AbstractGraphVertex<VL extends AbstractGraphVertexLabel, VP extends AbstractGraphVertexProperty> {

    private final List<VL> labels;
    private final List<VP> vertexProperties;

    public AbstractGraphVertex(List<VL> labels) {
        this.labels = labels;
        vertexProperties = new ArrayList<>();
        for (VL l : labels) {
            vertexProperties.addAll(l.getVertexProperties());
        }
    }

    public String labelsNamesAsString() {
        return labels.stream().map(t -> t.getLabelName()).collect(Collectors.joining(", "));
    }

    public List<VL> getLabels() {
        return labels;
    }

    public List<VP> getVertexProperties() {
        return vertexProperties;
    }

    public String propertyNamesAsString(Function<VP, String> function) {
        return getVertexProperties().stream().map(function).collect(Collectors.joining(", "));
    }
}
