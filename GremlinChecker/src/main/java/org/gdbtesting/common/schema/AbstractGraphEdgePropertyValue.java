package org.gdbtesting.common.schema;

import org.gdbtesting.common.ast.Operator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AbstractGraphEdgePropertyValue<EL extends AbstractGraphRelationship, EP extends AbstractGraphEdgeProperty> {
    private final EL labels;
    private final Map<EP, Operator> values;

    protected AbstractGraphEdgePropertyValue(EL labels, Map<EP, Operator> values) {
        this.labels = labels;
        this.values = values;
    }

    public EL getLabels() {
        return labels;
    }

    public Map<EP, Operator> getValues() {
        return values;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        int i = 0;
        for (Object EP : labels.getEdgeProperties()) {
            if (i++ != 0) {
                sb.append(", ");
            }
            sb.append(values.get(EP));
        }
        return sb.toString();
    }

    public String getPropertyValuesAsString() {
        List<EP> propertiesToCheck = labels.getEdgeProperties();
        return getPropertyValuesAsString(propertiesToCheck);
    }

    public String getPropertyValuesAsString(List<EP> propertiesToCheck) {
        StringBuilder sb = new StringBuilder();
        Map<EP, Operator> expectedValues = getValues();
        for (int i = 0; i < propertiesToCheck.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            Operator expectedPropertyValue = expectedValues.get(propertiesToCheck.get(i));
            sb.append(expectedPropertyValue);
        }
        return sb.toString();
    }

    public String asStringGroupedByLabels() {
        StringBuilder sb = new StringBuilder();
        List<EP> propertiesList = getValues().keySet().stream().collect(Collectors.toList());
        List<AbstractGraphRelationship> labelList = propertiesList.stream().map(c -> c.getEdgeLabel()).distinct().sorted()
                .collect(Collectors.toList());
        for (int j = 0; j < labelList.size(); j++) {
            if (j != 0) {
                sb.append("\n");
            }
            AbstractGraphRelationship t = labelList.get(j);
            sb.append("-- " + t.getLabelName() + "\n");
            List<EP> propertiesForLabel = propertiesList.stream().filter(c -> c.getEdgeLabel().equals(t))
                    .collect(Collectors.toList());
            for (int i = 0; i < propertiesForLabel.size(); i++) {
                if (i != 0) {
                    sb.append("\n");
                }
                sb.append("--\t");
                sb.append(propertiesForLabel.get(i));
                sb.append("=");
                sb.append(getValues().get(propertiesForLabel.get(i)));
            }
        }
        return sb.toString();
    }
}
