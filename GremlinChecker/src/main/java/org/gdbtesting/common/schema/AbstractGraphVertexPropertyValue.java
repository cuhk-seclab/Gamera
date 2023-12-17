package org.gdbtesting.common.schema;

import org.gdbtesting.common.ast.Operator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AbstractGraphVertexPropertyValue<VL extends AbstractGraphVertexLabel, VP extends AbstractGraphVertexProperty> {
    private final VL labels;
    private final Map<VP, Operator> values;

    protected AbstractGraphVertexPropertyValue(VL labels, Map<VP, Operator> values) {
        this.labels = labels;
        this.values = values;
    }

    public VL getLabels() {
        return labels;
    }

    public Map<VP, Operator> getValues() {
        return values;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        int i = 0;
        for (Object vp : labels.getVertexProperties()) {
            if (i++ != 0) {
                sb.append(", ");
            }
            sb.append(values.get(vp));
        }
        return sb.toString();
    }

    public String getPropertyValuesAsString() {
        List<VP> propertiesToCheck = labels.getVertexProperties();
        return getPropertyValuesAsString(propertiesToCheck);
    }

    public String getPropertyValuesAsString(List<VP> propertiesToCheck) {
        StringBuilder sb = new StringBuilder();
        Map<VP, Operator> expectedValues = getValues();
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
        List<VP> propertiesList = getValues().keySet().stream().collect(Collectors.toList());
        List<AbstractGraphVertexLabel> labelList = propertiesList.stream().map(c -> c.getLabel()).distinct().sorted()
                .collect(Collectors.toList());
        for (int j = 0; j < labelList.size(); j++) {
            if (j != 0) {
                sb.append("\n");
            }
            AbstractGraphVertexLabel t = labelList.get(j);
            sb.append("-- " + t.getLabelName() + "\n");
            List<VP> propertiesForLabel = propertiesList.stream().filter(c -> c.getLabel().equals(t))
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
