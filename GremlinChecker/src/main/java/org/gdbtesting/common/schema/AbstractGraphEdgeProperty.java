package org.gdbtesting.common.schema;


public class AbstractGraphEdgeProperty<T, EL extends AbstractGraphRelationship> implements Comparable<AbstractGraphEdgeProperty<?, ?>> {

    private final String edgePropertyName;
    private final T dataType;
    private EL edgeLabel;

    public AbstractGraphEdgeProperty(String name, T dataType, EL edgeLabel) {
        this.edgePropertyName = name;
        this.dataType = dataType;
        this.edgeLabel = edgeLabel;
    }

    public String getEdgePropertyName() {
        return edgePropertyName;
    }

    public T getDataType() {
        return dataType;
    }

    public EL getEdgeLabel() {
        return edgeLabel;
    }

    @Override
    public int compareTo(AbstractGraphEdgeProperty o) {
        if (o.getEdgeLabel().equals(this.getEdgeLabel())) {
            return getEdgePropertyName().compareTo(o.getEdgePropertyName());
        } else {
            return o.getEdgeLabel().compareTo(this.getEdgeLabel());
        }
    }

    @Override
    public String toString() {
        if (edgeLabel == null) {
            return String.format("%s: %s", getEdgePropertyName(), getDataType());
        } else {
            return String.format("%s.%s: %s", edgeLabel.getLabelName(), getEdgePropertyName(), getDataType());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AbstractGraphEdgeProperty)) {
            return false;
        } else {
            @SuppressWarnings("unchecked")
            AbstractGraphEdgeProperty<T, EL> gvp = (AbstractGraphEdgeProperty<T, EL>) obj;
            if (gvp.getEdgeLabel() == null) {
                return getEdgePropertyName().equals(gvp.getEdgePropertyName());
            }
            return getEdgeLabel().getLabelName().contentEquals(gvp.getEdgeLabel().getLabelName())
                    && getEdgePropertyName().equals(gvp.getEdgePropertyName());
        }
    }

    @Override
    public int hashCode() {
        return getEdgePropertyName().hashCode() + 11 * getDataType().hashCode();
    }

}
