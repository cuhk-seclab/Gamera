package org.gdbtesting.common.schema;


public class AbstractGraphVertexProperty<T, VL extends AbstractGraphVertexLabel> implements Comparable<AbstractGraphVertexProperty<?, ?>> {

    private final String vertexPerpertyName;
    private final T dataType;
    private VL label;

    public AbstractGraphVertexProperty(String name, T dataType, VL label) {
        this.vertexPerpertyName = name;
        this.dataType = dataType;
        this.label = label;
    }

    public String getVertexPropertyName() {
        return vertexPerpertyName;
    }

    public T getDataType() {
        return dataType;
    }

    public VL getLabel() {
        return label;
    }

    @Override
    public int compareTo(AbstractGraphVertexProperty<?, ?> o) {
        if (o.getLabel().equals(this.getLabel())) {
            return getVertexPropertyName().compareTo(o.getVertexPropertyName());
        } else {
            return o.getLabel().compareTo(this.getLabel());
        }
    }

    @Override
    public String toString() {
        if (label == null) {
            return String.format("%s: %s", getVertexPropertyName(), getDataType());
        } else {
            return String.format("%s.%s: %s", label.getLabelName(), getVertexPropertyName(), getDataType());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AbstractGraphVertexProperty)) {
            return false;
        } else {
            @SuppressWarnings("unchecked")
            AbstractGraphVertexProperty<T, VL> gvp = (AbstractGraphVertexProperty<T, VL>) obj;
            if (gvp.getLabel() == null) {
                return getVertexPropertyName().equals(gvp.getVertexPropertyName());
            }
            return getLabel().getLabelName().contentEquals(gvp.getLabel().getLabelName())
                    && getVertexPropertyName().equals(gvp.getVertexPropertyName());
        }
    }

    @Override
    public int hashCode() {
        return getVertexPropertyName().hashCode() + 11 * getDataType().hashCode();
    }
}
