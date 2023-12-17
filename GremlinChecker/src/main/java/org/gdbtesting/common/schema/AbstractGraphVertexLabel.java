package org.gdbtesting.common.schema;

import org.gdbtesting.Randomly;

import java.util.Collections;
import java.util.List;


public class AbstractGraphVertexLabel<VP extends AbstractGraphVertexProperty, VI extends AbstractGraphVertexIndex> implements Comparable<AbstractGraphVertexLabel> {

    protected static final int NO_ROW_COUNT_AVAILABLE = -1;
    protected int id;
    protected final String labelName;
    private final List<VP> vertexProperties;
    private VI indexes;
    protected long rowCount = NO_ROW_COUNT_AVAILABLE;

    public AbstractGraphVertexLabel(String name, List<VP> properties, VI indexes) {
        this.labelName = name;
        this.vertexProperties = Collections.unmodifiableList(properties);
        this.indexes = indexes;
        // TODO: generate global id
        // this.id = GlobalState.generateId();
    }

    public void setIndexes(VI indexes) {
        this.indexes = indexes;
    }

    public int getId() {
        return id;
    }

    public String getLabelName() {
        return labelName;
    }


    public List<VP> getVertexProperties() {
        return vertexProperties;
    }

    public VI getIndexes() {
        return indexes;
    }

    public VP getRandomVertexProperties() {
        return Randomly.fromList(vertexProperties);
    }

    public boolean hasIndexes() {
        return !(indexes == null);
    }

    public List<VP> getRandomNonEmptyVertexPropertiesSubset() {
        return Randomly.nonEmptySubset(getVertexProperties());
    }

    public List<VP> getRandomNonEmptyVertexPropertiesSubset(int size) {
        return Randomly.nonEmptySubset(getVertexProperties(), size);
    }

    @Override
    public int compareTo(AbstractGraphVertexLabel o) {
        return o.getLabelName().compareTo(getLabelName());
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(getLabelName());
        sb.append("\n");
        for (VP vp : vertexProperties) {
            sb.append("\t" + vp + "\n");
        }
       /* if(hasIndexes()){
            for (VI vp : indexes) {
                sb.append("\tindex: " + vp + "\n");
            }
        }*/
        return sb.toString();
    }


}
