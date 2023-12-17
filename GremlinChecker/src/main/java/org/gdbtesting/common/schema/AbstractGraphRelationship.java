package org.gdbtesting.common.schema;

import org.gdbtesting.Randomly;

import java.util.Collections;
import java.util.List;

public class AbstractGraphRelationship<EP extends AbstractGraphEdgeProperty, VL extends AbstractGraphVertexLabel, EI extends AbstractGraphEdgeIndex> implements Comparable<AbstractGraphRelationship> {

    // <outLabel, "", inLabel, "", edgeLabel, "", edgeProperties, "">
    protected int id;
    protected final String edgeLabel;
    private final VL outLabel;
    private final VL inLabel;
    private final List<EP> edgeProperties;
    private final List<EI> indexes;

    public AbstractGraphRelationship(String name, VL outLabel, VL inLabel, List<EP> properties, List<EI> indexes) {
        this.edgeLabel = name;
        this.edgeProperties = Collections.unmodifiableList(properties);
        this.indexes = indexes;
        this.outLabel = outLabel;
        this.inLabel = inLabel;
        // TODO: generate global id
        // this.id = GlobalState.generateId();
    }

    public int getId() {
        return id;
    }

    public VL getOutLabel() {
        return outLabel;
    }

    public VL getInLabel() {
        return inLabel;
    }

    public List<EI> getIndexes() {
        return indexes;
    }

    public String getLabelName() {
        return edgeLabel;
    }

    public List<EP> getEdgeProperties() {
        return edgeProperties;
    }

    public EP getRandomEdgeProperties() {
        return Randomly.fromList(edgeProperties);
    }

    public boolean hasIndexes() {
        return !indexes.isEmpty();
    }

    public EI getRandomIndex() {
        return Randomly.fromList(indexes);
    }

    public List<EP> getRandomNonEmptyEdgePropertiesSubset() {
        return Randomly.nonEmptySubset(getEdgeProperties());
    }

    public List<EP> getRandomNonEmptyEdgePropertiesSubset(int size) {
        return Randomly.nonEmptySubset(getEdgeProperties(), size);
    }

    @Override
    public int compareTo(AbstractGraphRelationship o) {
        return o.getLabelName().compareTo(getLabelName());
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(getLabelName());
        sb.append("\n");
        sb.append("outVertex: " + getOutLabel() + "\n");
        sb.append("inVertex: " + getInLabel() + "\n");
        for (EP ep : edgeProperties) {
            sb.append("\t" + ep + "\n");
        }
        /*if(hasIndexes()){
            for(EI ei : indexes){
                sb.append("\tindex: " + ei + "\n");
            }
        }*/
        return sb.toString();
    }
}
