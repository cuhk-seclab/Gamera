package org.gdbtesting.common.schema;

import org.gdbtesting.IgnoreMeException;
import org.gdbtesting.Randomly;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Get and randomly get vertex and edge info.
 */
public class AbstractGraphSchema<VL extends AbstractGraphVertexLabel, EL extends AbstractGraphRelationship> {

    private final List<VL> vertexList;
    private final List<EL> edgeList;
    private final List<String> indexList;
    private final List<String> EdgeindexList;

    public AbstractGraphSchema(List<VL> vertexList, List<EL> edgeList, List<String> indexList, List<String> EdgeindexList) {
        this.vertexList = Collections.unmodifiableList(vertexList);
        this.edgeList = Collections.unmodifiableList(edgeList);
        this.indexList = indexList;
        this.EdgeindexList = EdgeindexList;
    }

    public List<VL> getVertexList() {
        return vertexList;
    }

    public List<EL> getEdgeList() {
        return edgeList;
    }

    public List<String> getIndexList() {
        return indexList;
    }

    public List<String> getEdgeIndexList() {
        return EdgeindexList;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("vertex: \n");
        for (VL vl : getVertexList()) {
            sb.append(vl.toString());
            sb.append("\n");
        }
        sb.append("index: \n");
        for (String str : getIndexList()) {
            sb.append(str);
            sb.append("\n");
        }
        sb.append("\n");
        sb.append("edge: \n");
        for (EL el : getEdgeList()) {
            sb.append(el.toString());
            sb.append("\n");
        }
        sb.append("Edgeindex: \n");
        for (String str : getEdgeIndexList()) {
            sb.append(str);
            sb.append("\n");
        }
        return sb.toString();
    }

    public VL getRandomVertexLabel() {
        return Randomly.fromList(getVertexList());
    }

    public EL getRandomEdgeLabel() {
        return Randomly.fromList(getEdgeList());
    }

    public VL getRandomVertexLabelOrBailout() {
        if (vertexList.isEmpty()) {
            throw new IgnoreMeException();
        } else {
            return Randomly.fromList(getVertexList());
        }
    }

    public VL getRandomVertexLabel(Predicate<VL> predicate) {
        return Randomly.fromList(getVertexList().stream().filter(predicate).collect(Collectors.toList()));
    }

    public EL getRandomEdgeLabel(Predicate<EL> predicate) {
        return Randomly.fromList(getEdgeList().stream().filter(predicate).collect(Collectors.toList()));
    }

    // TODO: add getRandomEdge

    public VL getRandomVertexLabelOrBailout(Function<VL, Boolean> f) {
        List<VL> relevantLabels = vertexList.stream().filter(t -> f.apply(t)).collect(Collectors.toList());
        if (relevantLabels.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(relevantLabels);
    }

    public List<VL> getVertexLabelsRandomSubsetNotEmpty() {
        return Randomly.nonEmptySubset(vertexList);
    }

    public List<EL> getEdgeLabelsRandomSubsetNotEmpty() {
        return Randomly.nonEmptySubset(edgeList);
    }

    public VL getVertexLabel(String name) {
        return vertexList.stream().filter(t -> t.getLabelName().equals(name)).findAny().orElse(null);
    }

    public EL getEdgeLabel(String name) {
        return edgeList.stream().filter(t -> t.getLabelName().equals(name)).findAny().orElse(null);
    }


    public String getFreeVertexLabelName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String vertexLabel = String.format("t%d", i++);
            if (vertexList.stream().noneMatch(t -> t.getLabelName().equalsIgnoreCase(vertexLabel))) {
                return vertexLabel;
            }
        } while (true);

    }

    public String getFreeEdgeLabelName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String edgeLabel = String.format("t%d", i++);
            if (edgeList.stream().noneMatch(t -> t.getLabelName().equalsIgnoreCase(edgeLabel))) {
                return edgeLabel;
            }
        } while (true);

    }

}
