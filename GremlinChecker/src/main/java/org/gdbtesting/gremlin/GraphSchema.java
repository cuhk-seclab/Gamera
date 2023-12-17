package org.gdbtesting.gremlin;

import org.gdbtesting.common.schema.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Refer to graph schema/metadata, e.g., vertexProperty, vertexIndex, edgeProperty, edgeIndex
 * vertexLabel, edgeLabel, etc.
 * All the get/set functions and some static classes
 */
public class GraphSchema extends AbstractGraphSchema<GraphSchema.GraphVertexLabel, GraphSchema.GraphRelationship> {

    List<GraphVertexProperty> vertexProperties;
    List<GraphVertexIndex> vertexIndices;
    List<GraphEdgeProperty> edgeProperties;
    List<GraphEdgeIndex> edgeIndices;

    Map<String, GraphVertexLabel> vertexLabelMap;
    Map<String, GraphRelationship> edgeLabelMap;

    // record the out Vertex Label
    Map<String, List<GraphVertexLabel>> outVertexLabelMap = new HashMap<>();
    // record the in Vertex Label
    Map<String, List<GraphVertexLabel>> inVertexLabelMap = new HashMap<>();

    Map<ConstantType, List<GraphVertexProperty>> vertexPropertyMap = new HashMap<>();
    Map<ConstantType, List<GraphEdgeProperty>> edgePropertyMap = new HashMap<>();


    public GraphSchema(List<GraphVertexLabel> vertexList, List<GraphRelationship> edgeList, List<String> indexList, List<String> EdgeindexList) {
        super(vertexList, edgeList, indexList, EdgeindexList);
    }

    public GraphSchema(List<GraphVertexLabel> vertexList, List<GraphRelationship> edgeList, List<GraphVertexProperty> vertexProperties,
                       List<GraphVertexIndex> vertexIndices, List<GraphEdgeProperty> edgeProperties, List<GraphEdgeIndex> edgeIndices, List<String> indexList, List<String> EdgeindexList) {
        super(vertexList, edgeList, indexList, EdgeindexList);
        this.vertexProperties = vertexProperties;
        this.vertexIndices = vertexIndices;
        this.edgeProperties = edgeProperties;
        this.edgeIndices = edgeIndices;
        setVertexLabelMap();
        setEdgeLabelMap();
        setVertexPropertyType();
        setEdgePropertyType();
    }

    public Map<String, GraphVertexLabel> getVertexLabelMap() {
        return vertexLabelMap;
    }

    public void setVertexLabelMap() {
        vertexLabelMap = new HashMap<>();
        for (GraphVertexLabel vl : getVertexList()) {
            vertexLabelMap.put(vl.getLabelName(), vl);
        }
    }

    public Map<String, GraphRelationship> getEdgeLabelMap() {
        return edgeLabelMap;
    }

    public void setEdgeLabelMap() {
        edgeLabelMap = new HashMap<>();
        for (GraphRelationship el : getEdgeList()) {
            edgeLabelMap.put(el.getLabelName(), el);
        }
    }

    public Map<ConstantType, List<GraphVertexProperty>> getVertexPropertyMap() {
        return vertexPropertyMap;
    }

    public Map<ConstantType, List<GraphEdgeProperty>> getEdgePropertyMap() {
        return edgePropertyMap;
    }

    public void setVertexPropertyType() {
        for (GraphVertexProperty property : vertexProperties) {
            // constantType: property1, property2...
            ConstantType type = property.getDataType();
            if (vertexPropertyMap.containsKey(type)) {
                vertexPropertyMap.get(type).add(property);
            } else {
                List<GraphVertexProperty> list = new ArrayList<>();
                list.add(property);
                vertexPropertyMap.put(type, list);
            }
        }
    }

    public void setEdgePropertyType() {
        for (GraphEdgeProperty property : edgeProperties) {
            ConstantType type = property.getDataType();
            if (edgePropertyMap.containsKey(type)) {
                edgePropertyMap.get(type).add(property);
            } else {
                List<GraphEdgeProperty> list = new ArrayList<>();
                list.add(property);
                edgePropertyMap.put(type, list);
            }
        }
    }

    public static class GraphVertexLabel extends AbstractGraphVertexLabel<GraphVertexProperty, GraphVertexIndex> {

        public GraphVertexLabel(String name, List properties, GraphVertexIndex indexes) {
            super(name, properties, indexes);
        }

    }

    public static class GraphRelationship extends AbstractGraphRelationship<GraphEdgeProperty, GraphVertexLabel, GraphEdgeIndex> {

        public GraphRelationship(String name, GraphVertexLabel outLabel, GraphVertexLabel inLabel, List properties, List<GraphEdgeIndex> indexes) {
            super(name, outLabel, inLabel, properties, indexes);
        }
    }

    public static class GraphVertexProperty extends AbstractGraphVertexProperty<ConstantType, GraphVertexLabel> {

        public GraphVertexProperty(String name, ConstantType dataType, GraphVertexLabel label) {
            super(name, dataType, label);
        }
    }

    public static class GraphEdgeProperty extends AbstractGraphEdgeProperty<ConstantType, GraphRelationship> {

        public GraphEdgeProperty(String name, ConstantType dataType, GraphRelationship edgeLabel) {
            super(name, dataType, edgeLabel);
        }
    }

    public static class GraphVertexIndex extends AbstractGraphVertexIndex {
        private List<GraphVertexProperty> vpList;

        private GraphVertexLabel vl;

        private String indexName;

        public GraphVertexIndex(GraphVertexLabel vl, List<GraphVertexProperty> vpList) {
            super(vl.getLabelName());
            this.vl = vl;
            this.vpList = vpList;
            StringBuilder sb = new StringBuilder();
            for (GraphVertexProperty vp : vpList) {
                sb.append(vp.getVertexPropertyName()).append("|");
            }
            setIndexName("index-" + vl.getLabelName() + "-" + sb.toString().subSequence(0, sb.length() - 1));
        }

        public static GraphVertexIndex create(GraphVertexLabel vl, List<GraphVertexProperty> vpList) {
            return new GraphVertexIndex(vl, vpList);
        }

        public void setIndexName(String indexName) {
            this.indexName = indexName;
        }

        @Override
        public String getIndexName() {
            return indexName;
        }

        public List<GraphVertexProperty> getVpList() {
            return vpList;
        }

        public GraphVertexLabel getVl() {
            return vl;
        }
    }

    public static class GraphEdgeIndex extends AbstractGraphEdgeIndex {

        public GraphEdgeIndex(String indexName) {
            super(indexName);
        }

        public static GraphEdgeIndex create(String indexName) {
            return new GraphEdgeIndex(indexName);
        }

        @Override
        public String getIndexName() {
            return super.getIndexName();
        }
    }

    public static class GraphVertexPropertyValue extends AbstractGraphVertexPropertyValue<GraphVertexLabel, GraphVertexProperty> {

        public GraphVertexPropertyValue(GraphVertexLabel labels, Map values) {
            super(labels, values);
        }
    }

    public static class GraphEdgePropertyValue extends AbstractGraphEdgePropertyValue<GraphRelationship, GraphEdgeProperty> {

        public GraphEdgePropertyValue(GraphRelationship labels, Map values) {
            super(labels, values);
        }
    }

    public static class Vertex extends AbstractGraphVertex<GraphVertexLabel, GraphVertexProperty> {

        public Vertex(List labels) {
            super(labels);
        }
    }

    public static class Edge extends AbstractGraphEdge<GraphRelationship, GraphEdgeProperty> {

        public Edge(List labels) {
            super(labels);
        }
    }

    public List<GraphVertexProperty> getVertexProperties() {
        return vertexProperties;
    }

    public void setVertexProperties(List<GraphVertexProperty> vertexProperties) {
        this.vertexProperties = vertexProperties;
    }

    public List<GraphVertexIndex> getVertexIndices() {
        return vertexIndices;
    }

    public void setVertexIndices(List<GraphVertexIndex> vertexIndices) {
        this.vertexIndices = vertexIndices;
    }

    public List<GraphEdgeProperty> getEdgeProperties() {
        return edgeProperties;
    }

    public void setEdgeProperties(List<GraphEdgeProperty> edgeProperties) {
        this.edgeProperties = edgeProperties;
    }

    public List<GraphEdgeIndex> getEdgeIndices() {
        return edgeIndices;
    }

    public void setEdgeIndices(List<GraphEdgeIndex> edgeIndices) {
        this.edgeIndices = edgeIndices;
    }

    /**
     * vertex label, properties, in/out Vertex label
     */
    public void setInOutVertexLabelRelations() {
        for (GraphRelationship e : getEdgeList()) {
            GraphVertexLabel out = e.getOutLabel();
            GraphVertexLabel in = e.getInLabel();
            String outKey = out.getLabelName();
            if (outVertexLabelMap.containsKey(outKey)) {
                List<GraphVertexLabel> list = outVertexLabelMap.get(outKey);
                list.add(in);
                outVertexLabelMap.put(outKey, list);
            } else {
                List<GraphVertexLabel> list = new ArrayList<>();
                list.add(in);
                outVertexLabelMap.put(outKey, list);
            }
            String inKey = in.getLabelName();
            if (inVertexLabelMap.containsKey(inKey)) {
                List<GraphVertexLabel> list = inVertexLabelMap.get(inKey);
                list.add(out);
                inVertexLabelMap.put(inKey, list);
            } else {
                List<GraphVertexLabel> list = new ArrayList<>();
                list.add(out);
                inVertexLabelMap.put(inKey, list);
            }
        }
    }

    public Map<String, List<GraphVertexLabel>> getInVertexLabelMap() {
        return inVertexLabelMap;
    }

    public Map<String, List<GraphVertexLabel>> getOutVertexLabelMap() {
        return outVertexLabelMap;
    }
}
