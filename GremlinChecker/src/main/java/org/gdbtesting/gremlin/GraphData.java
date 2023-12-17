package org.gdbtesting.gremlin;

import org.gdbtesting.gremlin.ast.GraphConstant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implement GraphData's CRUD
 */
public class GraphData {

    private List<VertexObject> vertices;
    private List<EdgeObject> edges;

    private Map<String, List<VertexObject>> labelVertices;
    private Map<String, List<EdgeObject>> labelEdges;

    // vp1, List{value1, value2,...}
    private Map<String, List<Object>> vertexProperties = new HashMap<>();
    private Map<String, List<Object>> edgeProperties = new HashMap<>();

    // used for generating query statement
    Map<String, List<GraphConstant>> vpValuesMap = new HashMap<>();
    Map<String, List<GraphConstant>> epValueMap = new HashMap<>();

    public Map<String, List<GraphConstant>> getVpValuesMap() {
        return vpValuesMap;
    }

    public void setVpValuesMap() {
        // vp1 : [value1, value2, ...]
        for (VertexObject v : vertices) {
            Map<String, GraphConstant> vmap = v.getProperites();
            for (String vp : vmap.keySet()) {
                if (vpValuesMap.containsKey(vp)) {
                    vpValuesMap.get(vp).add(vmap.get(vp));
                } else {
                    List<GraphConstant> list = new ArrayList<>();
                    list.add(vmap.get(vp));
                    vpValuesMap.put(vp, list);
                }
            }
        }
    }

    public Map<String, List<GraphConstant>> getEpValueMap() {
        return epValueMap;
    }

    public void setEpValuesMap() {
        // ep1 : [value1, value2, ...]
        for (EdgeObject e : edges) {
            Map<String, GraphConstant> emap = e.getProperites();
            for (String ep : emap.keySet()) {
                if (epValueMap.containsKey(ep)) {
                    epValueMap.get(ep).add(emap.get(ep));
                } else {
                    List<GraphConstant> list = new ArrayList<>();
                    list.add(emap.get(ep));
                    epValueMap.put(ep, list);
                }
            }
        }
    }

    public static class VertexObject {

        private String label;

        private int id;

        private Map<String, GraphConstant> properites;

        public VertexObject() {
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public Map<String, GraphConstant> getProperites() {
            return properites;
        }

        public void setProperties(Map<String, GraphConstant> properties) {
            this.properites = properties;
        }

    }

    public static class EdgeObject {

        private String label;

        private int id;

        private GraphSchema.GraphVertexLabel outLabel;
        private GraphSchema.GraphVertexLabel inLabel;
        private Map<String, GraphConstant> properites;

        private VertexObject outVertex;
        private VertexObject inVertex;

        public VertexObject getOutVertex() {
            return outVertex;
        }

        public void setOutVertex(VertexObject outVertex) {
            this.outVertex = outVertex;
        }

        public VertexObject getInVertex() {
            return inVertex;
        }

        public void setInVertex(VertexObject inVertex) {
            this.inVertex = inVertex;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public GraphSchema.GraphVertexLabel getOutLabel() {
            return outLabel;
        }

        public void setOutLabel(GraphSchema.GraphVertexLabel outLabel) {
            this.outLabel = outLabel;
        }

        public GraphSchema.GraphVertexLabel getInLabel() {
            return inLabel;
        }

        public void setInLabel(GraphSchema.GraphVertexLabel inLabel) {
            this.inLabel = inLabel;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public Map<String, GraphConstant> getProperites() {
            return properites;
        }

        public void setProperites(Map<String, GraphConstant> properites) {
            this.properites = properites;
        }
    }

    public List<VertexObject> getVertices() {
        return vertices;
    }

    public void setVertices(List<VertexObject> vertices) {
        this.vertices = vertices;
    }

    public List<EdgeObject> getEdges() {
        return edges;
    }

    public void setEdges(List<EdgeObject> edges) {
        this.edges = edges;
    }

    public Map<String, List<Object>> getVertexProperties() {
        return vertexProperties;
    }

    public void setVertexProperties(Map<String, List<Object>> vertexProperties) {
        this.vertexProperties = vertexProperties;
    }

    public Map<String, List<Object>> getEdgeProperties() {
        return edgeProperties;
    }

    public void setEdgeProperties(Map<String, List<Object>> edgeProperties) {
        this.edgeProperties = edgeProperties;
    }

    public Map<String, List<VertexObject>> getLabelVertices() {
        return labelVertices;
    }

    public List<String> getVertexLabels() {
        List<String> list = new ArrayList<>();
        for (String s : labelVertices.keySet()) {
            list.add(s);
        }
        return list;
    }

    public List<String> getEdgeLabels() {
        List<String> list = new ArrayList<>();
        for (String s : labelEdges.keySet()) {
            list.add(s);
        }
        return list;
    }

    public void setLabelVertices(Map<String, List<VertexObject>> labelVertices) {
        this.labelVertices = labelVertices;
    }

    public Map<String, List<EdgeObject>> getLabelEdges() {
        return labelEdges;
    }

    public void setLabelEdges(Map<String, List<EdgeObject>> labelEdges) {
        this.labelEdges = labelEdges;
    }

    public void updateVertices() {
        labelVertices = new HashMap<>();
        for (VertexObject v : vertices) {
            if (labelVertices.containsKey(v.label)) {
                labelVertices.get(v.label).add(v);
            } else {
                List<VertexObject> list = new ArrayList<>();
                list.add(v);
                labelVertices.put(v.label, list);
            }
        }
    }

    public void updateEdges() {
        labelEdges = new HashMap<>();
        for (EdgeObject e : edges) {
            if (labelEdges.containsKey(e.label)) {
                labelEdges.get(e.label).add(e);
            } else {
                List<EdgeObject> list = new ArrayList<>();
                list.add(e);
                labelEdges.put(e.label, list);
            }
        }
    }

    public void updateVertex(VertexObject v, String type) {
        if (type.equals("add")) {
            if (labelVertices.containsKey(v.label)) {
                labelVertices.get(v.label).add(v);
            } else {
                List<VertexObject> list = new ArrayList<>();
                list.add(v);
                labelVertices.put(v.label, list);
            }
        }
    }

    public void updateVertexPropertyMap(Map<String, GraphConstant> prop) {
        for (String key : prop.keySet()) {
            if (vertexProperties.containsKey(key)) {
                vertexProperties.get(key).add(prop.get(key));
            } else {
                List<Object> list = new ArrayList<>();
                list.add(prop.get(key));
                vertexProperties.put(key, list);
            }
        }
    }

    public void updateEdgePropertyMap(Map<String, GraphConstant> prop) {
        for (String key : prop.keySet()) {
            if (edgeProperties.containsKey(key)) {
                edgeProperties.get(key).add(prop.get(key));
            } else {
                List<Object> list = new ArrayList<>();
                list.add(prop.get(key));
                edgeProperties.put(key, list);
            }
        }
    }
}
