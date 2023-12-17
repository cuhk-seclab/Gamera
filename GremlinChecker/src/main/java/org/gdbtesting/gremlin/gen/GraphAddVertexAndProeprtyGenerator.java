package org.gdbtesting.gremlin.gen;

import org.gdbtesting.gremlin.GraphData;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GraphSchema;
import org.gdbtesting.gremlin.ast.GraphConstant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class GraphAddVertexAndProeprtyGenerator {
    private GraphGlobalState state;
    private GraphExpressionGenerator generator;

    public List<GraphData.VertexObject> getAddMap() {
        return addMap;
    }

    private List<GraphData.VertexObject> addMap;

    public GraphAddVertexAndProeprtyGenerator(GraphGlobalState state) {
        this.state = state;
        this.generator = new GraphExpressionGenerator(state);
    }

    public GraphData.VertexObject generateVertexAndProperty(GraphSchema.GraphVertexLabel label, int id) {
        // randomly choose vertex label
        List<GraphSchema.GraphVertexProperty> properties = label.getRandomNonEmptyVertexPropertiesSubset();
        // add a vertex
        GraphData.VertexObject v = new GraphData.VertexObject();
        v.setLabel(label.getLabelName());
        Map<String, GraphConstant> prop = new HashMap<>();
        // add properties
        for (int i = 0; i < properties.size(); i++) {
            prop.put(properties.get(i).getVertexPropertyName(), generator.generateConstant(properties.get(i).getDataType()));
        }
        state.getGraphData().updateVertexPropertyMap(prop);
        v.setProperties(prop);
        v.setId(id);
        return v;
    }

    public void generateVerticesAndProperties(int random) {
        for (int i = 0; i < random; i++) {
            GraphSchema.GraphVertexLabel label = state.getSchema().getRandomVertexLabel();
            addMap.add(generateVertexAndProperty(label, i));
        }
    }

    public List<GraphData.VertexObject> addVertices(int random) {
        addMap = new ArrayList<>();
        generateVerticesAndProperties(random);
        return addMap;
    }
}
