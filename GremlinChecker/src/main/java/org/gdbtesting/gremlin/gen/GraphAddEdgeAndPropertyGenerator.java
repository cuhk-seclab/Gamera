package org.gdbtesting.gremlin.gen;

import org.gdbtesting.Randomly;
import org.gdbtesting.gremlin.GraphData;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GraphSchema;
import org.gdbtesting.gremlin.ast.GraphConstant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GraphAddEdgeAndPropertyGenerator {

    private GraphGlobalState state;
    private GraphExpressionGenerator generator;

    public List<GraphData.EdgeObject> getAddMap() { return addMap; }
    private List<GraphData.EdgeObject> addMap;
    private Map<String, GraphData.EdgeObject> checkRepeate;

    public GraphAddEdgeAndPropertyGenerator(GraphGlobalState state) {
        this.state = state;
        this.generator = new GraphExpressionGenerator(state);
        this.checkRepeate = new HashMap<>();
    }

    enum Action {
        FROM_TO, FROM, TO
    }

    public GraphData.EdgeObject generateOneEdgeWithFromTo(GraphSchema.GraphRelationship label, int id) {
        Map<String, List<GraphData.VertexObject>> vertexMaps = state.getGraphData().getLabelVertices();
        // add an Edge
        GraphData.EdgeObject e = new GraphData.EdgeObject();
        e.setLabel(label.getLabelName());
        e.setOutLabel(label.getOutLabel());

        List<GraphData.VertexObject> outList = vertexMaps.get(label.getOutLabel().getLabelName());
        if (outList == null && outList.size() == 0) {
            GraphAddVertexAndProeprtyGenerator generator = new GraphAddVertexAndProeprtyGenerator(state);
            GraphData.VertexObject v = generator.generateVertexAndProperty(label.getOutLabel(), state.getGraphData().getVertices().size());
            state.getGraphData().updateVertex(v, "add");
            e.setOutVertex(v);
        } else {
            e.setOutVertex(outList.get(Randomly.getInteger(0, outList.size())));
        }

        List<GraphData.VertexObject> inList = vertexMaps.get(label.getInLabel().getLabelName());
        if (inList == null && inList.size() == 0) {
            GraphAddVertexAndProeprtyGenerator generator = new GraphAddVertexAndProeprtyGenerator(state);
            GraphData.VertexObject v = generator.generateVertexAndProperty(label.getInLabel(), state.getGraphData().getVertices().size());
            state.getGraphData().updateVertex(v, "add");
            e.setInVertex(v);
        } else {
            e.setInVertex(inList.get(Randomly.getInteger(0, inList.size())));
        }

        e.setOutLabel(label.getOutLabel());
        e.setInLabel(label.getInLabel());
        // add properties
        List<GraphSchema.GraphEdgeProperty> properties = label.getRandomNonEmptyEdgePropertiesSubset();
        Map<String, GraphConstant> prop = new HashMap<>();
        for (int i = 0; i < properties.size(); i++) {
            prop.put(properties.get(i).getEdgePropertyName(), generator.generateConstant(properties.get(i).getDataType()));
        }
        state.getGraphData().updateEdgePropertyMap(prop);
        e.setProperites(prop);
        e.setId(id);
        return e;
    }

    public void generateEdgesAndProperties(int random) {
        for (int i = 0; i < random; i++) {
            GraphSchema.GraphRelationship label = state.getSchema().getRandomEdgeLabel();
            GraphData.EdgeObject addone = generateOneEdgeWithFromTo(label, i);
            String check = addone.getInVertex().getId() + "to" + addone.getOutVertex().getId() + "pro" + addone.getLabel();
            if (checkRepeate.get(check) == null) {
                checkRepeate.put(check, addone);
                addMap.add(addone);
            } else {
                i--;
            }
        }
    }

    public List<GraphData.EdgeObject> addEdges(int random) {
        addMap = new ArrayList<>();
        generateEdgesAndProperties(random);
        return addMap;
    }
}
