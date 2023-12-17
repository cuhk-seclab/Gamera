package org.gdbtesting.gremlin.gen;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.gdbtesting.Randomly;
import org.gdbtesting.common.GDBCommon;
import org.gdbtesting.gremlin.ConstantType;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GraphSchema;
import org.gdbtesting.gremlin.GremlinPrint;

import java.util.Iterator;
import java.util.List;

/**
 * Actions:
 * UPDATE: Update Property value
 * ADD: Add Property
 * DELETE: Delete Property
 */
public class GraphAlterVertexPropertyGeneration {

    private GraphGlobalState state;
    private GraphExpressionGenerator generator;
    private GremlinPrint print;

    public GraphAlterVertexPropertyGeneration(GraphGlobalState state) {
        this.state = state;
        this.generator = new GraphExpressionGenerator(state);
        this.print = new GremlinPrint();
    }

    public void alterVertexProperty() {
        // Randomly choose one vertex
        alterVertexProperty(Randomly.fromList(state.getConnection().getG().V().toList()));
    }

    enum Action {
        UPDATE, ADD, DELETE
    }

    public void alterVertexProperty(Vertex v) {
        Action a = Randomly.fromOptions(Action.values());
        switch (a) {
            case ADD:
                addVertexProperty(v);
            case DELETE:
                deleteVertexProperty(v);
            default:
                updateVertexProperty(v);
        }
    }

    public void addVertexProperty(Vertex v) {
        System.out.println("add property");
        GraphSchema.GraphVertexLabel vl = state.getSchema().getVertexLabelMap().get(v.label());
        // add a new vertex property
        GraphSchema.GraphVertexProperty vp =
                new GraphSchema.GraphVertexProperty(
                        GDBCommon.createVertexPropertyName(state.getSchema().getVertexProperties().size()),
                        ConstantType.getRandom(), vl);
        state.getSchema().getVertexProperties().add(vp);
        System.out.println("Before add:");
        print.printVertex(v);
        v.property(vp.getVertexPropertyName(), generator.generateConstant(vp.getDataType()));
        System.out.println("After add:");
        print.printVertex(v);
        state.getVertexPropertyIndex().addAndGet(1);
    }

    public void deleteVertexProperty(Vertex v) {
        System.out.println("delete property");
        Iterator<VertexProperty<Object>> i = v.properties();
        GraphDropGenerator dropGenerator = new GraphDropGenerator(state);
        while (i.hasNext()) {
            if (Randomly.getBoolean()) {
                VertexProperty p = i.next();
                // drop property
                System.out.println("Before drop:");
                print.printVertex(v);
                dropGenerator.dropVertexProperty(v, p);
                System.out.println("After drop:");
                print.printVertex(v);
                return;
            }
        }

    }

    public void updateVertexProperty(Vertex v) {
        System.out.println("update property");
        // randomly choose one property
        GraphSchema.GraphVertexLabel vl = state.getSchema().getVertexLabelMap().get(v.label());
        List<GraphSchema.GraphVertexProperty> properties = vl.getVertexProperties();
        String property = Randomly.fromList(properties).getVertexPropertyName();
        System.out.println("Before update:");
        print.printVertex(v);
        v.property(property, generator.generateConstant(getPropertyType(property)));
        System.out.println("After update:");
        print.printVertex(v);
    }

    public ConstantType getPropertyType(String key) {
        List<GraphSchema.GraphVertexProperty> list = state.getSchema().getVertexProperties();
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getVertexPropertyName().equals(key)) {
                return list.get(i).getDataType();
            }
        }
        return ConstantType.STRING;
    }

}
