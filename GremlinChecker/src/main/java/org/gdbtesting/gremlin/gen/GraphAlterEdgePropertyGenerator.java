package org.gdbtesting.gremlin.gen;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.gdbtesting.Randomly;
import org.gdbtesting.common.GDBCommon;
import org.gdbtesting.gremlin.ConstantType;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GraphSchema;
import org.gdbtesting.gremlin.GremlinPrint;

import java.util.Iterator;
import java.util.List;

public class GraphAlterEdgePropertyGenerator {

    private GraphGlobalState state;
    private GraphExpressionGenerator generator;
    private GremlinPrint print;

    public GraphAlterEdgePropertyGenerator(GraphGlobalState state) {
        this.state = state;
        this.generator = new GraphExpressionGenerator(state);
        this.print = new GremlinPrint();
    }

    public void alterEdgeProperty() {
        // Randomly choose one edge
        alterEdgeProperty(Randomly.fromList(state.getConnection().getG().E().toList()));
    }

    enum Action {
        UPDATE, ADD, DELETE
    }

    public void alterEdgeProperty(Edge e) {
        Action a = Randomly.fromOptions(Action.values());
        switch (a) {
            case ADD:
                addEdgeProperty(e);
            case DELETE:
                deleteEdgeProperty(e);
            default:
                updateEdgeProperty(e);
        }
    }

    public void addEdgeProperty(Edge e) {
        System.out.println("add property");
        GraphSchema.GraphRelationship el = state.getSchema().getEdgeLabelMap().get(e.label());
        GraphSchema.GraphEdgeProperty ep =
                new GraphSchema.GraphEdgeProperty(
                        GDBCommon.createEdgePropertyName(state.getSchema().getEdgeProperties().size()),
                        ConstantType.getRandom(), el);
        state.getSchema().getEdgeProperties().add(ep);
        System.out.println("Before add:");
        print.printEdge(e);
        e.property(ep.getEdgePropertyName(), generator.generateConstant(ep.getDataType()));
        System.out.println("After add:");
        print.printEdge(e);
        state.getEdgePropertyIndex().addAndGet(1);
    }

    public void deleteEdgeProperty(Edge e) {
        System.out.println("delete property");
        Iterator<Property<Object>> i = e.properties();
        GraphDropGenerator dropGenerator = new GraphDropGenerator(state);
        while (i.hasNext()) {
            if (Randomly.getBoolean()) {
                Property p = i.next();
                // drop property
                System.out.println("Before drop:");
                print.printEdge(e);
                dropGenerator.dropEdgeProperty(e, p);
                System.out.println("After drop:");
                print.printEdge(e);
                return;
            }
        }

    }

    public void updateEdgeProperty(Edge e) {
        System.out.println("update property");
        // randomly choose one property
        GraphSchema.GraphRelationship el = state.getSchema().getEdgeLabelMap().get(e.label());
        List<GraphSchema.GraphEdgeProperty> properties = el.getEdgeProperties();
        String property = Randomly.fromList(properties).getEdgePropertyName();
        System.out.println("Before update:");
        print.printEdge(e);
        e.property(property, generator.generateConstant(getPropertyType(property)));
        System.out.println("After update:");
        print.printEdge(e);
    }

    public ConstantType getPropertyType(String key) {
        List<GraphSchema.GraphEdgeProperty> list = state.getSchema().getEdgeProperties();
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getEdgePropertyName().equals(key)) {
                return list.get(i).getDataType();
            }
        }
        return ConstantType.STRING;
    }
}
