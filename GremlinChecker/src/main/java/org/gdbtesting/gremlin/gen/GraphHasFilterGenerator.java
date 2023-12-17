package org.gdbtesting.gremlin.gen;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.gdbtesting.Randomly;
import org.gdbtesting.gremlin.GraphGlobalState;
import org.gdbtesting.gremlin.GraphSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class GraphHasFilterGenerator {

    /*  private List<Vertex> vertexList;*/
    /*    private List<String> idList;*/
    private List<GraphSchema.GraphVertexProperty> vertexPropertyList;
    private Map<String, Object> propertyMap = new HashMap<>();
    private List<GraphSchema.GraphVertexLabel> vertexLabels;
    private GraphGlobalState state;

    public GraphHasFilterGenerator(List<GraphSchema.GraphVertexLabel> vertexLabels, List<GraphSchema.GraphVertexProperty> properties,
                                   GraphGlobalState state) {
        this.vertexLabels = vertexLabels;
        this.vertexPropertyList = properties;
        this.state = state;
    }

    private enum HasFilter {
        //        HAS_KEY_VALUE, // has(key, value)
//        HAS_LABEL_KEY_VALUE, // has(label, key, value)
//        HAS_KEY_PREDICATE,
        HAS_LABEL,
        //        HAS_ID,
        HAS_KEY,
        HAS_NOT_KEY
    }

    public GraphTraversal getHasFilter(GraphTraversal previous) {
        System.out.println("get has filter");
        switch (Randomly.fromOptions(HasFilter.values())) {
            case HAS_LABEL:
                String label = Randomly.fromList(vertexLabels).getLabelName();
                System.out.println("has_label: " + label);
                return previous.hasLabel(label).dedup();
            /*case HAS_ID:
                return previous.hasId(Randomly.fromList(idList));*/
            case HAS_KEY:
                String key = Randomly.fromList(vertexPropertyList).getVertexPropertyName();
                System.out.println("has_key: " + key);
                return previous.has(key).dedup();
            case HAS_NOT_KEY:
                String not_key = Randomly.fromList(vertexPropertyList).getVertexPropertyName();
                System.out.println("has_not_key: " + not_key);
                return previous.hasNot(Randomly.fromList(vertexPropertyList).getVertexPropertyName()).dedup();
            /*case HAS_KEY_VALUE:
                String key = Randomly.getOneInfo(propertyMap.keySet());
                return previous.has(key, propertyMap.get(key));*/
        }
        return previous;
    }

}
