package org.sanketika.knowlg;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.__.*;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;


public class KnowlgGraph {

    public static Client client;
    public static GraphTraversalSource g;

    public static JanusGraph graph;

    public void getGraphClient() throws Exception {
        g = traversal().withRemote("conf/remote-graph.properties");
        graph = JanusGraphFactory.open("conf/janusgraph-inmemory.properties");
        System.out.println("GraphTraversalSource: " + g);
        System.out.println("JanusGraphFactory: " + graph);
    }

    public void createElements() {
        Map<Object, Object> properties = new HashMap<Object, Object>();
        properties.put("identifier", "NCF");
        properties.put("objectType", "Framework");
        properties.put("createdBy", "Mahesh-Mac-M1");
        g.addV("domain").property(properties).next();
    }

    public void getElements() {
        GraphTraversal traversal = g.V().hasLabel("domain").has("IL_UNIQUE_ID", "NCF")
                .repeat(outE().inV().simplePath()).emit().times(5).path();
        while (traversal.hasNext()) {
            Object path = traversal.next();
            System.out.println("Path:" + path);
        }
    }

    public void getSubGraph() {
        List<Path> subGraph = g.V().hasLabel("domain").has("IL_UNIQUE_ID", "NCF")
                .repeat(outE().inV().simplePath()).emit().times(5).path().by(valueMap()).toList();
        Map<String, Object> frameworkData = new HashMap<>();
        subGraph.forEach(path -> {
            path.forEach(prop -> {
                Object data = prop.toString();
                System.out.println("prop:" + data);
            });
            System.out.println("Path:" + path);
        });
    }

    public void createUniqueConstraint() {
        JanusGraphManagement mgmt = graph.openManagement();
        if(!mgmt.containsPropertyKey("KNOWLG_GRAPH_UNIQUE_CONSTRAINT")){
            PropertyKey name = mgmt.getOrCreatePropertyKey("IL_UNIQUE_ID");
            mgmt.buildIndex("KNOWLG_GRAPH_UNIQUE_CONSTRAINT", Vertex.class).addKey(name).unique().buildCompositeIndex();
            mgmt.commit();
            System.out.println("created UniqueConstraint successfully.");
        }
    }

    public void updateElement(){
        Vertex updatedVertex = g.V().has("domain", "identifier", "NCF")
                .property("createdBy", "Mahesh-Mac-M1")
                .next();
        System.out.println(updatedVertex.graph());

    }

    public void retireElement(){
        Vertex retiredVertex = g.V().has("domain", "identifier", "NCF")
                .property("createdBy", "Mahesh-Mac-M1")
                .property("retired", true)
                .next();
        System.out.println(retiredVertex.graph());
    }

    public void toCheckRetiredElement(){
        Vertex vertex = g.V().has("domain", "identifier", "NCF")
                .property("createdBy", "Mahesh-Mac-M1").next();
        Boolean isRetired = vertex.value("retired");
        if (isRetired) {
            System.out.println("Vertex is retired.");
        } else {
            System.out.println("Vertex is not retired.");
        }
    }

    public void deleteElement(){
        GraphTraversal  dropVertex = g.V().has("domain", "identifier", "NCF")
                .property("createdBy", "Mahesh-Mac-M1").drop().iterate();
        System.out.println(dropVertex);
    }

    public void closeClient() throws Exception {
        g.close();
    }
}
