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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.concurrent.atomic.AtomicReference;
import java.util.*;

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
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode json = mapper.createObjectNode();
        ObjectNode frameworks = json.putObject("frameworks");
        ArrayNode categoriesNode = frameworks.putArray("categories");
        AtomicReference<String> categoryIdentifier = new AtomicReference<>("");

        List<Path> subGraph = g.V().hasLabel("domain").has("IL_UNIQUE_ID", "NCF")
                .repeat(outE().inV().simplePath()).emit().times(5).path().by(valueMap()).toList();
        subGraph.forEach(path -> {
            System.out.println("path ->" + path);
            Iterator<Object> stepIterator = path.iterator();
            while (stepIterator.hasNext()) {
                Object stepObject = stepIterator.next();
                if (stepObject instanceof Map) {
                    Map<String, Object> step = (Map<String, Object>) stepObject;

                    ArrayList<?> objectTypeList = (ArrayList<?>) step.getOrDefault("IL_FUNC_OBJECT_TYPE", new ArrayList<>());
                    String objectType = objectTypeList.size() > 0 ? (String) objectTypeList.get(0) : "Invalid";

                    ArrayList<?> identifierList = (ArrayList<?>) step.getOrDefault("IL_UNIQUE_ID", new ArrayList<>());
                    String identifier = identifierList.size() > 0 ? (String) identifierList.get(0) : "Invalid";

                    switch (objectType) {
                        case "Framework":
                            if (!frameworks.has("identifier")) {
                                frameworks.put("identifier", identifier);
                                frameworks.put("objectType", objectType);
                            }
                            break;
                        case "Category":
                            categoryIdentifier.set(identifier);
                            boolean categoryExists = false;
                            for (JsonNode category : categoriesNode) {
                                String existingIdentifier = category.get("identifier").asText();
                                if (existingIdentifier.equals(identifier)) {
                                    categoryExists = true;
                                    break;
                                }
                            }
                            if (!categoryExists) {
                                ObjectNode categoryNode = categoriesNode.addObject();
                                categoryNode.put("identifier", identifier);
                                categoryNode.put("objectType", objectType);
                                categoryNode.putArray("terms");
                            }
                            break;
                        case "Term":
                            String categoryId = categoryIdentifier.get();
                            if (categoryId != null && !categoryId.isEmpty()) {
                                ObjectNode categoryNode = findCategoryNode(categoriesNode, categoryId);
                                if (categoryNode != null) {
                                    ArrayNode termsNode = categoryNode.withArray("terms");
                                    boolean termExists = false;
                                    for (JsonNode term : termsNode) {
                                        String existingTermIdentifier = term.get("identifier").asText();
                                        if (existingTermIdentifier.equals(identifier)) {
                                            termExists = true;
                                            break;
                                        }
                                    }
                                    if (!termExists) {
                                        ObjectNode termNode = termsNode.addObject();
                                        termNode.put("identifier", identifier);
                                        termNode.put("objectType", objectType);
                                    }
                                }
                            }
                            break;

                        default:
                            break;
                    }
                }
            }
        });
        System.out.println(json);
    }

    private ObjectNode findCategoryNode(ArrayNode categoriesNode, String categoryIdentifier) {
        for (JsonNode category : categoriesNode) {
            String existingIdentifier = category.get("identifier").asText();
            if (existingIdentifier.equals(categoryIdentifier)) {
                return (ObjectNode) category;
            }
        }
        return null;
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
