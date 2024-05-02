package org.sanketika.knowlg;

import java.util
import org.apache.tinkerpop.gremlin.driver.Client
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal
import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.{outE, valueMap}
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.janusgraph.core.JanusGraph
import org.janusgraph.core.JanusGraphFactory
import org.janusgraph.core.PropertyKey
import org.janusgraph.core.schema.JanusGraphManagement

class KnowlgGraph {

    var client: Client = null
    var g: GraphTraversalSource = null

    var graph: JanusGraph = null

    @throws[Exception]
    def getGraphClient(): Unit = {
        g = traversal.withRemote("/Users/sanketika-mac1/Documents/GitHub/knowlg-gremlin/src/main/resources/remote-graph.properties")
        graph = JanusGraphFactory.open("/Users/sanketika-mac1/Documents/GitHub/knowlg-gremlin/src/main/resources/janusgraph-inmemory.properties")
        System.out.println("GraphTraversalSource: " + g)
        System.out.println("JanusGraphFactory: " + graph)
    }

    def createElements(): Unit = {
        val properties: util.Map[AnyRef, AnyRef] = new util.HashMap[AnyRef, AnyRef]
        properties.put("identifier", "NCF")
        properties.put("objectType", "Framework")
        properties.put("createdBy", "Mahesh-Mac-M1")
        g.addV("domain").property(properties).next()
    }

    def getElements(): Unit = {
        val traversal: GraphTraversal[_, _] = g.V().hasLabel("domain").has("IL_UNIQUE_ID", "NCF").repeat(outE().inV().simplePath).emit().times(5).path()
        while (traversal.hasNext) {
            val path: Any = traversal.next()
            System.out.println("Path:" + path)
        }
    }

    def getSubGraph(): Unit = {
        val subGraph: util.List[Path] = g.V().hasLabel("domain").has("IL_UNIQUE_ID", "NCF").repeat(outE().inV().simplePath).emit().times(5).path().by(valueMap()).toList
        val frameworkData: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
        subGraph.forEach((path: Path) => {
            path.forEach((prop: AnyRef) => {
                val data: AnyRef = prop.toString
                System.out.println("prop:" + data)

            })
            System.out.println("Path:" + path)

        })
    }

    def createUniqueConstraint(): Unit = {
        val mgmt: JanusGraphManagement = graph.openManagement()
        if (!mgmt.containsPropertyKey("KNOWLG_GRAPH_UNIQUE_CONSTRAINT")) {
            val name: PropertyKey = mgmt.getOrCreatePropertyKey("IL_UNIQUE_ID")
            mgmt.buildIndex("KNOWLG_GRAPH_UNIQUE_CONSTRAINT", classOf[Vertex]).addKey(name).unique().buildCompositeIndex()
            mgmt.commit()
            System.out.println("created UniqueConstraint successfully.")
        }
    }

    def updateElement(): Unit = {
        val updatedVertex: Vertex = g.V().has("domain", "identifier", "NCF").property("createdBy", "Mahesh-Mac-M1").next()
        System.out.println(updatedVertex.graph)
    }

    def retireElement(): Unit = {
        val retiredVertex: Vertex = g.V().has("domain", "identifier", "NCF").property("createdBy", "Mahesh-Mac-M1").property("retired", true).next()
        System.out.println(retiredVertex.graph)
    }

    def toCheckRetiredElement(): Unit = {
        val vertex: Vertex = g.V().has("domain", "identifier", "NCF").property("createdBy", "Mahesh-Mac-M1").next()
        val isRetired: Boolean = vertex.value("retired")
        if (isRetired) System.out.println("Vertex is retired.")
        else System.out.println("Vertex is not retired.")
    }

    def deleteElement(): Unit = {
        val dropVertex: GraphTraversal[_, _] = g.V().has("domain", "identifier", "NCF").property("createdBy", "Mahesh-Mac-M1").drop().iterate()
        System.out.println(dropVertex)
    }

    @throws[Exception]
    def closeClient(): Unit = {
        g.close()
    }
}
