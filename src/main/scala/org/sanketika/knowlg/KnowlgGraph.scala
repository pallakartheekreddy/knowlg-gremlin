package org.sanketika.knowlg

import java.util
import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.{in, outE, valueMap}
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal
import org.apache.tinkerpop.gremlin.driver.Client
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.__
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{ GraphTraversal, GraphTraversalSource}
import org.apache.tinkerpop.gremlin.structure.{Graph, Vertex}
import org.janusgraph.core.{JanusGraph, JanusGraphFactory, PropertyKey }
import org.janusgraph.core.schema.JanusGraphManagement
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.__._
import scala.collection.JavaConverters.asScalaBufferConverter


class KnowlgGraph {

    var client: Client = null
    var g: GraphTraversalSource = null

    var graph: JanusGraph = null

    @throws[Exception]
    def getGraphClient(): Unit = {
        g = traversal.withRemote("conf/remote-graph.properties")
        graph = JanusGraphFactory.open("conf/janusgraph-inmemory.properties")
        println("GraphTraversalSource: " + g)
        println("JanusGraphFactory: " + graph)
    }

    def createElements(): Unit = {
        val properties: util.Map[AnyRef, AnyRef] = new util.HashMap[AnyRef, AnyRef]
        properties.put("identifier", "NCF-3")
        properties.put("objectType", "Framework")
        properties.put("createdBy", "Mahesh-Mac-M1")
        g.addV("domain").property(properties).next()
    }

    def getElements(): Unit = {
        val traversal: GraphTraversal[_, _] = g.V().hasLabel("domain").has("IL_UNIQUE_ID", "NCF").repeat(outE().inV().simplePath).emit().times(5).path()
        val traversal1 = g.V().valueMap().toList
        println(traversal1)
        while (traversal.hasNext) {
            val path: Any = traversal.next()
            System.out.println("Path:" + path)
        }
    }

    @throws[JsonProcessingException]
    def getSubGraph(): Unit = {
        val mapper = new ObjectMapper()
        val subGraph : Graph = g.V().hasLabel("domain").has("IL_UNIQUE_ID", "NCF").repeat(bothE().subgraph("sg").otherV).until(__.loops.is(5)).cap("sg").next()
        val sg = subGraph.traversal()
        val results = sg.V().hasLabel("domain").as("vertex").project("vertexDetails", "incomingEdges", "parentVertex").by(valueMap()).by(inE().as("edge").outV.dedup().select("edge").valueMap(true)).by(in().valueMap()).dedup().toList.asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]].asScala

        val nestedJson = new util.HashMap[String, AnyRef]()
            for (result: util.Map[String, AnyRef] <- results) {
                System.out.println("result: " + result)
                val vertexDetails = result.getOrDefault("vertexDetails", new java.util.HashMap()).asInstanceOf[java.util.HashMap[String, AnyRef]]
                val identifier: String = vertexDetails.getOrDefault("identifier", new util.ArrayList[String]()).asInstanceOf[util.List[String]].get(0)
                val parentVertices= result.getOrDefault("parentVertex", new java.util.HashMap()).asInstanceOf[java.util.HashMap[String, AnyRef]]
                if (parentVertices.isEmpty) {
                    nestedJson.put(identifier, vertexDetails)
                }
                else {
                    val parentIdentifier: String = parentVertices.get("identifier").asInstanceOf[util.List[String]].get(0)
                    var parentInNestedJson= nestedJson.get(parentIdentifier).asInstanceOf[util.HashMap[String, AnyRef]]
                    if (parentInNestedJson == null) {
                        parentInNestedJson = new util.HashMap[String, AnyRef]()
                        nestedJson.put(parentIdentifier, parentInNestedJson)
                    }
                    var children = parentInNestedJson.get("children").asInstanceOf[util.HashMap[String, AnyRef]]
                    if (children == null) {
                        children = new util.HashMap[String, AnyRef]()
                        parentInNestedJson.put("children", children)
                    }
                    children.putAll(vertexDetails)
                }
            }
        val json: String = mapper.writeValueAsString(nestedJson)
        System.out.println("Nested JSON: " + json)
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
        val retiredVertex: Vertex = g.V().has("domain", "identifier", "NCF").property("createdBy", "Mahesh-Mac-M1-011").property("retired", true).next()
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
