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
import org.janusgraph.core.{JanusGraph, JanusGraphFactory}
import org.apache.tinkerpop.gremlin.structure.{Direction, Edge}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.__._
import com.fasterxml.jackson.databind.node.ArrayNode

import java.util.concurrent.{Executors, ThreadPoolExecutor}


class KnowlgGraph {

    var client: Client = null
    var g: GraphTraversalSource = null

    var graph: JanusGraph = null
    val mapper = new ObjectMapper()

    @throws[Exception]
    def getGraphClient(): Unit = {
        g = traversal.withRemote("conf/remote-graph.properties")
        graph = JanusGraphFactory.open("conf/janusgraph-inmemory.properties")
        println("GraphTraversalSource: " + g)
//        graph = JanusGraphFactory.open("conf/remote-graph.properties")
        println("graph: " + graph)
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
    def getSubGraph(): String = {
        val subGraph: Graph = g.V().hasLabel("domain").has("IL_UNIQUE_ID", "NCF")
          .repeat(bothE().subgraph("sg").otherV).until(__.loops.is(5))
          .cap("sg").next()
        val json = getSubGraphJson(subGraph)
        println("json: " + json)
        json
    }

    def getSubGraphJson(subGraph: Graph): String = {
        val rootObject = mapper.createObjectNode()
        val sg = subGraph.traversal()
        var parentIdentifier: Option[String] = None

        sg.V().forEachRemaining { vertex: Vertex =>
            val vertexDetails = mapper.createObjectNode()
            vertex.keys().forEach { key =>
                vertexDetails.put(key, vertex.value(key).toString)
            }
            val edgeDetails = getEdgeDetails(sg, vertex)
            if (edgeDetails.size() > 0) {
                edgeDetails.forEach { edgeDetail =>
                    edgeDetail.fields().forEachRemaining { field =>
                        vertexDetails.put(field.getKey, field.getValue.asText())
                    }
                }
            }
            val childrenArray = findChildren(sg, vertex)
            if (childrenArray.size() > 0) {
                vertexDetails.set("children", childrenArray)
            }
            rootObject.set(vertexDetails.get("identifier").asText, vertexDetails)
            if (!vertex.edges(Direction.IN).hasNext) {
                parentIdentifier = Some(vertexDetails.get("identifier").asText)
            }
        }
        parentIdentifier.flatMap(parentId => Option(rootObject.get(parentId))).map(_.toString).getOrElse("{}")
    }

    def findChildren(sg: GraphTraversalSource, parent: Vertex): ArrayNode = {
        val childrenArray = mapper.createArrayNode()
        sg.V(parent).out().forEachRemaining { child: Vertex =>
            val childDetails = mapper.createObjectNode()
            child.keys().forEach { key =>
                childDetails.put(key, child.value(key).toString)
            }
            val edgeDetails = getEdgeDetails(sg, child)
            if (edgeDetails.size() > 0) {
                edgeDetails.forEach { edgeDetail =>
                    edgeDetail.fields().forEachRemaining { field =>
                        childDetails.put(field.getKey, field.getValue.asText())
                    }
                }
            }
            val grandChildrenArray = findChildren(sg, child)
            if (grandChildrenArray.size() > 0) {
                childDetails.set("children", grandChildrenArray)
            }
            childrenArray.add(childDetails)
        }
        childrenArray
    }

    def getEdgeDetails(sg: GraphTraversalSource, vertex: Vertex): ArrayNode = {
        val edgeArray = mapper.createArrayNode()
        sg.V(vertex).inE().forEachRemaining { edge: Edge =>
            val edgeDetails = mapper.createObjectNode()
            edge.keys().forEach { key =>
                edgeDetails.put(key, edge.value(key).toString)
            }
            edgeDetails.put("label", edge.label())
            edgeArray.add(edgeDetails)
        }
        edgeArray
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
    def createOneMillionVerticesConcurrently(): Unit = {
        val numVertices = 1000000
        val executor = Executors.newFixedThreadPool(20).asInstanceOf[ThreadPoolExecutor]
        val start = System.currentTimeMillis()
        println("Starting the loop at " + start)
        for (i <- 1 to numVertices) {
            executor.execute(new Runnable {
                override def run(): Unit = {
                    val tx = graph.newTransaction()
                    try {
                        val vertex = tx.addVertex()
                        vertex.property("identifier", s"do_$i")
                        vertex.property("objectType", "Content")
                        println(s"Thread ${Thread.currentThread().getName} - Vertex $i created with id: ${vertex.id()}")
                        tx.commit()
                    } catch {
                        case e: Exception =>
                            println(s"Exception while creating vertex $i: $e")
                            tx.rollback()
                    } finally {
                        tx.close()
                    }
                }
            })
        }

        executor.shutdown()
        while (!executor.isTerminated) {}
        val end = System.currentTimeMillis()
        println("Loop finished at " + end)
        println("Time taken to create 1 million vertices: " + (end - start) + " milliseconds")
    }

    def closeClient(): Unit = {
        g.close()
        graph.close()
    }
}