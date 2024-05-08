package org.sanketika.knowlg;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

object App {
    def main(args: Array[String]): Unit = {
        val knowlgGraph = new KnowlgGraph()
        knowlgGraph.getGraphClient()
//        knowlgGraph.getElements()
        knowlgGraph.getSubGraph()
//        knowlgGraph.createOneMillionVerticesConcurrently()
        knowlgGraph.closeClient()
        println("created successfully")
    }
}
