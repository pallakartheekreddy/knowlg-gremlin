package org.sanketika.knowlg;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

object App {
    def main(args: Array[String]): Unit = {
        val knowlgGraph = new KnowlgGraph()
        knowlgGraph.getGraphClient()
        knowlgGraph.createElements()
        //    knowlgGraph.getSubGraph()
        knowlgGraph.closeClient()
        println("created successfully")
    }
}
