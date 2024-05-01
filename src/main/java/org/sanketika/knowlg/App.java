package org.sanketika.knowlg;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class App {

    public static void main(String[] args) throws Exception {
        KnowlgGraph knowlgGraph = new KnowlgGraph();
        knowlgGraph.getGraphClient();
        knowlgGraph.getSubGraph();
        knowlgGraph.closeClient();
        System.out.println("Created the elements successfully.");

    }
}
