package main.com.valkryst.VcLSM.node;

import java.lang.instrument.Instrumentation;

public class NodeInstrumentation {
    /** Handle to instance of the Instrumentation interface. */
    private static Instrumentation instrumentation;

    /**
     * Implementation of the overloaded premain method that is first invoked by
     * the JVM during use of instrumentation.
     *
     * @param args
     *         Agent options provided as a single String.
     *
     * @param inst
     *         Handle to instance of Instrumentation provided on command-line.
     */
    public static void premain(final String args, final Instrumentation inst) {
        instrumentation = inst;
    }

    /**
     * Implementation of the overloaded premain method that is first invoked by
     * the JVM during use of instrumentation.
     *
     * @param args
     *         Agent options provided as a single String.
     *
     * @param inst
     *         Handle to instance of Instrumentation provided on command-line.
     */
    public static void agentmain(final String args, final Instrumentation inst) {
        instrumentation = inst;
    }

    /**
     * Estimates the size of the specified node, in bytes.
     *
     * @param node
     *         The node.
     *
     * @return
     *         The estimated size of the node, in bytes.
     */
    public static long getNodeSize(final Node node) {
        return instrumentation.getObjectSize(node);
    }
}
