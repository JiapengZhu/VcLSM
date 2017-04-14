package main.com.valkryst.VcLSM;

import main.com.valkryst.VcLSM.node.Node;
import main.com.valkryst.VcLSM.node.NodeBuilder;
import org.junit.Test;

public class TreeTest {
    @Test
    public void putValidNode() {
        final Tree<String> tree = new Tree<>(1000);
        final Node<String> node = new NodeBuilder<String>().setKey("Node Key").setValue("Node Value").build();

        tree.put(node);
    }

    @Test
    public void putNullNode() {
        final Tree<String> tree = new Tree<>(1000);
        final Node<String> node = null;

        tree.put(node);
    }
}
