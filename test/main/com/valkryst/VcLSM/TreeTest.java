package main.com.valkryst.VcLSM;

import main.com.valkryst.VcLSM.node.Node;
import main.com.valkryst.VcLSM.node.NodeBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class TreeTest {
    @Test
    public void getExistingNode() {
        final Tree<String> tree = new Tree<>(1000);
        final Node<String> node = new NodeBuilder<String>().setKey("Node Key").setValue("Node Value").build();

        tree.put(node);

        final Optional<Node<String>> retrievedNode = tree.get("Node Key");
        Assert.assertTrue(retrievedNode.isPresent());
        Assert.assertEquals(node, retrievedNode.get());
    }

    @Test
    public void getNonExistingNode() {
        final Tree<String> tree = new Tree<>(1000);

        final Optional<Node<String>> retrievedNode = tree.get("Node Key");
        Assert.assertFalse(retrievedNode.isPresent());
    }

    @Test
    public void getWithNullKey() {
        final Tree<String> tree = new Tree<>(1000);
        Assert.assertFalse(tree.get(null).isPresent());
    }

    @Test
    public void getWithEmptyKey() {
        final Tree<String> tree = new Tree<>(1000);
        Assert.assertFalse(tree.get("").isPresent());
    }
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
