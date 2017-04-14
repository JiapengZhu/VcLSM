package main.com.valkryst.VcLSM.node;

import org.junit.Assert;
import org.junit.Test;

public class NodeTest {
    @Test
    public void constructWithBuilder() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        builder.setKey("Node Key");
        builder.setValue("Node Value");
        builder.build();
    }

    @Test
    public void equalsWithSelf() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        builder.setKey("Node Key");
        builder.setValue("Node Value");

        final Node<String> node = builder.build();
        Assert.assertTrue(node.equals(node));
    }

    @Test
    public void equalsWithDuplicate() {
        final NodeBuilder<String> builderA = new NodeBuilder<>();
        builderA.setKey("Node Key");
        builderA.setValue("Node Value");

        final NodeBuilder<String> builderB = new NodeBuilder<>();
        builderB.setKey("Node Key");
        builderB.setValue("Node Value");

        final Node<String> nodeA = builderA.build();
        final Node<String> nodeB = builderB.build();
        Assert.assertTrue(nodeA.equals(nodeB));
    }

    @Test
    public void equalsWithDifferentNode() {
        final NodeBuilder<String> builderA = new NodeBuilder<>();
        builderA.setKey("Node Key A");
        builderA.setValue("Node Value A");

        final NodeBuilder<String> builderB = new NodeBuilder<>();
        builderB.setKey("Node Key B");
        builderB.setValue("Node Value B");

        final Node<String> nodeA = builderA.build();
        final Node<String> nodeB = builderB.build();
        Assert.assertFalse(nodeA.equals(nodeB));
    }

    @Test
    public void equalsWithNull() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        builder.setKey("Node Key");
        builder.setValue("Node Value");
        Assert.assertFalse(builder.build().equals(null));
    }

    @Test
    public void equalsWithWrongObjectType() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        builder.setKey("Node Key");
        builder.setValue("Node Value");
        Assert.assertFalse(builder.build().equals(5));
    }
}
