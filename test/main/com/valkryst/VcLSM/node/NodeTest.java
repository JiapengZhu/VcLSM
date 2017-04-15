package main.com.valkryst.VcLSM.node;

import org.junit.Assert;
import org.junit.Test;

public class NodeTest {
    @Test
    public void constructWithBuilder() {
        final NodeBuilder builder = new NodeBuilder();
        builder.setKey("Node Key");
        builder.setValue("Node Value");
        builder.build();
    }

    @Test
    public void equalsWithSelf() {
        final NodeBuilder builder = new NodeBuilder();
        builder.setKey("Node Key");
        builder.setValue("Node Value");

        final Node node = builder.build();
        Assert.assertTrue(node.equals(node));
    }

    @Test
    public void equalsWithDuplicate() {
        final NodeBuilder builderA = new NodeBuilder();
        builderA.setKey("Node Key");
        builderA.setValue("Node Value");

        final NodeBuilder builderB = new NodeBuilder();
        builderB.setKey("Node Key");
        builderB.setValue("Node Value");

        final Node nodeA = builderA.build();
        final Node nodeB = builderB.build();
        Assert.assertTrue(nodeA.equals(nodeB));
    }

    @Test
    public void equalsWithDifferentNode() {
        final NodeBuilder builderA = new NodeBuilder();
        builderA.setKey("Node Key A");
        builderA.setValue("Node Value A");

        final NodeBuilder builderB = new NodeBuilder();
        builderB.setKey("Node Key B");
        builderB.setValue("Node Value B");

        final Node nodeA = builderA.build();
        final Node nodeB = builderB.build();
        Assert.assertFalse(nodeA.equals(nodeB));
    }

    @Test
    public void equalsWithNull() {
        final NodeBuilder builder = new NodeBuilder();
        builder.setKey("Node Key");
        builder.setValue("Node Value");
        Assert.assertFalse(builder.build().equals(null));
    }

    @Test
    public void equalsWithWrongObjectType() {
        final NodeBuilder builder = new NodeBuilder();
        builder.setKey("Node Key");
        builder.setValue("Node Value");
        Assert.assertFalse(builder.build().equals(5));
    }
}
