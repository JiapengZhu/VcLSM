package main.com.valkryst.VcLSM.node;

import org.junit.Assert;
import org.junit.Test;

public class NodeBuilderTest {
    @Test
    public void buildValidNode() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        builder.setKey("Node Key");
        builder.setValue("Node Value");

        final Node<String> node = builder.build();

        Assert.assertTrue(node.getKey().equals("Node Key"));
        Assert.assertTrue(node.getValue().equals("Node Value"));
    }

    @Test(expected = IllegalStateException.class)
    public void buildNodeWithNullKey() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        builder.setKey(null);
        builder.setValue("Node Value");

        builder.build();
    }

    @Test(expected = IllegalStateException.class)
    public void buildNodeWithNullValue() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        builder.setKey("Node Key");
        builder.setValue(null);

        builder.build();
    }

    @Test(expected = IllegalStateException.class)
    public void buildNodeWithNullKeyAndValue() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        builder.setKey(null);
        builder.setValue(null);

        builder.build();
    }

    @Test(expected = IllegalStateException.class)
    public void buildNodeWithEmptyKey() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        builder.setKey("");
        builder.setValue("Node Value");

        builder.build();
    }

    @Test(expected = IllegalStateException.class)
    public void buildNodeWithEmptyValue() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        builder.setKey("Node Key");
        builder.setValue("");

        builder.build();
    }

    @Test(expected = IllegalStateException.class)
    public void buildNodeWithEmptyKeyAndValue() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        builder.setKey("");
        builder.setValue("");

        builder.build();
    }

    @Test
    public void setValidKey() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        builder.setKey("Node Key");
    }

    @Test
    public void setNullKey() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        builder.setKey(null);
    }

    @Test
    public void setKeyReturnValueCheck() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        Assert.assertEquals(builder.setKey("Node Key"), builder);
    }

    @Test
    public void setValidValue() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        builder.setValue("Node Value");
    }

    @Test
    public void setNullValue() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        builder.setValue(null);
    }

    @Test
    public void setValueReturnValueCheck() {
        final NodeBuilder<String> builder = new NodeBuilder<>();
        Assert.assertEquals(builder.setValue("Node Value"), builder);
    }
}
