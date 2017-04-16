package main.com.valkryst.VcLSM.node;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;

public class NodeListTest {
    @Test
    public void removeDuplicateNodesTwoDuplicateNodesWithSameTimes() {
        final NodeList nodeList = new NodeList();

        final LocalDateTime now = LocalDateTime.now();

        final Node nodeA = new NodeBuilder().setKey("Node Key").setTime(now).setValue("Node Value").build();
        final Node nodeB = new NodeBuilder().setKey("Node Key").setTime(now).setValue("Node Value").build();

        nodeList.add(nodeA);
        nodeList.add(nodeB);

        nodeList.removeDuplicateNodes();

        Assert.assertEquals(1, nodeList.size());
        Assert.assertTrue(nodeList.get(0).equals(nodeA));
    }

    @Test
    public void removeDuplicateNodesTwoDuplicateNodesWithDifferentTimes() {
        final NodeList nodeList = new NodeList();

        final LocalDateTime old = LocalDateTime.MIN;
        final LocalDateTime now = LocalDateTime.now();

        final Node nodeA = new NodeBuilder().setKey("Node Key").setTime(old).setValue("Node Value").build();
        final Node nodeB = new NodeBuilder().setKey("Node Key").setTime(now).setValue("Node Value").build();

        nodeList.add(nodeA);
        nodeList.add(nodeB);

        nodeList.removeDuplicateNodes();

        Assert.assertEquals(1, nodeList.size());
    }

    @Test
    public void removeDuplicateNodesFourDuplicateNodesWithSameTimes() {
        final NodeList nodeList = new NodeList();

        final LocalDateTime now = LocalDateTime.now();

        final Node nodeA = new NodeBuilder().setKey("Node Key").setTime(now).setValue("Node Value").build();
        final Node nodeB = new NodeBuilder().setKey("Node Key").setTime(now).setValue("Node Value").build();
        final Node nodeC = new NodeBuilder().setKey("Node Key").setTime(now).setValue("Node Value").build();
        final Node nodeD = new NodeBuilder().setKey("Node Key").setTime(now).setValue("Node Value").build();

        nodeList.add(nodeA);
        nodeList.add(nodeB);
        nodeList.add(nodeC);
        nodeList.add(nodeD);

        nodeList.removeDuplicateNodes();

        Assert.assertEquals(1, nodeList.size());
        Assert.assertTrue(nodeList.get(0).equals(nodeD));
    }

    @Test
    public void removeDuplicateNodesFourDuplicateNodesWithDifferentTimes() {
        final NodeList nodeList = new NodeList();

        final LocalDateTime timeA = LocalDateTime.of(0, 1, 1,1, 1);
        final LocalDateTime timeB = LocalDateTime.of(1, 1, 1,1, 1);
        final LocalDateTime timeC = LocalDateTime.of(2, 1, 1,1, 1);
        final LocalDateTime timeD = LocalDateTime.of(3, 1, 1,1, 1);

        final Node nodeA = new NodeBuilder().setKey("Node Key").setTime(timeA).setValue("Node Value").build();
        final Node nodeB = new NodeBuilder().setKey("Node Key").setTime(timeB).setValue("Node Value").build();
        final Node nodeC = new NodeBuilder().setKey("Node Key").setTime(timeC).setValue("Node Value").build();
        final Node nodeD = new NodeBuilder().setKey("Node Key").setTime(timeD).setValue("Node Value").build();

        nodeList.add(nodeA);
        nodeList.add(nodeB);
        nodeList.add(nodeC);
        nodeList.add(nodeD);

        nodeList.removeDuplicateNodes();

        Assert.assertEquals(1, nodeList.size());
        Assert.assertTrue(nodeList.get(0).equals(nodeD));
    }
}
