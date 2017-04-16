package main.com.valkryst.VcLSM;

import main.com.valkryst.VcLSM.node.Node;
import main.com.valkryst.VcLSM.node.NodeBuilder;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public class TreeTest {

    @Test
    public void getExistingNode() {
        final Tree tree = new Tree(1000);
        final Node node = new NodeBuilder().setKey("Node Key").setValue("Node Value").build();

        tree.put(node);

        final Optional<Node> retrievedNode = tree.get(node.getKey());
        Assert.assertTrue(retrievedNode.isPresent());
        Assert.assertEquals(node, retrievedNode.get());
    }

    @Test
    public void getNonExistingNode() {
        final Tree tree = new Tree(1000);

        final Optional<Node> retrievedNode = tree.get("Node Key");
        Assert.assertFalse(retrievedNode.isPresent());
    }

    @Test
    public void getWithNullKey() {
        final Tree tree = new Tree(1000);
        Assert.assertFalse(tree.get(null).isPresent());
    }

    @Test
    public void getWithEmptyKey() {
        final Tree tree = new Tree(1000);
        Assert.assertFalse(tree.get("").isPresent());
    }
    @Test
    public void putValidNode() {
        final Tree tree = new Tree(1000);
        final Node node = new NodeBuilder().setKey("Node Key").setValue("Node Value").build();

        tree.put(node);

        Assert.assertEquals(1, tree.getTotalNodes());
    }

    @Test
    public void putNullNode() {
        final Tree tree = new Tree(1000);
        final Node node = null;

        tree.put(node);

        Assert.assertEquals(0, tree.getTotalNodes());
    }

    @Test
    public void searchForExistingNodeInMemory() {
        final Tree tree = new Tree(1000);
        final Node node = new NodeBuilder().setKey("Node Key").setValue("Node Value").build();

        tree.put(node);

        final Optional<Node> retrievedNode = tree.search(node.getKey());
        Assert.assertTrue(retrievedNode.isPresent());
        Assert.assertEquals(node, retrievedNode.get());

        deleteDataDirectory();
    }

    @Test
    public void searchForExistingNodeOnDisk() {
        final Tree tree = new Tree(1);
        final Node nodeA = new NodeBuilder().setKey("Node Key").setValue("Node Value").build();
        tree.put(nodeA);

        for (int i = 0 ; i < 1000 ; i++) {
            final String tempKeyValue = String.valueOf(i);
            tree.put(new NodeBuilder().setKey(tempKeyValue)
                                      .setValue(tempKeyValue)
                                      .build());
        }


        final Optional<Node> retrievedNode = tree.search(nodeA.getKey());
        Assert.assertTrue(retrievedNode.isPresent());
        Assert.assertEquals(nodeA, retrievedNode.get());

        deleteDataDirectory();
    }

    @Test
    public void snapshotWithNullBeginningTime() {
        final Tree tree = new Tree(1);
        tree.snapshot(null, LocalDateTime.now());
    }

    @Test
    public void snapshotWithNullEndingTime() {
        final Tree tree = new Tree(1);
        tree.snapshot(LocalDateTime.now(), null);
    }

    @Test
    public void snapshotWithNullBeginningAndEndingTime() {
        final Tree tree = new Tree(1);
        tree.snapshot(null, null);
    }

    @Test
    public void getSnapshotWithOneUniqueReturnableNodeInMemory() {
        final Tree tree = new Tree(1000);

        final LocalDateTime beginningTime = LocalDateTime.now();
        final LocalDateTime endingTime = LocalDateTime.MAX;

        final Node nodeA = new NodeBuilder().setKey("Node Key").setTime(beginningTime).setValue("Node Value").build();
        final Node nodeB = new NodeBuilder().setKey("Node Key").setTime(beginningTime).setValue("Node Value").build();

        tree.put(nodeA);
        tree.put(nodeB);

        final List<Node> snapshot = tree.snapshot(beginningTime, endingTime);
        Assert.assertEquals(1, snapshot.size());

        deleteDataDirectory();
    }

    @Test
    public void getSnapshotWithTwoUniqueReturnableNodesInMemory() {
        final Tree tree = new Tree(1000);

        final LocalDateTime beginningTime = LocalDateTime.now();
        final LocalDateTime endingTime = LocalDateTime.MAX;

        final Node nodeA = new NodeBuilder().setKey("Node Key A").setTime(beginningTime).setValue("Node Value").build();
        final Node nodeB = new NodeBuilder().setKey("Node Key B").setTime(endingTime).setValue("Node Value").build();

        tree.put(nodeA);
        tree.put(nodeB);

        final List<Node> snapshot = tree.snapshot(beginningTime, endingTime);
        Assert.assertEquals(2, snapshot.size());

        deleteDataDirectory();
    }

    @Test
    public void getSnapshotWithOneUniqueReturnableNodeOnDisk() {
        final Tree tree = new Tree(1);

        final LocalDateTime beforeStartTime = LocalDateTime.of(1, 1, 1, 1, 1);
        final LocalDateTime beginningTime = LocalDateTime.now();
        final LocalDateTime endingTime = LocalDateTime.MAX;

        final Node nodeA = new NodeBuilder().setKey("Node Key").setTime(beforeStartTime).setValue("Node Value").build();
        final Node nodeB = new NodeBuilder().setKey("Node Key").setTime(beforeStartTime).setValue("Node Value").build();
        final Node nodeC = new NodeBuilder().setKey("Node Key").setTime(beginningTime).setValue("Node Value").build();
        final Node nodeD = new NodeBuilder().setKey("Node Key").setTime(beginningTime).setValue("Node Value").build();

        tree.put(nodeA);
        tree.put(nodeB);
        tree.put(nodeC);
        tree.put(nodeD);

        // Fill the tree with 1000 random nodes to ensure our first 4 nodes are
        // merged to disk before doing a snapshot:
        for (int i = 0 ; i < 1000 ; i++) {
            final String tempKeyValue = String.valueOf(i);
            tree.put(new NodeBuilder().setKey(tempKeyValue)
                    .setValue(tempKeyValue)
                    .build());
        }

        final List<Node> snapshot = tree.snapshot(beginningTime, endingTime);
        Assert.assertEquals(1, snapshot.size());

        deleteDataDirectory();
    }

    @Test
    public void getSnapshotWithTwoUniqueReturnableNodesOnDisk() {
        final Tree tree = new Tree(1);

        final LocalDateTime beforeStartTime = LocalDateTime.of(1, 1, 1, 1, 1);
        final LocalDateTime beginningTime = LocalDateTime.now();
        final LocalDateTime endingTime = LocalDateTime.MAX;

        final Node nodeA = new NodeBuilder().setKey("Node Key A").setTime(beforeStartTime).setValue("Node Value").build();
        final Node nodeB = new NodeBuilder().setKey("Node Key B").setTime(beginningTime).setValue("Node Value").build();

        tree.put(nodeA);
        tree.put(nodeB);

        // Fill the tree with 1000 random nodes to ensure our first 4 nodes are
        // merged to disk before doing a snapshot:
        for (int i = 0 ; i < 1000 ; i++) {
            final String tempKeyValue = String.valueOf(i);
            tree.put(new NodeBuilder().setKey(tempKeyValue)
                    .setValue(tempKeyValue)
                    .build());
        }

        final List<Node> snapshot = tree.snapshot(beginningTime, endingTime);
        Assert.assertEquals(2, snapshot.size());

        deleteDataDirectory();
    }

    @Test
    public void getTotalNodesOneThousand() {
        final Tree tree = new Tree(100000);

        for (int i = 0 ; i < 1000 ; i++) {
            final String tempKeyValue = String.valueOf(i);
            tree.put(new NodeBuilder().setKey(tempKeyValue)
                    .setValue(tempKeyValue)
                    .build());
        }

        Assert.assertEquals(1000, tree.getTotalNodes());

        deleteDataDirectory();
    }

    @AfterClass
    public static void deleteDataDirectory() {
        try {
            FileUtils.deleteDirectory(new File("data/"));
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }
}
