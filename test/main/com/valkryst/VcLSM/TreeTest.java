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
import java.util.Optional;

public class TreeTest {
    /*
    @Test
    public void getExistingNode() {
        final Tree<String> tree = new Tree<>(1000);
        final Node<String> node = new NodeBuilder<String>().setKey("Node Key").setValue("Node Value").build();

        tree.put(node);

        final Optional<Node<String>> retrievedNode = tree.get(node.getKey());
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

        Assert.assertEquals(tree.getTotalNodes(), 1);
    }

    @Test
    public void putNullNode() {
        final Tree<String> tree = new Tree<>(1000);
        final Node<String> node = null;

        tree.put(node);

        Assert.assertEquals(tree.getTotalNodes(), 0);
    }

    @Test
    public void searchForExistingNodeInMemory() {
        final Tree<String> tree = new Tree<>(1000);
        final Node<String> node = new NodeBuilder<String>().setKey("Node Key").setValue("Node Value").build();

        tree.put(node);

        final Optional<Node<String>> retrievedNode = tree.search(node.getKey());
        Assert.assertTrue(retrievedNode.isPresent());
        Assert.assertEquals(node, retrievedNode.get());
    }
*/
    @Test
    public void searchForExistingNodeOnDisk() {
        final Tree<String> tree = new Tree<>(1);
        final Node<String> nodeA = new NodeBuilder<String>().setKey("Node Key").setValue("Node Value").build();

        for (int i = 0 ; i < 1000 ; i++) {
            final String tempKeyValue = String.valueOf(i);
            tree.put(new NodeBuilder<String>().setKey(tempKeyValue)
                                              .setValue(tempKeyValue)
                                              .build());
        }


        final Optional<Node<String>> retrievedNode = tree.search(nodeA.getKey());
        Assert.assertTrue(retrievedNode.isPresent());
        Assert.assertEquals(nodeA, retrievedNode.get());
    }

    @Test
    public void snapshotWithNullBeginningTime() {
        final Tree<String> tree = new Tree<>(1);
        tree.snapshot(null, LocalDateTime.now());
    }

    @Test
    public void snapshotWithNullEndingTime() {
        final Tree<String> tree = new Tree<>(1);
        tree.snapshot(LocalDateTime.now(), null);
    }

    @Test
    public void snapshotWithNullBeginningAndEndingTime() {
        final Tree<String> tree = new Tree<>(1);
        tree.snapshot(null, null);
    }

    @Test
    public void getTotalNodesOneThousand() {
        final Tree<String> tree = new Tree<>(100000);

        for (int i = 0 ; i < 1000 ; i++) {
            final String tempKeyValue = String.valueOf(i);
            tree.put(new NodeBuilder<String>().setKey(tempKeyValue)
                    .setValue(tempKeyValue)
                    .build());
        }

        Assert.assertEquals(tree.getTotalNodes(), 1000);
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
