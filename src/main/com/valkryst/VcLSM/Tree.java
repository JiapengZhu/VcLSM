package main.com.valkryst.VcLSM;

import main.com.valkryst.VcLSM.node.Node;
import main.com.valkryst.VcLSM.node.NodeInstrumentation;
import main.com.valkryst.VcLSM.node.NodeList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Tree {
    /** The maximum size of the tree, in bytes, before a Merge must occur. */
    private final int maximumSize;
    /** The current size of the tree, in bytes. */
    private int currentSize = 0;
    /** The underlying data structure of the tree. */
    private final ConcurrentSkipListMap<String, Node> map = new ConcurrentSkipListMap<>();
    private final Lock writeLock = new ReentrantReadWriteLock().writeLock();


    /**
     * Constructs a new tree.
     *
     * @param maximumSize
     *         The maximum size of the tree, in kilobytes, before a Merge must occur.
     */
    public Tree(final int maximumSize) throws IllegalArgumentException {
        if (maximumSize <= 0) {
            final Logger logger = LogManager.getLogger();
            logger.error("The maximumSize of a Tree cannot be less than 1 kilobyte.");

            this.maximumSize = 1000; // 1 Kilobyte
        } else {
            this.maximumSize = maximumSize * 1000; // 1000 bytes = 1 kilobyte
        }
    }

    /**
     * Gets the most recent node whose key matches the specified key.
     *
     * Only gets from the in-memory tree.
     *
     * @param key
     *         The key.
     *
     * @return
     *         The found node.
     */
    public Optional<Node> get(final String key) {
        if (key == null || key.isEmpty()) {
            return Optional.empty();
        }

        for (final Map.Entry<String, Node> entry : map.entrySet()) {
            final String nodeKey = entry.getValue().getKey();

            if (nodeKey.equals(key)) {
                return Optional.of(entry.getValue());
            }
        }

        return Optional.empty();
    }

    /**
     * Puts the specified node into the tree.
     *
     * If a null node is specified, then nothing happens.
     *
     * @param node
     *         The node.
     */
    public void put(final Node node) {
        if (node == null) {
            return;
        }

        final long estimatedNodeSize = NodeInstrumentation.getNodeSize(node);

        // If the tree will exceed it's maximum size by adding the new node,
        // then perform a merge before adding the new node.
        if (currentSize + estimatedNodeSize >= maximumSize) {
            merge(estimatedNodeSize);
        }

        map.put(node.getKeyWithTimestamp(), node);
        currentSize += estimatedNodeSize;
    }

    /**
     * Searches for the most recent node whose key matches the specified key.
     *
     * @param key
     *         The key.
     *
     * @return
     *         The found node.
     */
    public Optional<Node> search(final String key) {
        if (key == null || key.isEmpty()) {
            return Optional.empty();
        }

        // Search the in-memory map:
        Optional<Node> tmp = get(key);

        if (tmp.isPresent()) {
            return tmp;
        }

        // Search each of the on-disk files
        return new FileSearcher().search(key);
    }

    private void merge(final long estimatedNodeSize) {
        // Before Merge:
        writeLock.lock();

        // A merge may have already happened, so we perform a second check to determine if we must still perform
        // the merge.
        if (currentSize + estimatedNodeSize < maximumSize) {
            writeLock.unlock();
            return;
        }

        final ConcurrentSkipListMap<String, Node> newMap = new ConcurrentSkipListMap<> ();
        newMap.putAll(map);

        map.clear();
        currentSize = 0;

        writeLock.unlock();

        // Merge:
        try {
            final FileMerger fileMerger = new FileMerger();

            fileMerger.mergeToDisk(newMap, System.currentTimeMillis() + ".dat");
            fileMerger.mergeOnDiskFiles(maximumSize);
        } catch (final IOException | IllegalStateException e) {
            final Logger logger = LogManager.getLogger();
            logger.error(e.getMessage());

            map.putAll(newMap);
        }
    }

    /**
     * Retrieves all of the nodes created at, and between, the beginning and ending times.
     * If two nodes share the same key, then only the most-recent node is returned.
     *
     * @param beginning
     *         The earliest time that a node can have been created, in order to be returned.
     *
     * @param ending
     *         The latest time that a node can have been created, in order to be returned.
     *
     * @return
     *         A collection of nodes, with unique keys, created at, and between, the beginning and ending times.
     */
    public List<Node> snapshot(final LocalDateTime beginning, final LocalDateTime ending) {
        if (beginning == null || ending == null) {
            return new ArrayList<>();
        }

        final NodeList snapshotNodeList = new NodeList();

        // Search in-memory nodes for any nodes created within specified time-range:
        map.forEach((key, node) -> {
            if (node.isWithinTimeRange(beginning, ending)) {
                // The user shouldn't be able to alter in-memory nodes from the returned nodes, so we use a copy of the node.
                snapshotNodeList.add(node.copy());
            }
        });

        // Search on-disk files for any nodes created within the specified time-range:
        snapshotNodeList.addAll(new FileSearcher().rangeSearchFile(beginning, ending));
        snapshotNodeList.removeDuplicateNodes();

        return snapshotNodeList;
    }

    /** @return The total number of nodes in the tree. */
    public int getTotalNodes() {
        return map.size();
    }
}
