package main.com.valkryst.VcLSM;

import main.com.valkryst.VcLSM.node.Node;
import main.com.valkryst.VcLSM.node.NodeInstrumentation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Tree <V> {
    /** The maximum size of the tree, in kilobytes, before a Merge must occur. */
    private final int maximumSize;
    /** The current size of the tree, in kilobytes. */
    private int currentSize = 0;
    /** The underlying data structure of the tree. */
    private final ConcurrentSkipListMap<String, Node<V>> map = new ConcurrentSkipListMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    private final FileSearcher<V> fileSearcher = new FileSearcher<>();
    private List<Node> snapshotNodeList = new ArrayList<Node>();


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

            this.maximumSize = 1;
        } else {
            this.maximumSize = maximumSize;
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
    public Optional<Node<V>> get(final String key) {
        if (key == null || key.isEmpty()) {
            return Optional.empty();
        }

        return Optional.ofNullable(map.get(key));
    }

    /**
     * Puts the specified node into the tree.
     *
     * If a null node is specified, then nothing happens.
     *
     * @param node
     *         The node.
     */
    public void put(final Node<V> node) {
        if (node == null) {
            return;
        }

        readLock.lock();
        final long estimatedNodeSize = NodeInstrumentation.getNodeSize(node);

        // If the tree will exceed it's maximum size by adding the new node,
        // then perform a merge before adding the new node.
        if (currentSize + estimatedNodeSize >= maximumSize) {
            merge();
        }

        map.put(node.getKeyWithTimestamp(), node);
        currentSize += estimatedNodeSize;
        readLock.unlock();
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
    public Optional<Node<V>> search(final String key) {
        // Search the in-memory map:
        Optional<Node<V>> result = get(key);

        if (result.isPresent()) {
            return result;
        }

        // Search each of the on-disk files
        result = fileSearcher.search(key);

        return result;
    }

    public void merge() {
        // Before Merge:
        writeLock.lock();

        // Create a new in-memory map, this is immutable and will be merged onto disk.
        // The old in-memory map will is mutable and will be reused.
        final ConcurrentSkipListMap<String, Node<V>> newMap = new ConcurrentSkipListMap<> ();
        newMap.putAll(map);
        map.clear();
        writeLock.unlock();

        // Merge:
        try {
            writeLock.lock();
            final FileMerger<V> fileMerger = new FileMerger<>();
            final String fileName = System.currentTimeMillis() + ".json";

            // Merge in-memory data into disk
            fileMerger.mergeToDisk(newMap, fileName);

            // Merge all of the on-disk files:
            fileMerger.merge(maximumSize);
        } catch (final IOException e) {
            final Logger logger = LogManager.getLogger();
            logger.error(e.getMessage());
        } finally {
            writeLock.unlock();
        }
    }

    public void snapshot(final LocalDateTime beginning, final LocalDateTime ending) {
        readLock.lock();

        // Search nodes within specified time range from memory component
        for (final Map.Entry<String, Node<V>> entry : map.entrySet()) {
            final Node<V> node = entry.getValue();
            final LocalDateTime nodeTimestamp = node.getTime();

            if (nodeTimestamp.isAfter(beginning) && nodeTimestamp.isBefore(ending)) {
                snapshotNodeList.add(node);
            }
        }

        // Search nodes within specified time range from disks
        final List<Node> nodeList = fileSearcher.rangeSearchFile(beginning, ending);

        if (nodeList.size() > 0) {
            snapshotNodeList.addAll(nodeList);
        }

        readLock.unlock();

        // delete the duplicated nodes
        writeLock.lock();
        refineSnapshotNode();
        writeLock.unlock();
    }

    // Once the search is done, delete the duplicated nodes to keep latest node version
    private void refineSnapshotNode(){
        final Set<String> detectionSet = new HashSet<>();
        final ArrayList<Node> oldNodeList = new ArrayList<>();
        int counter = 0;

        for (final Node node : snapshotNodeList) {
            if (!detectionSet.add(node.getKey())) {
                final Node oldNode = oldNodeList.get(counter - 1);
                final LocalDateTime nodeTimestamp = node.getTime();
                final LocalDateTime oldNodeTimestamp = oldNode.getTime();

                if (oldNodeTimestamp.isBefore(nodeTimestamp)) {
                    snapshotNodeList.remove(oldNode);
                } else {
                    snapshotNodeList.remove(node);
                }
            } else {
                oldNodeList.add(node);
            }

            counter++;
        }
    }
}
