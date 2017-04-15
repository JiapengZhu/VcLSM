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
    /** The maximum size of the tree, in bytes, before a Merge must occur. */
    private final int maximumSize;
    /** The current size of the tree, in bytes. */
    private int currentSize = 0;
    /** The underlying data structure of the tree. */
    private final ConcurrentSkipListMap<String, Node<V>> map = new ConcurrentSkipListMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    private final FileSearcher<V> fileSearcher = new FileSearcher<>();


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
    public Optional<Node<V>> get(final String key) {
        if (key == null || key.isEmpty()) {
            return Optional.empty();
        }

        for (final Map.Entry<String, Node<V>> entry : map.entrySet()) {
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
    public void put(final Node<V> node) {
        if (node == null) {
            return;
        }

        final long estimatedNodeSize = NodeInstrumentation.getNodeSize(node);

        // If the tree will exceed it's maximum size by adding the new node,
        // then perform a merge before adding the new node.
        if (currentSize + estimatedNodeSize >= maximumSize) {
            merge();
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
    public Optional<Node<V>> search(final String key) {
        if (key == null || key.isEmpty()) {
            return Optional.empty();
        }

        // Search the in-memory map:
        Optional<Node<V>> tmp = get(key);

        if (tmp.isPresent()) {
            return tmp;
        }

        // Search each of the on-disk files
        return fileSearcher.search(key);
    }

    private void merge() {
        // Before Merge:
        writeLock.lock();

        // Create a new in-memory map, this is immutable and will be merged onto disk.
        // The old in-memory map will is mutable and will be reused.
        final ConcurrentSkipListMap<String, Node<V>> newMap = new ConcurrentSkipListMap<> ();
        newMap.putAll(map);
        map.clear();
        currentSize = 0;
        writeLock.unlock();

        // Merge:
        try {
            writeLock.lock();
            final FileMerger<V> fileMerger = new FileMerger<>();
            final String fileName = System.currentTimeMillis() + ".dat";

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

    public List<Node> snapshot(final LocalDateTime beginning, final LocalDateTime ending) {
        if (beginning == null || ending == null) {
            return new ArrayList<>();
        }

        readLock.lock();

        final List<Node> snapshotNodeList = new ArrayList<>();

        // Search in-memory nodes for any nodes created within specified time-range:
        map.forEach((key, node) -> {
            final LocalDateTime nodeTimestamp = node.getTime();

            if (nodeTimestamp.isAfter(beginning) && nodeTimestamp.isBefore(ending)) {
                snapshotNodeList.add(node);
            }
        });

        // Search on-disk files for any nodes created within the specified time-range:
        snapshotNodeList.addAll(fileSearcher.rangeSearchFile(beginning, ending));

        readLock.unlock();

        // Delete duplicated nodes:
        final ListIterator<Node> iteratorOuter = snapshotNodeList.listIterator();

        while (iteratorOuter.hasNext()) {
            final Node outerNode = iteratorOuter.next();
            final ListIterator<Node> iteratorInner = snapshotNodeList.listIterator();

            while (iteratorInner.hasNext()) {
                final Node innerNode = iteratorInner.next();
                boolean keysEqual = outerNode.getKey().equals(innerNode.getKey());

                if (keysEqual) {
                    boolean outerIsOlder = outerNode.getTime().isBefore(innerNode.getTime());

                    if (outerIsOlder) {
                        iteratorOuter.remove();
                    } else {
                        iteratorInner.remove();
                    }
                }
            }
        }

        return snapshotNodeList;
    }

    /** @return The total number of nodes in the tree. */
    public int getTotalNodes() {
        return map.size();
    }
}
