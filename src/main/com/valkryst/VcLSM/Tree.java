package main.com.valkryst.VcLSM;

import main.com.valkryst.VcLSM.node.Node;
import main.com.valkryst.VcLSM.node.NodeInstrumentation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

public class Tree <K, V> {
    /** The maximum size of the tree, in kilobytes, before a Merge must occur. */
    private final int maximumSize;
    /** The current size of the tree, in kilobytes. */
    private int currentSize = 0;
    /** The underlying data structure of the tree. */
    private final ConcurrentSkipListMap<K, Node<K, V>> map = new ConcurrentSkipListMap<>();

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
    public Optional<Node<K, V>> get(final K key) {
        return Optional.ofNullable(map.get(key));
    }

    /**
     * Puts the specified node into the tree.
     *
     * @param node
     *         The node.
     */
    public void put(final Node<K, V> node) {
        final long estimatedNodeSize = NodeInstrumentation.getNodeSize(node);

        // If the tree will exceed it's maximum size by adding the new node,
        // then perform a merge before adding the new node.
        if (currentSize + estimatedNodeSize >= maximumSize) {
            merge();
        }

        map.put(node.getKey(), node);
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
    public Optional<Node<K, V>> search(final K key) {
        // todo Use the get function to search the map to see if a node with the specified key exists within.

        // todo If a node with the specified key doesn't exist in the in-memory map, then search the on-disk maps.

        // todo Return an Optional
        return Optional.empty(); // REPLACE THIS LINE WHEN CODE IS WRITTEN.
    }

    public void merge() {
        // todo Implement merge.
        // todo Maybe we should lock the map, so that nothing can alter it while the merge is taking place.

        // Merge all of the on-disk files:
        final FileMerger fileMerger = new FileMerger();
        fileMerger.merge(maximumSize);

        // Clear the in-memory structure:
        map.clear();
    }

    public void snapshot(final LocalDateTime beginning, final LocalDateTime ending) {
        // todo Implement snapshot.
    }
}
