package main.com.valkryst.VcLSM;

import main.com.valkryst.VcLSM.node.Node;
import main.com.valkryst.VcLSM.node.NodeInstrumentation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Tree <K, V> {
    /** The maximum size of the tree, in kilobytes, before a Merge must occur. */
    private final int maximumSize;
    /** The current size of the tree, in kilobytes. */
    private int currentSize = 0;
    /** The underlying data structure of the tree. */
    private final ConcurrentSkipListMap<K, Node<K, V>> map = new ConcurrentSkipListMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();


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
        readLock.lock();
        final long estimatedNodeSize = NodeInstrumentation.getNodeSize(node);

        // If the tree will exceed it's maximum size by adding the new node,
        // then perform a merge before adding the new node.
        if (currentSize + estimatedNodeSize >= maximumSize) {
            merge();
        }

        String key = node.getKeyWithTimestamp();
        map.put((K)key, node);
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
    public Optional<Node<K, V>> search(final K key) {
        // Search the in-memory map:
        Optional<Node<K, V>> result = get(key);

        if (result.isPresent()) {
            return result;
        }

        // Search each of the on-disk files
        final FileSearcher<K, V> fileSearcher = new FileSearcher<>();
        result = fileSearcher.search(key);

        return result;
    }

    public void merge() {
        // todo Implement merge.
        // todo Maybe we should lock the map, so that nothing can alter it while the merge is taking place.
        // Before the merge
        writeLock.lock();
        // create a new in-memory component, new compinent will be immutable and merged into disk
        // old in-memory component will be mutable and reused
        ConcurrentSkipListMap<K, Node<K, V>> newMap = map;
        map.clear();
        writeLock.unlock();

        // Doing Merge...

        try {
            writeLock.lock();
            final FileMerger fileMerger = new FileMerger();
            String fileName = System.currentTimeMillis() + ".json";
            // Merge in-memory data into disk
            fileMerger.mergeToDisk(newMap, fileName);
            // Merge all of the on-disk files:
            fileMerger.merge(maximumSize);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            writeLock.unlock();
        }

        // After merge
        writeLock.lock();
        // Clear the in-memory structure:
        newMap.clear();
        //map.clear();
        writeLock.unlock();
    }

    public void snapshot(final LocalDateTime beginning, final LocalDateTime ending) {
        // todo Implement snapshot.
    }
}
