package main.com.valkryst.VcLSM;

import lombok.Getter;

import java.time.LocalDateTime;

public class Node <K, V> {
    /** The key. */
    @Getter private final K key;
    /** The date & time at which the node was created. */
    @Getter private final LocalDateTime time;
    /** The value. */
    @Getter private final V value;

    /**
     * Constructs a new Node.
     *
     * @param builder
     *         The builder to use.
     */
    public Node(final NodeBuilder<K, V> builder) {
        key = builder.getKey();
        time = LocalDateTime.now();
        value = builder.getValue();
    }
}
