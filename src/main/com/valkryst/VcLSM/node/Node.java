package main.com.valkryst.VcLSM.node;

import lombok.Getter;
import main.com.valkryst.VcLSM.C;

import java.time.LocalDateTime;

public class Node <V> implements Comparable<Node<V>> {
    /** The key. */
    @Getter private final String key;
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
    public Node(final NodeBuilder<V> builder) {
        key = builder.getKey();
        time = LocalDateTime.now();
        value = builder.getValue();
    }

    @Override
    public int compareTo(final Node<V> other) {
        return this.time.compareTo(other.time);
    }

    /** @return The key concatenated with the timestamp. */
    public String getKeyWithTimestamp(){
        return key + C.DILIMETER + time;
    }
}
