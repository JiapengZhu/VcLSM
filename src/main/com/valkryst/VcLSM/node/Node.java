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
        time = builder.getTime();
        value = builder.getValue();
    }

    // Construct a customized constructor by specifying a timestamp
    public Node(final String key, final V value, final LocalDateTime time){
        this.key = key;
        this.value = value;
        this.time = time;
    }

    @Override
    public int compareTo(final Node<V> other) {
        return this.time.compareTo(other.time);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null || ! (obj instanceof Node)) {
            return false;
        }

        final Node<V> otherNode = (Node<V>) obj;

        boolean isEqual = key.equals(otherNode.getKey());
        isEqual &= time.equals(otherNode.getTime());
        isEqual &= value.equals(otherNode.getValue());

        return isEqual;
    }

    /** @return The key concatenated with the timestamp. */
    public String getKeyWithTimestamp(){
        return key + C.DILIMETER + time;
    }
}
