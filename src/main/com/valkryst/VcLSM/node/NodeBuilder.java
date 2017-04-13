package main.com.valkryst.VcLSM.node;

import lombok.Getter;
import lombok.Setter;

public class NodeBuilder <V> {
    /** The key. */
    @Getter @Setter private String key = null;
    /** The value. */
    @Getter @Setter private V value = null;

    /**
     * Uses the builder to construct a new Node.
     *
     * @return
     *          The new Node.
     *
     * @throws IllegalStateException
     *          If something is wrong with the builder's state.
     */
    public Node build() throws IllegalStateException {
        checkState();
        return new Node<>(this);
    }

    /**
     * Checks the current state of the builder.
     *
     * @throws IllegalStateException
     *          If something is wrong with the builder's state.
     */
    private void checkState() throws IllegalStateException {
        if (key == null) {
            throw new IllegalStateException("A Node cannot have a null key.");
        }

        if (key.isEmpty()) {
            throw new IllegalStateException("A Node cannot have an empty key.");
        }

        if (value == null) {
            throw new IllegalStateException("A Node cannot have a null value.");
        }
    }
}
