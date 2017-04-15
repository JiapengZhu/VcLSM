package main.com.valkryst.VcLSM.node;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;
import main.com.valkryst.VcLSM.C;

import java.time.LocalDateTime;

public class NodeBuilder <V> {
    /** The key. */
    @Getter private String key = null;
    /** The date & time at which the node was created. */
    @Getter private LocalDateTime time = LocalDateTime.now();
    /** The value. */
    @Getter private V value = null;

    /**
     * Uses the builder to construct a new Node.
     *
     * @return
     *          The new Node.
     *
     * @throws IllegalStateException
     *          If something is wrong with the builder's state.
     */
    public Node<V> build() throws IllegalStateException {
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

        if (time == null) {
            time = LocalDateTime.now();
        }

        if (value == null) {
            throw new IllegalStateException("A Node cannot have a null value.");
        }

        if (value instanceof String) {
            if (((String) value).isEmpty()) {
                throw new IllegalStateException("A Node cannot have an empty value.");
            }
        }
    }

    public void loadFromJSON(final JsonNode object) {
        final String keyTmp = object.get("key").asText();
        final String timeTmp = object.get("time").asText();
        final String valueTmp = object.get("value").asText();

        this.key = keyTmp;
        this.time = LocalDateTime.parse(timeTmp, C.FORMATTER);
        // todo Finish
    }

    public NodeBuilder<V> setKey(final String key) {
        this.key = key;
        return this;
    }

    public NodeBuilder<V> setTime(final LocalDateTime time) {
        this.time = time;
        return this;
    }

    public NodeBuilder<V> setValue(final V value) {
        this.value = value;
        return this;
    }
}
