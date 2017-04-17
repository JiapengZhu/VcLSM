package main.com.valkryst.VcLSM.node;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;
import main.com.valkryst.VcLSM.C;
import main.com.valkryst.VcLSM.FileSearcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

public class NodeBuilder {
    /** The key. */
    @Getter private String key = null;
    /** The date & time at which the node was created. */
    @Getter private LocalDateTime time = null;
    /** The value. */
    @Getter private String value = null;

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
        return new Node(this);
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

        if (value.isEmpty()) {
                throw new IllegalStateException("A Node cannot have an empty value.");
        }
    }

    /**
     * Attempts to load a node from the key, time, and value within the specified JsonNode.
     *
     * @param jsonNode
     *         The JsonNode to load from.
     */
    public NodeBuilder loadFromJSON(final JsonNode jsonNode) {
        try {
            this.key = jsonNode.path(C.K).asText();
            this.time = FileSearcher.formatTimeString(jsonNode.path(C.TIME).asText());
            this.value = jsonNode.path(C.V).asText();
        } catch (final DateTimeParseException e) {
            final Logger logger = LogManager.getLogger();
            logger.error(e.getMessage());

            this.key = null;
            this.time = null;
            this.value = null;
        }

        return this;
    }

    /** Sets the key, time, and value to null. */
    public void reset() {
        this.key = null;
        this.time = null;
        this.value = null;
    }

    public NodeBuilder setKey(final String key) {
        this.key = key;
        return this;
    }

    public NodeBuilder setTime(final LocalDateTime time) {
        this.time = time;
        return this;
    }

    public NodeBuilder setValue(final String value) {
        this.value = value;
        return this;
    }
}
