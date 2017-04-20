package main.com.valkryst.VcLSM.node;

import lombok.Getter;
import main.com.valkryst.VcLSM.C;

import java.time.LocalDateTime;

public class Node implements Comparable<Node> {
    /** The key. */
    @Getter private final String key;
    /** The date & time at which the node was created. */
    @Getter private final LocalDateTime time;
    /** The value. */
    @Getter private final String value;

    /**
     * Constructs a new Node.
     *
     * @param builder
     *         The builder to use.
     */
    public Node(final NodeBuilder builder) {
        key = builder.getKey();
        time = builder.getTime();
        value = builder.getValue();
    }

    /**
     * Constructs a new Node as a copy of an existing Node.
     *
     * @param node
     *         The node whose data is to be copied.
     */
    private Node(final Node node) {
        this.key = node.getKey();
        this.time = LocalDateTime.parse(node.getTime().toString());
        this.value = node.getValue();
    }

    @Override
    public int compareTo(final Node other) {
        return this.time.compareTo(other.time);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null || ! (obj instanceof Node)) {
            return false;
        }

        final Node otherNode = (Node) obj;

        boolean isEqual = key.equals(otherNode.getKey());
        isEqual &= time.equals(otherNode.getTime());
        isEqual &= value.equals(otherNode.getValue());

        return isEqual;
    }

    /** @return A copy of the Node. */
    public Node copy() {
        return new Node(this);
    }

    /** @return The key concatenated with the timestamp. */
    public String getKeyWithTimestamp(){
        return key + C.DILIMETER + time;
    }

    /**
     * Determines if the node's time is equal to, or within, the specified time range.
     *
     * @param beginning
     *         The beginning time.
     *
     * @param ending
     *         The ending time.
     *
     * @return
     *         Whether or not the node's time is equal to, or within, the specified time range.
     */
    public boolean isWithinTimeRange(final LocalDateTime beginning, final LocalDateTime ending) {
        return isWithinTimeRange(beginning, ending, time);
    }

    /**
     * Determines if the time is equal to, or within, the specified time range.
     *
     * @param beginning
     *         The beginning time.
     *
     * @param ending
     *         The ending time.
     *
     * @param time
     *         The time to check.
     *
     * @return
     *         Whether or not the time is equal to, or within, the specified time range.
     */
    public static boolean isWithinTimeRange(final LocalDateTime beginning, final LocalDateTime ending, final LocalDateTime time) {
        if (beginning == null || ending == null) {
            return false;
        }

        boolean isBeginningOrAfter = time.isAfter(beginning);
        isBeginningOrAfter |= time.isEqual(beginning);

        boolean isEndingOrBefore = time.isBefore(ending);
        isEndingOrBefore |= time.isEqual(ending);

        return isBeginningOrAfter & isEndingOrBefore;
    }
}
