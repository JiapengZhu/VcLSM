package main.com.valkryst.VcLSM.node;

import java.util.ArrayList;
import java.util.ListIterator;

public class NodeList extends ArrayList<Node> {
    /**
     * Removes all duplicate nodes from the list.
     *
     * If two nodes, using the same key, are found, then the oldest node is removed.
     */
    public void removeDuplicateNodes() {
        // Delete duplicated nodes:
        final ListIterator<Node> it = this.listIterator();

        while (it.hasNext()) {
            final Node outerNode = it.next();

            while (it.hasNext()) {
                final Node innerNode = it.next();
                boolean keysEqual = outerNode.getKey().equals(innerNode.getKey());

                if (keysEqual) {
                    boolean outerIsOlder = outerNode.getTime().isBefore(innerNode.getTime());

                    if (outerIsOlder) {
                        it.remove();
                    } else {
                        it.remove();
                    }
                }
            }
        }
    }
}
