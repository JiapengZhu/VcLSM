package main.com.valkryst.VcLSM.node;

import java.time.LocalDateTime;
import java.util.ArrayList;

public class NodeList extends ArrayList<Node> {
    /**
     * Removes all duplicate nodes from the list.
     *
     * If two nodes, using the same key, are found, then the oldest node is removed.
     *
     * If two nodes, using the same key and time, are found, then the first node is removed.
     */
    public void removeDuplicateNodes() {
        for (int a = 0 ; a < this.size() ; a++) {
            final Node outerNode = this.get(a);
            final String outerKey = outerNode.getKey();
            final LocalDateTime outerTime = outerNode.getTime();

            for (int b = a + 1 ; b < this.size() ; b++) {
                final Node innerNode = this.get(b);
                final String innerKey = innerNode.getKey();
                final LocalDateTime innerTime = innerNode.getTime();


                final boolean keysEqual = outerKey.equals(innerKey);

                if (keysEqual) {
                    final boolean outerIsOlder = outerTime.isBefore(innerTime);
                    final boolean nodesAreEquallyAsOld = outerTime.equals(innerTime);

                    if (outerIsOlder || nodesAreEquallyAsOld) {
                        this.remove(a);

                        if (a > 0) {
                            a--;
                        }

                        if (b > 0) {
                            b--;
                        }
                    } else {
                        this.remove(b);

                        if (b > 0) {
                            b--;
                        }
                    }
                }
            }
        }
    }
}
