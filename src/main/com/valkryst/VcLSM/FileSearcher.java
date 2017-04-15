package main.com.valkryst.VcLSM;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import main.com.valkryst.VcLSM.node.Node;
import main.com.valkryst.VcLSM.node.NodeBuilder;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;

public class FileSearcher {
    private final ObjectMapper mapper = new ObjectMapper();
    private List<Node> nodeList = new ArrayList<>();

    /**
     * Searches through all .dat files within the data directory for the first occurrence of a node using the specified
     * key.
     *
     * @param key
     *         The key to look for.
     *
     * @return
     *         The node, or nothing if no node is found.
     */
    public Optional<Node> search(final String key) {
        for (final File file : getSortedFiles()) {
            final Optional<Node> opt = searchFile(key, file);

            // Return the first occurrence of a node using the specified key.
            if (opt.isPresent()) {
                return opt;
            }
        }

        return Optional.empty();
    }

    /**
     * Searches through all .dat files within the data directory for a specified time range from user
     * key.
     *
     * @param beginning
     *         The starting time
     * @param ending
     *         The ending time
     *
     * @return
     *         a list contains found nodes
     */
    public List<Node> rangeSearchFile(final LocalDateTime beginning, final LocalDateTime ending){
        for (final File file : getSortedFiles()) {
            searchFileByTimestamp(beginning, ending, file);
        }
        return nodeList;
    }

    /** @return An array of all .dat files within the data folder, sorted from most to least recently created. */
    private File[] getSortedFiles() {
        // Retrieve all files in the data folder that end with the ".dat" extension:
        final File[] files = new File("data/").listFiles(pathname -> {
            boolean accept = pathname.getName().toLowerCase().endsWith(".dat");
            accept &= pathname.isFile();

            return accept;
        });

        if (files == null) {
            return new File[0];
        }

        Arrays.sort(files, Comparator.comparingLong(File::lastModified));
        return files;
    }

    /**
     * Searches the specified JSON file for a node using the specified key.
     *
     * @param key
     *         The key to look for.
     *
     * @param file
     *         The JSON file to search in.
     *
     * @return
     *         The node, or nothing if no node is found.
     */
    private Optional<Node> searchFile(final String key, final File file) {
        try {
            // Read the contents of the JSON file.
            final JsonNode rootNode = mapper.readTree(file);
            final Iterator<Map.Entry<String, JsonNode>> fields = rootNode.fields();

            while(fields.hasNext()){
                final Map.Entry<String, JsonNode> entry = fields.next();

                if(entry.getValue().get("key").asText().equals(key)) {
                    final JsonNode jsonNode = entry.getValue();

                    // Construct and return the node:
                    final Node node = new NodeBuilder().loadFromJSON(jsonNode).build();
                    return Optional.of(node);
                }
            }
        } catch (final IOException e) {
            C.logger.error(e.getMessage());
        }

        return Optional.empty();
    }

    /**
     * Searches a file in data directory for a specified time range from user
     * key.
     *
     * @param beginning
     *         The beginning time.
     *
     * @param ending
     *         The ending time.
     *
     * @param file
     *         The file to be searched.
     *
     */
    private void searchFileByTimestamp(final LocalDateTime beginning, final LocalDateTime ending, final File file){
        try{
            final JsonNode rootNode = mapper.readTree(file);
            final Iterator<Map.Entry<String, JsonNode>> fields = rootNode.fields();

            while(fields.hasNext()){
                final Map.Entry<String, JsonNode> entry = fields.next();
                final JsonNode nodeVal = entry.getValue();

                // Determine if Node is within specified time-range:
                final LocalDateTime nodeTimestamp = LocalDateTime.parse(nodeVal.path("time").asText(), C.FORMATTER);

                boolean isBeginningOrAfter = nodeTimestamp.isAfter(beginning);
                isBeginningOrAfter |= nodeTimestamp.isEqual(beginning);

                boolean isEndingOrBefore = nodeTimestamp.isBefore(ending);
                isEndingOrBefore |= nodeTimestamp.isEqual(ending);

                // Construct and add the Node if it's within the time-range:
                if (isBeginningOrAfter && isEndingOrBefore) {
                    final Node nodeObj = new NodeBuilder().loadFromJSON(nodeVal).build();
                    nodeList.add(nodeObj);
                }
            }
        } catch(final IOException e) {
            C.logger.error(e.getMessage());
        }
    }
}
