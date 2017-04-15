package main.com.valkryst.VcLSM;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import main.com.valkryst.VcLSM.node.Node;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;

public class FileSearcher <V> {
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
    public Optional<Node<V>> search(final String key) {
        for (final File file : getSortedFiles()) {
            final Optional<Node<V>> opt = searchFile(key, file);

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
    private Optional<Node<V>> searchFile(final String key, final File file) {
        try {
            // Read the contents of the JSON file.
            final JsonNode rootNode = mapper.readTree(file);
            final Iterator<Map.Entry<String, JsonNode>> fields = rootNode.fields();

            while(fields.hasNext()){
                final Map.Entry<String, JsonNode> entry = fields.next();

                if(entry.getValue().get("key").asText().equals(key)) {
                    final JsonNode jsonNode = entry.getValue();
                    final LocalDateTime time = stringToLocalDateTime(jsonNode.path(C.TIME).asText());

                    // Construct and return the node:
                    final Node<V> node = new Node(key, jsonNode.path(C.V).asText(), time);
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
     * @param start
     *         The starting time
     * @param end
     *         The ending time
     * @param file
     *         The file to be searched
     *
     */
    private void searchFileByTimestamp(final LocalDateTime start, final LocalDateTime end, final File file){
        try{
            final JsonNode rootNode = mapper.readTree(file);
            final Iterator<Map.Entry<String, JsonNode>> fields = rootNode.fields();

            while(fields.hasNext()){
                final Map.Entry<String, JsonNode> entry = fields.next();
                final JsonNode nodeVal = entry.getValue();
                final String nodeTimeStampStr = nodeVal.path(C.TIME).asText();
                final LocalDateTime nodeTimeStamp = stringToLocalDateTime(nodeTimeStampStr);

                if (nodeTimeStamp.isAfter(start) && nodeTimeStamp.isBefore(end)){
                    final Node<V> nodeObj = new Node(nodeVal.path(C.K).asText(), nodeVal.path(C.V).asText(), nodeTimeStamp);
                    nodeList.add(nodeObj);
                }
            }
        } catch(final IOException e) {
            C.logger.error(e.getMessage());
        }
    }

    /**
     * Convert a formated datetime string to LocalDateTime type
     *
     * @param str
     *         The plaint datatime string
     *
     * @return
     *         The localDateTime type datetime
     */
    private LocalDateTime stringToLocalDateTime(String str){
        return LocalDateTime.parse(str, C.FORMATTER);
    }
}
