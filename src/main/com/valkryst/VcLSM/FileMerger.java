package main.com.valkryst.VcLSM;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import main.com.valkryst.VcLSM.node.Node;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class FileMerger<V> {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Logger logger = LogManager.getLogger();

    /** Constructs a new FileMerger. */
    public FileMerger() {
        // If the data directory doesn't exist, attempt to create it:
        final File dataDirectory = new File("data/");

        if (! dataDirectory.exists()) {
            if (dataDirectory.mkdir()) {
                logger.error("Unable to create data directory.");
                // todo Maybe exit the program?
            }
        }
    }

    /**
     * Find files to be merged from disk, and then merge them
     *
     * @param maximumTreeSize
     *         The maximum size of the tree, in kilobytes, before a Merge must occur.
     *
     */
    public void merge(final int maximumTreeSize) {
        long minLength = 0;
        long maxLength = maximumTreeSize;
        long totalFiles = getFilesInSizeRange(0, Long.MAX_VALUE).size();

        while (maxLength > 0 && maxLength < Long.MAX_VALUE && totalFiles > 0) {
            final List<File> files = getFilesInSizeRange(minLength, maxLength);
            final ListIterator<File> iterator = files.listIterator();

            totalFiles -= files.size();

            // Save computation time by only merging when there are at-least 4 files
            // within the list.
            // The value 4 is arbitrary.
            if (files.size() < 4) {
                minLength += maximumTreeSize;
                maxLength += maximumTreeSize;
                continue;
            }

            // For each file in the list, merge the previous and current files.
            while (iterator.hasNext()) {
                final File previous = iterator.previous();
                final File current = iterator.next();

                if (previous != null && current != null) {
                    mergeFiles(previous, current);
                    iterator.remove();
                }
            }

            minLength += maximumTreeSize;
            maxLength += maximumTreeSize;
        }
    }

    /**
     * Merges the data stored in memory to disk by converting map structure to JSON file.
     *
     * @param map
     *         The underlying data store structure.
     *
     * @param fileName
     *         The output file name.
     */
    public void mergeToDisk(ConcurrentSkipListMap<String, Node<V>> map, String fileName) throws IOException{
        mapper.writeValue(new File("data/"+fileName), map);
    }

    /**
     * Merges the specified files into a single file.
     *
     * @param fileA
     *         The first file.
     *
     * @param fileB
     *         The second file.
     */
    private void mergeFiles(final File fileA, final File fileB) {
        final Set<String> s = new HashSet<>();
        final JsonNode rootNodeA, rootNodeB;
        final ArrayNode objArrNodeA = mapper.createArrayNode();
        final ArrayNode keyArrNodeA = mapper.createArrayNode();

        try {
            rootNodeA = mapper.readTree(fileA);
            rootNodeB = mapper.readTree(fileB);

            final ObjectNode aObjNodes = rootNodeA.deepCopy(); // Convert JsonNode to ObjectNode
            final Iterator<Map.Entry<String, JsonNode>> aFields = rootNodeA.fields();
            final Iterator<Map.Entry<String, JsonNode>> bFields = rootNodeB.fields();

            while (aFields.hasNext()) {
                final Map.Entry<String, JsonNode> entry = aFields.next();
                objArrNodeA.add(entry.getValue()); // store Node<K, V>
                keyArrNodeA.add(entry.getKey()); // store key with timestamp
            }

            while (bFields.hasNext()) {
                final Map.Entry<String, JsonNode> entry = bFields.next();
                final JsonNode bJsonNodeVal = entry.getValue();

                // check duplicated data between two files
                int i = objArrNodeA.findValuesAsText(C.K).indexOf(bJsonNodeVal.path(C.K).asText());

                if (i > 0) {
                    // if exists, check timestamps,
                    // if to-be merged data is later than merging data, update the merging data
                    final String bTime = bJsonNodeVal.path(C.TIME).asText();
                    final String aTime = objArrNodeA.get(i).path(C.TIME).asText();

                    if (isDateTimeBefore(aTime, bTime)) {
                        final String aNewKey = objArrNodeA.get(i).path(C.K).asText() + "+" + bTime;
                        aObjNodes.remove(keyArrNodeA.get(i).asText());
                        keyArrNodeA.remove(i);
                        aObjNodes.set(aNewKey, bJsonNodeVal);
                        keyArrNodeA.add(aNewKey);
                    }
                } else {
                    // otherwise, insert merged data into merging data directly
                    final String bKeyWithTimestamp = bJsonNodeVal.path(C.K).asText() + C.DILIMETER + bJsonNodeVal.path(C.TIME).asText();
                    keyArrNodeA.add(bKeyWithTimestamp);
                    aObjNodes.set(bKeyWithTimestamp, bJsonNodeVal);
                }
            }

            // Merge duplicated data in merged file
            int k = 0; // trace the index
            final Iterator<Map.Entry<String, JsonNode>> aObjNodeFields = aObjNodes.fields();
            final ArrayNode keyWithTimeArr = mapper.createArrayNode(); // trace the old keys with timestamp

            while(aObjNodeFields.hasNext()){
                final Map.Entry<String, JsonNode> entry = aObjNodeFields.next();
                final JsonNode entryValueNode = entry.getValue();
                final String entryKeyNode = entry.getKey();

                if (!s.add(entryValueNode.path(C.K).asText())) {
                    final String oldKeyWithTime = keyWithTimeArr.get(k-1).asText();
                    final JsonNode oldEntryValueNode = aObjNodes.get(oldKeyWithTime);

                    if (isDateTimeBefore(oldEntryValueNode.path(C.TIME).asText(), entryValueNode.path(C.TIME).asText())) {
                        aObjNodes.remove(oldKeyWithTime);
                    } else {
                        aObjNodeFields.remove();
                    }
                } else {
                    keyWithTimeArr.add(entryKeyNode);
                }

                k++;
            }

            // overwrite file A by merged data
            mapper.writeValue(fileA, aObjNodes);

            // Delete file B
            if (!fileB.delete()) {
                logger.error("Fail to delete " + fileB.getName());
                System.exit(1);
            }

           // System.out.println(aObjNodes);

        } catch (final IOException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * Determines and returns all files within the data directory whose file-sizes are within the specified range.
     *
     * @param minLength
     *         The minimum file-size, in kilobytes.
     *
     * @param maxLength
     *         The maximum file-size, in kilobytes.
     *
     * @return
     *         A list of all files within the data directory whose file-sizes are within the specified range.
     */
    private List<File> getFilesInSizeRange(final long minLength, final long maxLength) {
        // Retrieve all files in the data folder that end with the ".dat" extension:
        final File[] allFiles = new File("data/").listFiles(pathname -> {
            boolean accept = pathname.getName().toLowerCase().endsWith(".dat");
            accept &= pathname.isFile();

            return accept;
        });

        final List<File> validFiles = new ArrayList<>();

        if (allFiles == null) {
            return validFiles;
        }

        // For each file, if the file-size is within the specified range, then
        // add it to the list of valid files.
        for (final File file : allFiles) {
            final long fileLength = file.length() / 1000;

            boolean isValid = fileLength >= minLength;
            isValid &= fileLength < maxLength;

            if (isValid) {
                validFiles.add(file);
            }
        }


        return validFiles;
    }

    private static boolean isDateTimeBefore(final String timeA, final String timeB){
        final LocalDateTime aLocalTime = LocalDateTime.parse(timeA, C.FORMATTER);
        final LocalDateTime bLocalTime = LocalDateTime.parse(timeB, C.FORMATTER);
        return aLocalTime.isBefore(bLocalTime);
    }

}
