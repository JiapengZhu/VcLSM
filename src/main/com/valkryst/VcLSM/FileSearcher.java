package main.com.valkryst.VcLSM;

import main.com.valkryst.VcLSM.node.Node;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;

public class FileSearcher <K, V>{
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
    public Optional<Node<K, V>> search(final K key) {
        for (final File file : getSortedFiles()) {
            final Optional<Node<K, V>> opt = searchFile(key, file);

            // Return the first occurrence of a node using the specified key.
            if (opt.isPresent()) {
                return opt;
            }
        }

        return Optional.empty();
    }

    /** @return An array of all .dat files within the data folder, sorted from most to least recently created. */
    private File[] getSortedFiles() {
        // Retrieve all files in the data folder that end with the ".dat" extension:
        final File[] files = new File("/data/").listFiles(pathname -> {
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
    private Optional<Node<K, V>> searchFile(final K key, final File file) {
        try (
            final InputStream is = new FileInputStream(file);
        ) {
            // todo Read the contents of the JSON file. If a node is found, then
            // todo construct the Node object and return Optional.ofNullable(node).
            // todo Else return Optional.empty().
        } catch (final IOException e) {
            final Logger logger = LogManager.getLogger();
            logger.error(e.getMessage());
        }

        return Optional.empty();
    }
}
