package main.com.valkryst.VcLSM;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.*;

public class FileMerger {
    /** Constructs a new FileMerger. */
    public FileMerger() {
        // If the data directory doesn't exist, attempt to create it:
        final File dataDirectory = new File("/data/");

        if (! dataDirectory.exists()) {
            if (dataDirectory.mkdir()) {
                final Logger logger = LogManager.getLogger();
                logger.error("Unable to create data directory.");
                System.exit(1); // todo Maybe attempt to create the directory a different way before exiting.
            }
        }
    }

    // todo JavaDoc
    public void merge(final int maximumTreeSize) {
        long minLength = 0;
        long maxLength = maximumTreeSize;


        while (maxLength > 0 && maxLength < Long.MAX_VALUE) {
            final List<File> files = getFilesInSizeRange(minLength, maxLength);
            final ListIterator<File> iterator = files.listIterator();

            // Save computation time by only merging when there are at-least 4 files
            // within the list.
            // The value 4 is arbitrary.
            if (files.size() < 4) {
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
     * Merges the specified files into a single file.
     *
     * @param fileA
     *         The first file.
     *
     * @param fileB
     *         The second file.
     */
    private void mergeFiles(final File fileA, final File fileB) {
        // todo Place the contents of fileB into fileA.
        // todo Delete fileB.
        // todo This depends on which storage format we use. It will be different for JSON, txt, and Serialized files.
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
        final File[] allFiles = new File("/data/").listFiles(pathname -> {
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
}
