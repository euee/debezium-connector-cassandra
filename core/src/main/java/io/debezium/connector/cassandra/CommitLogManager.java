/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone commit log manager for tracking, relocating, and deleting Cassandra commit log files.
 * This class can be reused in any system that needs to manage Cassandra CDC commit logs.
 *
 * <p>Usage example:
 * <pre>
 * CommitLogManager manager = new CommitLogManager("/path/to/relocation/dir");
 * manager.initialize();
 *
 * // After processing a commit log successfully
 * manager.markSuccess("CommitLog-7-123456.log");
 *
 * // After processing a commit log with errors
 * manager.markError("CommitLog-7-123457.log");
 *
 * // Relocate marked logs from CDC directory to relocation directory
 * manager.relocateMarkedLogs("/path/to/cdc/directory");
 *
 * // Delete relocated logs (or implement custom handler)
 * manager.processRelocatedLogs(new CommitLogManager.DeleteHandler());
 *
 * manager.shutdown();
 * </pre>
 */
public class CommitLogManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogManager.class);

    private static final Pattern FILENAME_REGEX_PATTERN = Pattern.compile("CommitLog-\\d+-(\\d+).log");
    private static final Pattern FILENAME_INDEX_REGEX_PATTERN = Pattern.compile("CommitLog-\\d+-(\\d+)_cdc.idx");

    public static final String ARCHIVE_FOLDER = "archive";
    public static final String ERROR_FOLDER = "error";

    private static final int DEFAULT_THREAD_POOL_SIZE = 10;
    private static final int TERMINATION_WAIT_TIME_SECONDS = 10;

    private final String relocationDir;
    private final Set<String> successfulLogs;
    private final Set<String> errorLogs;
    private final ExecutorService executor;

    private File archiveDir;
    private File errorDir;

    /**
     * Create a new CommitLogManager with default thread pool size.
     *
     * @param relocationDir Directory where commit logs will be moved before deletion
     */
    public CommitLogManager(String relocationDir) {
        this(relocationDir, DEFAULT_THREAD_POOL_SIZE);
    }

    /**
     * Create a new CommitLogManager with custom thread pool size.
     *
     * @param relocationDir Directory where commit logs will be moved before deletion
     * @param threadPoolSize Size of the thread pool for processing relocated logs
     */
    public CommitLogManager(String relocationDir, int threadPoolSize) {
        this.relocationDir = relocationDir;
        this.successfulLogs = new HashSet<>();
        this.errorLogs = new HashSet<>();
        this.executor = Executors.newFixedThreadPool(threadPoolSize);
    }

    /**
     * Initialize the manager by creating necessary directories.
     *
     * @throws IOException if directory creation fails
     */
    public void initialize() throws IOException {
        File dir = new File(relocationDir);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Failed to create relocation directory: " + relocationDir);
        }

        archiveDir = new File(dir, ARCHIVE_FOLDER);
        if (!archiveDir.exists() && !archiveDir.mkdir()) {
            throw new IOException("Failed to create archive directory: " + archiveDir);
        }

        errorDir = new File(dir, ERROR_FOLDER);
        if (!errorDir.exists() && !errorDir.mkdir()) {
            throw new IOException("Failed to create error directory: " + errorDir);
        }

        LOGGER.info("Initialized CommitLogManager with relocation directory: {}", relocationDir);
    }

    /**
     * Mark a commit log as successfully processed.
     *
     * @param commitLogFileName The name of the commit log file
     */
    public void markSuccess(String commitLogFileName) {
        successfulLogs.add(commitLogFileName);
        errorLogs.remove(commitLogFileName);
        LOGGER.debug("Marked commit log {} as successful", commitLogFileName);
    }

    /**
     * Mark a commit log as processed with errors.
     *
     * @param commitLogFileName The name of the commit log file
     */
    public void markError(String commitLogFileName) {
        errorLogs.add(commitLogFileName);
        successfulLogs.remove(commitLogFileName);
        LOGGER.debug("Marked commit log {} as error", commitLogFileName);
    }

    /**
     * Check if a commit log has been marked as error.
     *
     * @param commitLogFileName The name of the commit log file
     * @return true if marked as error, false otherwise
     */
    public boolean isError(String commitLogFileName) {
        return errorLogs.contains(commitLogFileName);
    }

    /**
     * Check if a commit log has been marked as successful.
     *
     * @param commitLogFileName The name of the commit log file
     * @return true if marked as successful, false otherwise
     */
    public boolean isSuccess(String commitLogFileName) {
        return successfulLogs.contains(commitLogFileName);
    }

    /**
     * Relocate all marked commit logs from the CDC directory to the relocation directory.
     * Successful logs go to archive/, error logs go to error/.
     *
     * @param cdcDirectory The CDC directory containing commit logs
     */
    public void relocateMarkedLogs(String cdcDirectory) {
        File cdcDir = new File(cdcDirectory);
        if (!cdcDir.exists() || !cdcDir.isDirectory()) {
            LOGGER.warn("CDC directory does not exist: {}", cdcDirectory);
            return;
        }

        File[] commitLogs = getCommitLogs(cdcDir);
        for (File commitLog : commitLogs) {
            String fileName = commitLog.getName();
            if (successfulLogs.contains(fileName)) {
                moveCommitLog(commitLog.toPath(), archiveDir.toPath());
                successfulLogs.remove(fileName);
            } else if (errorLogs.contains(fileName)) {
                moveCommitLog(commitLog.toPath(), errorDir.toPath());
                errorLogs.remove(fileName);
            }
        }
    }

    /**
     * Relocate a specific commit log from the CDC directory.
     *
     * @param commitLogFile The commit log file to relocate
     */
    public void relocateCommitLog(File commitLogFile) {
        String fileName = commitLogFile.getName();
        if (successfulLogs.contains(fileName)) {
            moveCommitLog(commitLogFile.toPath(), archiveDir.toPath());
            successfulLogs.remove(fileName);
        } else if (errorLogs.contains(fileName)) {
            moveCommitLog(commitLogFile.toPath(), errorDir.toPath());
            errorLogs.remove(fileName);
        } else {
            LOGGER.warn("Commit log {} not marked for relocation", fileName);
        }
    }

    /**
     * Process all relocated commit logs using the provided handler.
     * This will process logs in both archive and error directories.
     *
     * @param handler The handler to process each commit log
     */
    public void processRelocatedLogs(CommitLogHandler handler) {
        processLogsInDirectory(archiveDir, handler, true);
        processLogsInDirectory(errorDir, handler, false);
    }

    private void processLogsInDirectory(File directory, CommitLogHandler handler, boolean success) {
        File[] commitLogs = getCommitLogs(directory);
        Arrays.sort(commitLogs, this::compareCommitLogs);

        for (File commitLog : commitLogs) {
            executor.submit(() -> {
                try {
                    if (success) {
                        handler.onSuccess(commitLog);
                    } else {
                        handler.onError(commitLog);
                    }
                } catch (Exception e) {
                    LOGGER.error("Error processing commit log {}: {}", commitLog.getName(), e.getMessage(), e);
                }
            });
        }
    }

    /**
     * Get all commit log files in a directory.
     *
     * @param directory The directory to search
     * @return Array of commit log files
     */
    public File[] getCommitLogs(File directory) {
        if (!directory.isDirectory()) {
            LOGGER.warn("Given path is not a directory: {}", directory);
            return new File[0];
        }
        File[] files = directory.listFiles(f -> f.isFile() && FILENAME_REGEX_PATTERN.matcher(f.getName()).matches());
        return files != null ? files : new File[0];
    }

    /**
     * Get all commit log index files in a directory.
     *
     * @param directory The directory to search
     * @return Array of index files
     */
    public File[] getIndexes(File directory) {
        if (!directory.isDirectory()) {
            LOGGER.warn("Given path is not a directory: {}", directory);
            return new File[0];
        }
        File[] files = directory.listFiles(f -> f.isFile() && FILENAME_INDEX_REGEX_PATTERN.matcher(f.getName()).matches());
        return files != null ? files : new File[0];
    }

    /**
     * Move a commit log and its index file to a new directory.
     *
     * @param file The commit log file path
     * @param toDir The destination directory
     */
    public void moveCommitLog(Path file, Path toDir) {
        try {
            Matcher filenameMatcher = FILENAME_REGEX_PATTERN.matcher(file.getFileName().toString());
            if (!filenameMatcher.matches()) {
                LOGGER.warn("Cannot move file {} because it does not appear to be a CommitLog", file.toAbsolutePath());
                return;
            }

            if (Files.exists(file)) {
                Files.move(file, toDir.resolve(file.getFileName()), REPLACE_EXISTING);
                LOGGER.info("Moved CommitLog file {} to {}", file.getFileName(), toDir);
            } else {
                LOGGER.warn("CommitLog file {} does not exist", file);
            }
        } catch (Exception ex) {
            LOGGER.error("Failed to move CommitLog file {} to {}", file.getFileName(), toDir, ex);
            throw new RuntimeException("Failed to move commit log", ex);
        }

        // Also move the index file if it exists
        Path indexFile = file.getParent().resolve(file.getFileName().toString().split("\\.")[0] + "_cdc.idx");
        try {
            if (Files.exists(indexFile)) {
                Files.move(indexFile, toDir.resolve(indexFile.getFileName()), REPLACE_EXISTING);
                LOGGER.info("Moved CommitLog index file {} to {}", indexFile.getFileName(), toDir);
            }
        } catch (Exception ex) {
            LOGGER.warn("Failed to move CommitLog index file {} to {}", indexFile.toAbsolutePath(), toDir, ex);
        }
    }

    /**
     * Delete a commit log and its index file.
     *
     * @param file The commit log file
     */
    public void deleteCommitLog(File file) {
        try {
            Matcher filenameMatcher = FILENAME_REGEX_PATTERN.matcher(file.getName());
            if (!filenameMatcher.matches()) {
                LOGGER.warn("Cannot delete file {} because it does not appear to be a CommitLog", file.getName());
                return;
            }

            Files.delete(file.toPath());
            LOGGER.info("Deleted CommitLog file {} from {}", file.getName(), file.getParent());
        } catch (Exception e) {
            LOGGER.error("Failed to delete CommitLog file {} from {}", file.getName(), file.getParent(), e);
            throw new RuntimeException("Failed to delete commit log", e);
        }

        // Also delete the index file if it exists
        Path indexFile = Paths.get(file.toString().split("\\.")[0] + "_cdc.idx");
        try {
            if (Files.exists(indexFile)) {
                Files.delete(indexFile);
                LOGGER.info("Deleted CommitLog index file {} from {}", indexFile.getFileName(), indexFile.getParent());
            }
        } catch (Exception ex) {
            LOGGER.warn("Failed to delete CommitLog index file {}", indexFile.toAbsolutePath(), ex);
        }
    }

    /**
     * Compare two commit log files by their timestamp.
     *
     * @param file1 First commit log file
     * @param file2 Second commit log file
     * @return -1 if file1 is older, 0 if same, 1 if file1 is newer
     */
    public int compareCommitLogs(File file1, File file2) {
        if (file1.equals(file2)) {
            return 0;
        }
        long ts1 = extractTimestamp(file1.getName());
        long ts2 = extractTimestamp(file2.getName());
        return Long.compare(ts1, ts2);
    }

    /**
     * Extract the timestamp from a commit log filename.
     *
     * @param commitLogFileName The commit log filename
     * @return The timestamp
     */
    public long extractTimestamp(String commitLogFileName) {
        Matcher filenameMatcher = FILENAME_REGEX_PATTERN.matcher(commitLogFileName);
        if (!filenameMatcher.matches()) {
            throw new IllegalArgumentException("Cannot extract timestamp from: " + commitLogFileName);
        }
        return Long.parseLong(filenameMatcher.group(1));
    }

    /**
     * Clear all tracked logs (both successful and error).
     */
    public void clearTracked() {
        successfulLogs.clear();
        errorLogs.clear();
        LOGGER.debug("Cleared all tracked commit logs");
    }

    /**
     * Get count of tracked successful logs.
     *
     * @return Number of logs marked as successful
     */
    public int getSuccessCount() {
        return successfulLogs.size();
    }

    /**
     * Get count of tracked error logs.
     *
     * @return Number of logs marked as error
     */
    public int getErrorCount() {
        return errorLogs.size();
    }

    /**
     * Shutdown the manager and cleanup resources.
     *
     * @param await Whether to wait for pending tasks to complete
     */
    public void shutdown(boolean await) {
        try {
            if (!executor.isShutdown()) {
                executor.shutdown();
                if (await) {
                    boolean terminated = executor.awaitTermination(TERMINATION_WAIT_TIME_SECONDS, TimeUnit.SECONDS);
                    if (!terminated) {
                        LOGGER.warn("Executor did not terminate in time, forcing shutdown");
                        executor.shutdownNow();
                    }
                }
            }
            LOGGER.info("CommitLogManager shutdown complete");
        } catch (InterruptedException e) {
            if (!executor.isTerminated()) {
                executor.shutdownNow();
            }
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Shutdown the manager and wait for pending tasks.
     */
    public void shutdown() {
        shutdown(true);
    }

    /**
     * Interface for handling commit log files.
     */
    public interface CommitLogHandler {
        /**
         * Handle a successfully processed commit log.
         *
         * @param file The commit log file
         */
        void onSuccess(File file);

        /**
         * Handle a commit log that was processed with errors.
         *
         * @param file The commit log file
         */
        void onError(File file);
    }

    /**
     * Default handler that deletes all commit logs.
     */
    public class DeleteHandler implements CommitLogHandler {
        @Override
        public void onSuccess(File file) {
            deleteCommitLog(file);
        }

        @Override
        public void onError(File file) {
            deleteCommitLog(file);
        }
    }

    /**
     * Handler that only deletes successful logs, keeps error logs.
     */
    public class DeleteSuccessfulHandler implements CommitLogHandler {
        @Override
        public void onSuccess(File file) {
            deleteCommitLog(file);
        }

        @Override
        public void onError(File file) {
            LOGGER.info("Keeping error commit log: {}", file.getName());
        }
    }

    /**
     * Handler that doesn't delete anything (for testing or archival).
     */
    public static class NoOpHandler implements CommitLogHandler {
        @Override
        public void onSuccess(File file) {
            LOGGER.debug("NoOp handler - keeping successful log: {}", file.getName());
        }

        @Override
        public void onError(File file) {
            LOGGER.debug("NoOp handler - keeping error log: {}", file.getName());
        }
    }
}
