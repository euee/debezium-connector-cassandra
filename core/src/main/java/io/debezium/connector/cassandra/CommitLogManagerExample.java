/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example usage of CommitLogManager for reuse in other systems.
 */
public class CommitLogManagerExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogManagerExample.class);

    public static void main(String[] args) throws Exception {
        // Example 1: Simple usage with deletion
        simpleUsage();

        // Example 2: Custom handler for archiving to S3/cloud storage
        customHandlerUsage();

        // Example 3: Integration with your own processing pipeline
        pipelineIntegration();
    }

    /**
     * Example 1: Simple usage with automatic deletion.
     */
    public static void simpleUsage() throws Exception {
        LOGGER.info("=== Example 1: Simple Usage ===");

        // Create and initialize the manager
        CommitLogManager manager = new CommitLogManager("/tmp/cassandra-relocation");
        manager.initialize();

        String cdcDirectory = "/var/lib/cassandra/cdc_raw";

        // Simulate processing commit logs
        File[] commitLogs = manager.getCommitLogs(new File(cdcDirectory));
        for (File commitLog : commitLogs) {
            try {
                // Your processing logic here
                processCommitLog(commitLog);

                // Mark as successful
                manager.markSuccess(commitLog.getName());
                LOGGER.info("Successfully processed: {}", commitLog.getName());
            }
            catch (Exception e) {
                // Mark as error
                manager.markError(commitLog.getName());
                LOGGER.error("Error processing: {}", commitLog.getName(), e);
            }
        }

        // Relocate all marked logs
        manager.relocateMarkedLogs(cdcDirectory);

        // Process relocated logs (delete them)
        manager.processRelocatedLogs(manager.new DeleteHandler());

        // Shutdown
        manager.shutdown();
    }

    /**
     * Example 2: Custom handler for archiving to cloud storage.
     */
    public static void customHandlerUsage() throws Exception {
        LOGGER.info("=== Example 2: Custom Handler ===");

        CommitLogManager manager = new CommitLogManager("/tmp/cassandra-relocation");
        manager.initialize();

        // Custom handler that archives to S3 before deletion
        CommitLogManager.CommitLogHandler s3Handler = new CommitLogManager.CommitLogHandler() {
            @Override
            public void onSuccess(File file) {
                // Upload to S3
                LOGGER.info("Uploading {} to S3...", file.getName());
                // uploadToS3(file);

                // Then delete local file
                manager.deleteCommitLog(file);
            }

            @Override
            public void onError(File file) {
                // Upload to S3 error bucket
                LOGGER.info("Uploading error log {} to S3 error bucket...", file.getName());
                // uploadToS3ErrorBucket(file);

                // Keep local copy for analysis
                LOGGER.info("Keeping error log locally: {}", file.getName());
            }
        };

        // Process with custom handler
        manager.processRelocatedLogs(s3Handler);
        manager.shutdown();
    }

    /**
     * Example 3: Integration with processing pipeline.
     */
    public static void pipelineIntegration() throws Exception {
        LOGGER.info("=== Example 3: Pipeline Integration ===");

        CommitLogManager manager = new CommitLogManager("/tmp/cassandra-relocation", 5);
        manager.initialize();

        String cdcDirectory = "/var/lib/cassandra/cdc_raw";

        // Get all commit logs sorted by timestamp
        File[] commitLogs = manager.getCommitLogs(new File(cdcDirectory));
        java.util.Arrays.sort(commitLogs, manager::compareCommitLogs);

        // Process in order
        for (File commitLog : commitLogs) {
            String fileName = commitLog.getName();

            // Check if already processed (idempotency)
            if (alreadyProcessed(fileName)) {
                manager.markSuccess(fileName);
                LOGGER.info("Already processed: {}", fileName);
                continue;
            }

            // Extract timestamp for metrics
            long timestamp = manager.extractTimestamp(fileName);
            LOGGER.info("Processing commit log with timestamp: {}", timestamp);

            try {
                // Your processing logic
                processCommitLog(commitLog);

                // Mark and relocate immediately
                manager.markSuccess(fileName);
                manager.relocateCommitLog(commitLog);

                // Record processed
                recordProcessed(fileName);
            }
            catch (Exception e) {
                manager.markError(fileName);
                manager.relocateCommitLog(commitLog);
                LOGGER.error("Failed to process: {}", fileName, e);
            }
        }

        // Background cleanup of relocated logs
        manager.processRelocatedLogs(manager.new DeleteSuccessfulHandler());

        LOGGER.info("Pipeline processed {} successful, {} error logs",
                manager.getSuccessCount(), manager.getErrorCount());

        manager.shutdown();
    }

    // Stub methods for examples
    private static void processCommitLog(File commitLog) throws Exception {
        // Your actual commit log processing logic
        LOGGER.debug("Processing {}", commitLog.getName());
    }

    private static boolean alreadyProcessed(String fileName) {
        // Check your offset/checkpoint storage
        return false;
    }

    private static void recordProcessed(String fileName) {
        // Record in your offset/checkpoint storage
        LOGGER.debug("Recorded processed: {}", fileName);
    }
}
