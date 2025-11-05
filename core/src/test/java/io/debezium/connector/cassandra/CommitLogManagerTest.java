/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for CommitLogManager.
 */
public class CommitLogManagerTest {

    private Path tempDir;
    private Path cdcDir;
    private CommitLogManager manager;

    @Before
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("commit-log-manager-test");
        cdcDir = Files.createTempDirectory("cdc-dir-test");
        manager = new CommitLogManager(tempDir.toString());
        manager.initialize();
    }

    @After
    public void tearDown() throws IOException {
        if (manager != null) {
            manager.shutdown();
        }
        deleteDirectory(tempDir.toFile());
        deleteDirectory(cdcDir.toFile());
    }

    @Test
    public void testMarkSuccess() {
        manager.markSuccess("CommitLog-7-123456.log");
        assertTrue(manager.isSuccess("CommitLog-7-123456.log"));
        assertFalse(manager.isError("CommitLog-7-123456.log"));
        assertEquals(1, manager.getSuccessCount());
        assertEquals(0, manager.getErrorCount());
    }

    @Test
    public void testMarkError() {
        manager.markError("CommitLog-7-123456.log");
        assertTrue(manager.isError("CommitLog-7-123456.log"));
        assertFalse(manager.isSuccess("CommitLog-7-123456.log"));
        assertEquals(0, manager.getSuccessCount());
        assertEquals(1, manager.getErrorCount());
    }

    @Test
    public void testMarkErrorOverridesSuccess() {
        manager.markSuccess("CommitLog-7-123456.log");
        manager.markError("CommitLog-7-123456.log");
        assertTrue(manager.isError("CommitLog-7-123456.log"));
        assertFalse(manager.isSuccess("CommitLog-7-123456.log"));
        assertEquals(0, manager.getSuccessCount());
        assertEquals(1, manager.getErrorCount());
    }

    @Test
    public void testClearTracked() {
        manager.markSuccess("CommitLog-7-123456.log");
        manager.markError("CommitLog-7-123457.log");
        assertEquals(1, manager.getSuccessCount());
        assertEquals(1, manager.getErrorCount());

        manager.clearTracked();
        assertEquals(0, manager.getSuccessCount());
        assertEquals(0, manager.getErrorCount());
    }

    @Test
    public void testGetCommitLogs() throws IOException {
        // Create test commit log files
        createFile(cdcDir, "CommitLog-7-123456.log");
        createFile(cdcDir, "CommitLog-7-123457.log");
        createFile(cdcDir, "not-a-commit-log.txt");

        File[] commitLogs = manager.getCommitLogs(cdcDir.toFile());
        assertEquals(2, commitLogs.length);
    }

    @Test
    public void testCompareCommitLogs() throws IOException {
        File older = createFile(cdcDir, "CommitLog-7-123456.log");
        File newer = createFile(cdcDir, "CommitLog-7-123457.log");

        assertTrue(manager.compareCommitLogs(older, newer) < 0);
        assertTrue(manager.compareCommitLogs(newer, older) > 0);
        assertEquals(0, manager.compareCommitLogs(older, older));
    }

    @Test
    public void testExtractTimestamp() {
        long timestamp = manager.extractTimestamp("CommitLog-7-123456.log");
        assertEquals(123456L, timestamp);
    }

    @Test
    public void testRelocateMarkedLogs() throws IOException {
        // Create test commit log files
        File successLog = createFile(cdcDir, "CommitLog-7-123456.log");
        File errorLog = createFile(cdcDir, "CommitLog-7-123457.log");

        // Mark them
        manager.markSuccess(successLog.getName());
        manager.markError(errorLog.getName());

        // Relocate
        manager.relocateMarkedLogs(cdcDir.toString());

        // Verify files moved
        assertFalse(successLog.exists());
        assertFalse(errorLog.exists());

        File archiveDir = new File(tempDir.toFile(), CommitLogManager.ARCHIVE_FOLDER);
        File errorDir = new File(tempDir.toFile(), CommitLogManager.ERROR_FOLDER);

        assertTrue(new File(archiveDir, successLog.getName()).exists());
        assertTrue(new File(errorDir, errorLog.getName()).exists());
    }

    @Test
    public void testDeleteCommitLog() throws IOException {
        File commitLog = createFile(cdcDir, "CommitLog-7-123456.log");
        File indexFile = createFile(cdcDir, "CommitLog-7-123456_cdc.idx");

        assertTrue(commitLog.exists());
        assertTrue(indexFile.exists());

        manager.deleteCommitLog(commitLog);

        assertFalse(commitLog.exists());
        assertFalse(indexFile.exists());
    }

    @Test
    public void testDeleteHandler() throws IOException, InterruptedException {
        // Create and relocate files
        File successLog = createFile(cdcDir, "CommitLog-7-123456.log");
        File errorLog = createFile(cdcDir, "CommitLog-7-123457.log");

        manager.markSuccess(successLog.getName());
        manager.markError(errorLog.getName());
        manager.relocateMarkedLogs(cdcDir.toString());

        File archiveDir = new File(tempDir.toFile(), CommitLogManager.ARCHIVE_FOLDER);
        File errorDir = new File(tempDir.toFile(), CommitLogManager.ERROR_FOLDER);

        File relocatedSuccess = new File(archiveDir, successLog.getName());
        File relocatedError = new File(errorDir, errorLog.getName());

        assertTrue(relocatedSuccess.exists());
        assertTrue(relocatedError.exists());

        // Process with delete handler
        manager.processRelocatedLogs(manager.new DeleteHandler());

        // Wait a bit for async processing
        Thread.sleep(500);
        manager.shutdown();

        // Both should be deleted
        assertFalse(relocatedSuccess.exists());
        assertFalse(relocatedError.exists());
    }

    @Test
    public void testDeleteSuccessfulHandler() throws IOException, InterruptedException {
        // Create and relocate files
        File successLog = createFile(cdcDir, "CommitLog-7-123456.log");
        File errorLog = createFile(cdcDir, "CommitLog-7-123457.log");

        manager.markSuccess(successLog.getName());
        manager.markError(errorLog.getName());
        manager.relocateMarkedLogs(cdcDir.toString());

        File archiveDir = new File(tempDir.toFile(), CommitLogManager.ARCHIVE_FOLDER);
        File errorDir = new File(tempDir.toFile(), CommitLogManager.ERROR_FOLDER);

        File relocatedSuccess = new File(archiveDir, successLog.getName());
        File relocatedError = new File(errorDir, errorLog.getName());

        // Process with delete successful handler
        manager.processRelocatedLogs(manager.new DeleteSuccessfulHandler());

        // Wait a bit for async processing
        Thread.sleep(500);
        manager.shutdown();

        // Only success should be deleted
        assertFalse(relocatedSuccess.exists());
        assertTrue(relocatedError.exists());
    }

    // Helper methods

    private File createFile(Path dir, String name) throws IOException {
        File file = new File(dir.toFile(), name);
        file.createNewFile();
        return file;
    }

    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    }
                    else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }
}
