# CommitLogManager - Standalone Commit Log Tracker and Deletion Manager

A reusable, standalone component for tracking, relocating, and deleting Cassandra CDC commit log files. This class can be used in any system that needs to manage Cassandra commit logs.

## Features

- **Track Processing Status**: Mark commit logs as successful or error
- **Safe Relocation**: Move logs to archive/error directories before deletion
- **Pluggable Handlers**: Custom logic for handling relocated logs (delete, upload to S3, etc.)
- **Thread-Safe**: Uses executor service for async processing
- **Index File Support**: Automatically handles `_cdc.idx` files
- **Standalone**: No Debezium dependencies, can be used in any Java project

## Architecture

```
CDC Directory (cdc_raw/)
    ↓ [Process logs]
    ↓ [Mark success/error]
    ↓
Relocation Directory
    ├── archive/     [successful logs]
    └── error/       [failed logs]
        ↓ [Process with handler]
        ↓
Delete or Archive (S3, HDFS, etc.)
```

## Quick Start

### Basic Usage - Delete After Processing

```java
// Initialize
CommitLogManager manager = new CommitLogManager("/tmp/relocation");
manager.initialize();

String cdcDir = "/var/lib/cassandra/cdc_raw";

// Process commit logs
for (File commitLog : manager.getCommitLogs(new File(cdcDir))) {
    try {
        // Your processing logic
        processCommitLog(commitLog);
        manager.markSuccess(commitLog.getName());
    } catch (Exception e) {
        manager.markError(commitLog.getName());
    }
}

// Relocate marked logs
manager.relocateMarkedLogs(cdcDir);

// Delete relocated logs
manager.processRelocatedLogs(manager.new DeleteHandler());

// Cleanup
manager.shutdown();
```

### Archive to S3 Before Deletion

```java
CommitLogManager manager = new CommitLogManager("/tmp/relocation");
manager.initialize();

// Custom handler for S3 archival
CommitLogManager.CommitLogHandler s3Handler = new CommitLogManager.CommitLogHandler() {
    @Override
    public void onSuccess(File file) {
        // Upload to S3
        s3Client.putObject("my-bucket", "archive/" + file.getName(), file);
        // Then delete
        manager.deleteCommitLog(file);
    }

    @Override
    public void onError(File file) {
        // Upload to error bucket
        s3Client.putObject("my-bucket", "errors/" + file.getName(), file);
        // Keep local copy for debugging
        LOGGER.warn("Keeping error log: {}", file.getName());
    }
};

manager.processRelocatedLogs(s3Handler);
manager.shutdown();
```

### Keep Error Logs, Delete Successful

```java
manager.processRelocatedLogs(manager.new DeleteSuccessfulHandler());
```

## API Reference

### Core Methods

**Initialization**
```java
void initialize() throws IOException
```
Creates archive/ and error/ directories.

**Tracking**
```java
void markSuccess(String commitLogFileName)
void markError(String commitLogFileName)
boolean isSuccess(String commitLogFileName)
boolean isError(String commitLogFileName)
int getSuccessCount()
int getErrorCount()
void clearTracked()
```

**Relocation**
```java
void relocateMarkedLogs(String cdcDirectory)
void relocateCommitLog(File commitLogFile)
```

**Processing**
```java
void processRelocatedLogs(CommitLogHandler handler)
```

**File Operations**
```java
File[] getCommitLogs(File directory)
File[] getIndexes(File directory)
void moveCommitLog(Path file, Path toDir)
void deleteCommitLog(File file)
```

**Utilities**
```java
int compareCommitLogs(File file1, File file2)
long extractTimestamp(String commitLogFileName)
```

**Lifecycle**
```java
void shutdown()
void shutdown(boolean await)
```

### Built-in Handlers

**DeleteHandler**
- Deletes both successful and error logs

**DeleteSuccessfulHandler**
- Deletes successful logs
- Keeps error logs for debugging

**NoOpHandler**
- Doesn't delete anything
- Useful for testing or when using external cleanup

### Custom Handlers

Implement `CommitLogManager.CommitLogHandler`:

```java
public interface CommitLogHandler {
    void onSuccess(File file);
    void onError(File file);
}
```

## Integration Patterns

### Pattern 1: Immediate Relocation

```java
for (File commitLog : commitLogs) {
    try {
        process(commitLog);
        manager.markSuccess(commitLog.getName());
        manager.relocateCommitLog(commitLog);  // Relocate immediately
    } catch (Exception e) {
        manager.markError(commitLog.getName());
        manager.relocateCommitLog(commitLog);
    }
}

// Later, cleanup in background
manager.processRelocatedLogs(manager.new DeleteHandler());
```

### Pattern 2: Batch Relocation

```java
// Process all logs
for (File commitLog : commitLogs) {
    try {
        process(commitLog);
        manager.markSuccess(commitLog.getName());
    } catch (Exception e) {
        manager.markError(commitLog.getName());
    }
}

// Relocate all at once
manager.relocateMarkedLogs(cdcDir);

// Process all relocated logs
manager.processRelocatedLogs(handler);
```

### Pattern 3: Streaming Pipeline

```java
CommitLogManager manager = new CommitLogManager("/tmp/relocation");
manager.initialize();

ExecutorService processor = Executors.newFixedThreadPool(10);

File[] logs = manager.getCommitLogs(new File(cdcDir));
Arrays.sort(logs, manager::compareCommitLogs);

for (File log : logs) {
    processor.submit(() -> {
        try {
            process(log);
            manager.markSuccess(log.getName());
        } catch (Exception e) {
            manager.markError(log.getName());
        }
        manager.relocateCommitLog(log);
    });
}

processor.shutdown();
processor.awaitTermination(1, TimeUnit.HOURS);
manager.processRelocatedLogs(manager.new DeleteHandler());
manager.shutdown();
```

## Configuration Options

### Constructor Options

```java
// Default thread pool size (10)
CommitLogManager manager = new CommitLogManager("/path/to/relocation");

// Custom thread pool size
CommitLogManager manager = new CommitLogManager("/path/to/relocation", 20);
```

### Directory Structure

The manager creates the following structure:

```
/path/to/relocation/
├── archive/              # Successfully processed logs
│   ├── CommitLog-7-123456.log
│   └── CommitLog-7-123456_cdc.idx
└── error/                # Logs with processing errors
    ├── CommitLog-7-123457.log
    └── CommitLog-7-123457_cdc.idx
```

## File Naming Convention

The manager recognizes Cassandra commit log files matching:
- Logs: `CommitLog-\\d+-(\\d+).log`
- Indexes: `CommitLog-\\d+-(\\d+)_cdc.idx`

Example: `CommitLog-7-1638360000.log` and `CommitLog-7-1638360000_cdc.idx`

## Thread Safety

- All tracking methods (`markSuccess`, `markError`, etc.) use synchronized collections
- File operations are thread-safe
- Processing uses an ExecutorService for parallel handling

## Error Handling

- File operation errors are logged and throw `RuntimeException`
- Invalid commit log names are detected and logged
- Missing index files are handled gracefully

## Testing

See `CommitLogManagerTest.java` for comprehensive test examples:

```bash
./mvnw test -pl core -Dtest=CommitLogManagerTest
```

## Use Cases

1. **CDC Processing**: Track and cleanup commit logs after streaming to Kafka
2. **Backup Systems**: Archive commit logs to S3/HDFS before deletion
3. **Migration Tools**: Process logs during Cassandra migrations
4. **Testing Frameworks**: Manage commit logs in integration tests
5. **Monitoring Tools**: Track commit log processing metrics

## Dependencies

Minimal dependencies:
- SLF4J for logging
- Java 8+
- No Debezium dependencies

## Source Files

- `CommitLogManager.java` - Main class (450+ lines)
- `CommitLogManagerExample.java` - Usage examples
- `CommitLogManagerTest.java` - Unit tests

## License

Apache License 2.0
