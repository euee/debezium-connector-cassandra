# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The Debezium Connector for Apache Cassandra is a Change Data Capture (CDC) connector that streams row-level changes from Cassandra commit logs to Kafka. It supports Apache Cassandra 3.x, 4.x, 5.x, and DataStax Enterprise (DSE).

## Build System

This is a Maven multi-module project with the following structure:

- **core**: Common classes shared across all Cassandra versions
- **cassandra-3**: Cassandra 3.x-specific implementation
- **cassandra-4**: Cassandra 4.x-specific implementation
- **cassandra-5**: Cassandra 5.x-specific implementation
- **dse**: DataStax Enterprise-specific implementation
- **tests**: Shared test classes used across version-specific modules

### Build Commands

Build the entire project:
```bash
./mvnw clean install
```

Build with assembly (distribution):
```bash
./mvnw clean install -Passembly
```

Quick build (skip integration tests):
```bash
./mvnw clean install -Dquick
```

Skip tests:
```bash
./mvnw clean install -DskipTests -DskipITs
```

Build specific module:
```bash
./mvnw clean install -pl cassandra-4 -am
```

Install DSE artifacts (required for DSE module):
```bash
./install-artifacts.sh
```

### Testing

Run all tests:
```bash
./mvnw test
```

Run tests for specific module:
```bash
./mvnw test -pl cassandra-4
```

Run integration tests:
```bash
./mvnw verify
```

Run specific test class:
```bash
./mvnw test -pl cassandra-4 -Dtest=ClassName
```

## Architecture

### Version-Specific Pattern

The connector uses an inheritance pattern to handle different Cassandra versions:

- **Core module** contains abstract base classes (`AbstractConnectorTask`, `AbstractSourceConnector`, `AbstractSchemaChangeListener`)
- **Version-specific modules** (cassandra-3/4/5, dse) extend these base classes and implement version-specific logic
- Each version module provides:
  - `CassandraXConnector`: Kafka Connect connector implementation
  - `CassandraXConnectorTask`: Task implementation
  - `CassandraXCommitLogSegmentReader`: Version-specific commit log reader
  - `CassandraXTypeProvider`: Type system provider
  - `CassandraXSchemaChangeListener`: Schema change tracking

### Core Components

**CommitLog Processing Pipeline**:
1. `CommitLogProcessor` - Main processor that watches CDC directory for new commit logs
2. `CommitLogSegmentReader` - Version-specific reader that parses commit log segments
3. `CommitLogIdxProcessor` - Processes commit log index files
4. `QueueProcessor` - Processes queued change events
5. `RecordMaker` - Converts Cassandra mutations to change records

**Change Event Flow**:
1. Cassandra writes mutations to commit log in CDC directory
2. `CommitLogProcessor` detects new commit log files via `AbstractDirectoryWatcher`
3. `CommitLogSegmentReader` reads and deserializes mutations
4. `RecordMaker` creates `ChangeRecord` or `TombstoneRecord` from mutations
5. `KafkaRecordEmitter` emits records to Kafka via `ChangeEventQueue`

**Schema Management**:
- `SchemaLoader` - Loads table schemas from Cassandra
- `SchemaHolder` - Maintains schema cache
- `CassandraSchemaFactory` - Creates Kafka Connect schemas from Cassandra schemas
- `AbstractSchemaChangeListener` - Tracks schema changes in Cassandra

**Type System**:
- `CassandraTypeProvider` - Interface for version-specific type providers
- `CassandraTypeDeserializer` - Deserializes Cassandra types to Kafka Connect types
- Version-specific deserializers in `transforms/type/` for complex types (UDT, Collections, etc.)

**Configuration & Context**:
- `CassandraConnectorConfig` - Central configuration class with all connector settings
- `CassandraConnectorContext` - Runtime context providing access to config, client, schema
- `CassandraClient` - Cassandra database client wrapper

**Offset Management**:
- `OffsetPosition` - Tracks position in commit log processing
- `OffsetWriter` - Persists offsets (file-based or Kafka Connect-based)
- `FileOffsetWriter` - File-based offset storage for standalone mode

### Component Factory Pattern

The connector supports two deployment modes via `ComponentFactory`:
- `ComponentFactoryDebezium` - For Kafka Connect deployment
- `ComponentFactoryStandalone` - For standalone deployment

## Development Guidelines

### Adding Support for New Cassandra Types

1. Create type deserializer in `core/src/main/java/io/debezium/connector/cassandra/transforms/type/`
2. Extend from `DebeziumTypeDeserializer`
3. Register in version-specific `CassandraXTypeProvider`

### Working with Version-Specific Code

- Common logic goes in `core` module abstract classes
- Version-specific implementations override as needed in cassandra-X modules
- Tests go in `tests` module when shared across versions, otherwise in version-specific module

### Java Module System

Cassandra 4+ requires JVM flags for module access. See `cassandra-4/pom.xml` surefire configuration for the required `--add-opens` and `--add-exports` flags. Apply these when running tests or the connector.

### Metrics and Monitoring

- Metrics exposed via JMX using Dropwizard Metrics
- Health checks available at HTTP endpoint (when configured)
- Key metrics classes: `CassandraStreamingMetrics`, `CassandraSnapshotMetrics`

## Testing with Cassandra

The test suite uses:
- Embedded Cassandra for unit tests
- Testcontainers for integration tests
- Docker for version-specific Cassandra instances

Test base classes:
- `CassandraConnectorTestBase` - Base for integration tests
- `AbstractCommitLogProcessorTest` - Base for commit log processing tests

## Debezium Core Dependency

This connector depends on Debezium Core. When building locally or in CI, Debezium Core must be built first and available in Maven local repository. See `.github/workflows/maven.yml` for the build order used in CI.
