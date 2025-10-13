# Kafgo

A minimal Kafka-like broker implemented in Go, designed for learning and experimentation. Kafgo implements core Kafka protocol features with proper request/response handling to help developers understand how Kafka works internally.

This project was built following the **CodeCrafters** Kafka implementation guide. CodeCrafters provides hands-on programming challenges to help you build real software and understand systems like Kafka, Redis, Git, and more.
[CodeCrafters Profile Link ](https://app.codecrafters.io/users/abdallahsellem)
## Overview

Kafgo is an educational implementation of a Kafka broker that demonstrates:
- Kafka protocol parsing and serialization
- TCP server implementation for handling concurrent connections
- Metadata management and cluster coordination
- Log file reading/writing in Kafka format
- Request/response handling for core APIs

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    TCP Server (0.0.0.0:9092)                │
│              Handles concurrent client connections           │
└──────────────────────────┬──────────────────────────────────┘
                           │
                ┌──────────┴──────────┐
                │                     │
        ┌───────▼────────┐   ┌───────▼────────┐
        │ Request Parser │   │Response Builder │
        │  (Decoding)    │   │ (Encoding)     │
        └───────┬────────┘   └───────┬────────┘
                │                    │
        ┌───────▼────────────────────▼───────┐
        │      API Handlers                  │
        │  • Produce (Write Records)         │
        │  • Fetch (Read Records)            │
        │  • DescribeTopicPartitions         │
        │  • ApiVersions                     │
        └────────────┬───────────────────────┘
                     │
        ┌────────────▼──────────────┐
        │   Metadata Management     │
        │  • Topic Metadata         │
        │  • Partition Info         │
        │  • Record Batch Parsing   │
        └────────────┬──────────────┘
                     │
        ┌────────────▼──────────────┐
        │   Log File Storage        │
        │  • Cluster Metadata Log   │
        │  • Partition Data Logs    │
        └───────────────────────────┘
```

## Features Implemented

### Kafka Protocol Support

**Request Handling**
- Reads TCP streams with 4-byte size prefix
- Parses request header (API key, version, correlation ID, client ID)
- Dispatches to appropriate API handler based on API key
- Decodes request bodies using Kafka's binary protocol (big-endian, compact arrays)

**Response Encoding**
- Builds responses with proper Kafka format
- Includes correlation ID and message size prefix
- Uses compact arrays (varint encoding) for variable-length fields
- Includes tag buffers for protocol extensions

### Produce API (Key: 0)
- Accepts produce requests with records for specified topics/partitions
- Validates topic and partition existence against cluster metadata
- Returns appropriate error codes:
  - `0`: Success
  - `3` (UNKNOWN_TOPIC_OR_PARTITION): Invalid topic or partition
- Writes validated records to partition log files
- Returns base offset and log start offset to client

**Request Fields:**
- TransactionalID, Acks, TimeoutMs
- Topic data with partition-specific records

**Response Fields:**
- ErrorCode per partition
- BaseOffset, LogAppendTime, LogStartOffset
- RecordErrors (if any)

### Fetch API (Key: 1)
- Accepts fetch requests for specified topic partitions
- Reads records from partition log files
- Returns error codes for unknown topics/partitions
- Streams record batches in Kafka log format

**Request Fields:**
- MaxWaitMs, MinBytes, MaxBytes (flow control)
- IsolationLevel (transaction visibility)
- SessionID/SessionEpoch (session optimization)
- Per-partition: FetchOffset, LogStartOffset, PartitionMaxBytes
- RackID (replica selection hint)

**Response Fields:**
- HighWatermark, LastStableOffset, LogStartOffset
- AbortedTransactions array
- PreferredReadReplica
- Record batches (if successful)

### DescribeTopicPartitions API (Key: 75)
- Returns metadata for requested topics and partitions
- If no topics specified, returns all topics in alphabetical order
- Includes partition details: leader, replicas, in-sync replicas (ISR)

**Response Fields:**
- Topic UUID (16 bytes)
- Partition index, leader ID, leader epoch
- Replica nodes, ISR nodes
- Authorized operations
- Error codes for unknown topics

### ApiVersions API (Key: 18)
- Returns supported API keys with min/max versions
- Helps clients discover broker capabilities

**Supported APIs:**
```
Produce:                  [0, 11]
Fetch:                    [1, 16]
ApiVersions:              [18, 18]
DescribeTopicPartitions:  [75, 75]
```

### Cluster Metadata Loading

Metadata is loaded from Kafka's cluster metadata log at startup and on-demand:

```
/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
```

**Parsing Process:**
1. Read record batches from metadata log
2. Extract TopicRecord entries (API key 2)
3. Extract PartitionRecord entries (API key 3)
4. Build in-memory topic and partition metadata
5. Map topic UUIDs to names and partition details

**Metadata Structure:**
- `TopicMetadata`: Name, UUID, partitions array
- `PartitionMetadata`: Index, leader ID, replicas, ISR nodes

## Core Components

### Server Package (`app/server/`)

**Main Handler: `HandleConnection(conn net.Conn)`**
- Runs in goroutine for each client connection
- Loops reading requests and building/sending responses
- Parses headers and dispatches to API-specific handlers

**Request Parsing:**
- `ReadRequest()`: Reads size prefix and full request
- `ParseProduceRequest()`: Decodes produce API body
- `ParseFetchRequest()`: Decodes fetch API body
- `ParseDescribeTopicPartitionsRequest()`: Decodes describe API body

**Response Building:**
- `BuildProduceResponse()`: Encodes produce response
- `BuildFetchResponse()`: Encodes fetch response with record batches
- `BuildDescribeTopicPartitionsResponse()`: Encodes topic metadata
- `WriteResponse()`: Sends response with correlation ID

**Binary Encoding Helpers:**
- `AppendInt16()`, `AppendInt32()`, `AppendInt64()`: Big-endian encoding
- Compact array/string encoding for space efficiency

### Metadata Package (`app/metadata/`)

**Core Data Structures:**
- `TopicMetadata`: Topic name, UUID, partitions
- `PartitionMetadata`: Partition index, leader, replicas, ISR
- `RecordBatch`: Kafka log record batch header and data

**Metadata Management:**
- `LoadClusterMetadata()`: Loads topics/partitions at startup
- `GetTopicMetadata()`: Returns current metadata map
- `ValidateTopicExists()`: Checks if topic exists
- `ValidatePartitionExists()`: Checks if partition exists

**Record Parsing:**
- `ReadRecordBatch()`: Reads batch header (base offset, length, etc.)
- `ParseRecords()`: Iterates through records in batch
- `ParseRecord()`: Extracts key, value, headers from record
- `ParseTopicRecordFromValue()`: Extracts topic name and UUID
- `ParsePartitionRecordFromValue()`: Extracts partition metadata

**Log File Operations:**
- `LoadPartitionMetadata()`: Reads records from partition log
- `WriteRecordsToLog()`: Appends records to partition log

## Binary Protocol Details

### Compact Arrays
Kafka uses variable-length integer (varint) encoding to save space:
```
0x01 = empty array
0x02 = 1 element
0x03 = 2 elements
value - 1 = element count
```

### Compact Strings
Strings use the same varint encoding:
```
0x00 = null/empty
0x01 = 1 character
value - 1 = character count
```

### Tag Buffers
Flexible versions include tag buffers for future protocol extensions:
```
0x00 = no tagged fields
```

### Big-Endian Integers
All integers (INT16, INT32, INT64) are big-endian encoded.

## Project Structure

```
.
├── app/
│   ├── main.go                       # Entry point, starts TCP server
│   ├── metadata/
│   │   ├── types.go                  # Data structures
│   │   ├── metadata.go               # Loading & parsing
│   │   └── batch.go                  # Record batch handling
│   └── server/
│       ├── types.go                  # Request/response types
│       ├── connection.go             # Connection handler
│       ├── request.go                # Request parsing
│       └── response.go               # Response building
├── your_program.sh                   # Launch system scripts
```

## How to Run

### Quick Start

```bash
./your_program.sh
```

The broker starts listening on:
```
0.0.0.0:9092
```


### Verifying Cluster Metadata

Check that metadata is properly loaded:

```bash
ls -la /tmp/kraft-combined-logs/__cluster_metadata-0/
```

## Getting Started

1. **Prerequisites**
   - Go 1.16 or later
   - Kafka cluster metadata log at `/tmp/kraft-combined-logs/`

2. **Run the Broker**
   ```bash
   ./your_program.sh
   ```

3. **Verify Connection**
   ```bash
   telnet localhost 9092
   ```

4. **Review Logs**
   Watch console output for:
   - Metadata loading
   - Connection handling
   - Request/response processing

## Learning Path

Use Kafgo to understand:

1. **Kafka Protocol Basics**
   - How requests and responses are structured
   - Compact array and string encoding
   - Big-endian byte ordering
   - Correlation IDs for request/response matching

2. **Metadata Management**
   - How brokers store topic and partition information
   - Reading metadata from cluster metadata logs
   - UUID-based topic identification

3. **Log File Format**
   - Record batch structure (headers and data)
   - Variable-length record encoding
   - Key/value/header extraction from records

4. **Concurrent Server Design**
   - TCP connection handling in Go
   - Goroutines for concurrent clients
   - Binary protocol parsing and encoding

5. **API Implementation**
   - Produce flow: validate → write → respond
   - Fetch flow: locate partition → read batches → respond
   - Metadata queries and pagination

## Implementation Notes

**Simplifications Made:**
- No transaction support (AbortedTransactions always empty)
- No replica synchronization or leader election
- Metadata loaded from fixed path only
- No authentication or authorization
- Single-broker cluster (no broker coordination)