import React, { useState } from 'react';
import { BookOpen, Code, Network, Database, CheckCircle, XCircle, ArrowRight } from 'lucide-react';

const KafgoRevision = () => {
  const [activeSection, setActiveSection] = useState('overview');
  const [quizAnswers, setQuizAnswers] = useState({});
  const [showResults, setShowResults] = useState(false);

  const sections = {
    completeFlow: {
      title: "Complete Data Flow",
      icon: ArrowRight,
      content: <CompleteFlowSection />
    },
    
    overview: {
      title: "Project Overview",
      icon: BookOpen,
      content: (
        <div className="space-y-4">
          <h3 className="text-xl font-bold text-gray-800">What is Kafgo?</h3>
          <p className="text-gray-700">A minimal Kafka-like message broker implemented in Go for educational purposes. Built following the CodeCrafters Kafka implementation guide.</p>
          
          <div className="bg-blue-50 border-l-4 border-blue-500 p-4">
            <h4 className="font-semibold text-blue-900 mb-2">Key Learning Goals:</h4>
            <ul className="list-disc list-inside text-blue-800 space-y-1">
              <li>Understanding Kafka's binary protocol</li>
              <li>TCP server implementation with concurrency</li>
              <li>Metadata management and log file operations</li>
              <li>Request/response handling patterns</li>
            </ul>
          </div>

          <div className="grid grid-cols-2 gap-4 mt-4">
            <div className="bg-green-50 p-4 rounded">
              <h4 className="font-semibold text-green-900">Implemented Features</h4>
              <ul className="text-sm text-green-800 mt-2 space-y-1">
                <li>✓ Produce API (write records)</li>
                <li>✓ Fetch API (read records)</li>
                <li>✓ DescribeTopicPartitions</li>
                <li>✓ ApiVersions</li>
              </ul>
            </div>
            <div className="bg-yellow-50 p-4 rounded">
              <h4 className="font-semibold text-yellow-900">Simplifications</h4>
              <ul className="text-sm text-yellow-800 mt-2 space-y-1">
                <li>• No transactions</li>
                <li>• No replication</li>
                <li>• Single broker only</li>
                <li>• No authentication</li>
              </ul>
            </div>
          </div>
        </div>
      )
    },
    
    architecture: {
      title: "Architecture & Flow",
      icon: Network,
      content: (
        <div className="space-y-4">
          <h3 className="text-xl font-bold text-gray-800">System Components</h3>
          
          <div className="bg-gray-800 text-green-400 p-4 rounded font-mono text-sm">
            <pre>{`┌─────────────────────────────────┐
│  TCP Server (0.0.0.0:9092)     │
│  Concurrent Connections         │
└──────────┬──────────────────────┘
           │
    ┌──────┴──────┐
    │             │
┌───▼────┐   ┌───▼────┐
│Request │   │Response│
│Parser  │   │Builder │
└───┬────┘   └───┬────┘
    │            │
┌───▼────────────▼───┐
│   API Handlers     │
│ • Produce          │
│ • Fetch            │
│ • Describe         │
└────────┬───────────┘
         │
┌────────▼──────────┐
│ Metadata Manager  │
└────────┬──────────┘
         │
┌────────▼──────────┐
│  Log Files        │
└───────────────────┘`}</pre>
          </div>

          <div className="space-y-3">
            <div className="border-l-4 border-purple-500 pl-4">
              <h4 className="font-semibold text-purple-900">Connection Flow</h4>
              <p className="text-sm text-gray-700">1. Client connects → 2. Go routine spawned → 3. Loop: Read request → Parse → Handle → Respond</p>
            </div>
            
            <div className="border-l-4 border-indigo-500 pl-4">
              <h4 className="font-semibold text-indigo-900">Request Processing</h4>
              <p className="text-sm text-gray-700">Size prefix (4 bytes) → Header (API key, version, correlation ID) → Body → Dispatch to handler</p>
            </div>
            
            <div className="border-l-4 border-pink-500 pl-4">
              <h4 className="font-semibold text-pink-900">Response Building</h4>
              <p className="text-sm text-gray-700">Handler generates response → Add correlation ID → Size prefix → Send to client</p>
            </div>
          </div>
        </div>
      )
    },
    
    protocol: {
      title: "Kafka Protocol Details",
      icon: Code,
      content: (
        <div className="space-y-4">
          <h3 className="text-xl font-bold text-gray-800">Binary Protocol Encoding</h3>
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="bg-blue-50 p-4 rounded">
              <h4 className="font-semibold text-blue-900 mb-2">Compact Arrays</h4>
              <code className="text-sm bg-white p-2 block rounded">
                0x01 = empty (0 elements)<br/>
                0x02 = 1 element<br/>
                0x03 = 2 elements<br/>
                value - 1 = count
              </code>
              <p className="text-xs text-blue-700 mt-2">Uses variable-length integer (varint) encoding</p>
            </div>
            
            <div className="bg-green-50 p-4 rounded">
              <h4 className="font-semibold text-green-900 mb-2">Compact Strings</h4>
              <code className="text-sm bg-white p-2 block rounded">
                0x00 = null/empty<br/>
                0x01 = 1 character<br/>
                value - 1 = char count
              </code>
              <p className="text-xs text-green-700 mt-2">Same varint encoding as arrays</p>
            </div>
            
            <div className="bg-purple-50 p-4 rounded">
              <h4 className="font-semibold text-purple-900 mb-2">Integers</h4>
              <code className="text-sm bg-white p-2 block rounded">
                INT16: 2 bytes, big-endian<br/>
                INT32: 4 bytes, big-endian<br/>
                INT64: 8 bytes, big-endian
              </code>
            </div>
            
            <div className="bg-orange-50 p-4 rounded">
              <h4 className="font-semibold text-orange-900 mb-2">Tag Buffers</h4>
              <code className="text-sm bg-white p-2 block rounded">
                0x00 = no tagged fields
              </code>
              <p className="text-xs text-orange-700 mt-2">For flexible protocol versions</p>
            </div>
          </div>

          <div className="bg-yellow-50 border border-yellow-200 p-4 rounded mt-4">
            <h4 className="font-semibold text-yellow-900 mb-2">⚡ Key Interview Point</h4>
            <p className="text-sm text-yellow-800">All multi-byte integers use <strong>big-endian</strong> encoding (network byte order). This is critical for cross-platform compatibility.</p>
          </div>
        </div>
      )
    },
    
    apis: {
      title: "API Implementations",
      icon: Database,
      content: (
        <div className="space-y-4">
          <h3 className="text-xl font-bold text-gray-800">Supported APIs</h3>
          
          <div className="space-y-3">
            <details className="bg-white border border-gray-200 rounded p-4">
              <summary className="font-semibold text-gray-800 cursor-pointer">
                Produce API (Key: 0) - Write Records
              </summary>
              <div className="mt-3 text-sm space-y-2">
                <p className="text-gray-700"><strong>Purpose:</strong> Accept records for topics/partitions and write to log files</p>
                <div className="bg-gray-50 p-2 rounded">
                  <p className="font-medium">Request Fields:</p>
                  <ul className="list-disc list-inside text-gray-600 ml-2">
                    <li>TransactionalID, Acks, TimeoutMs</li>
                    <li>TopicData with partition-specific records</li>
                  </ul>
                </div>
                <div className="bg-gray-50 p-2 rounded">
                  <p className="font-medium">Response Fields:</p>
                  <ul className="list-disc list-inside text-gray-600 ml-2">
                    <li>ErrorCode (0=success, 3=unknown topic/partition)</li>
                    <li>BaseOffset, LogAppendTime, LogStartOffset</li>
                  </ul>
                </div>
                <p className="text-blue-600"><strong>Flow:</strong> Validate topic exists → Validate partition → Write to log → Return offsets</p>
              </div>
            </details>

            <details className="bg-white border border-gray-200 rounded p-4">
              <summary className="font-semibold text-gray-800 cursor-pointer">
                Fetch API (Key: 1) - Read Records
              </summary>
              <div className="mt-3 text-sm space-y-2">
                <p className="text-gray-700"><strong>Purpose:</strong> Read records from partition log files</p>
                <div className="bg-gray-50 p-2 rounded">
                  <p className="font-medium">Request Fields:</p>
                  <ul className="list-disc list-inside text-gray-600 ml-2">
                    <li>MaxWaitMs, MinBytes, MaxBytes</li>
                    <li>SessionID, SessionEpoch</li>
                    <li>Per-partition: FetchOffset, PartitionMaxBytes</li>
                  </ul>
                </div>
                <div className="bg-gray-50 p-2 rounded">
                  <p className="font-medium">Response Fields:</p>
                  <ul className="list-disc list-inside text-gray-600 ml-2">
                    <li>HighWatermark, LastStableOffset, LogStartOffset</li>
                    <li>Record batches (if successful)</li>
                    <li>ErrorCode (100=unknown topic ID)</li>
                  </ul>
                </div>
              </div>
            </details>

            <details className="bg-white border border-gray-200 rounded p-4">
              <summary className="font-semibold text-gray-800 cursor-pointer">
                DescribeTopicPartitions API (Key: 75) - Metadata
              </summary>
              <div className="mt-3 text-sm space-y-2">
                <p className="text-gray-700"><strong>Purpose:</strong> Return topic and partition metadata</p>
                <div className="bg-gray-50 p-2 rounded">
                  <p className="font-medium">Behavior:</p>
                  <ul className="list-disc list-inside text-gray-600 ml-2">
                    <li>If no topics specified → return ALL topics (sorted alphabetically)</li>
                    <li>Returns: TopicID (UUID), partition details, leader, replicas, ISR</li>
                  </ul>
                </div>
              </div>
            </details>

            <details className="bg-white border border-gray-200 rounded p-4">
              <summary className="font-semibold text-gray-800 cursor-pointer">
                ApiVersions API (Key: 18) - Capabilities
              </summary>
              <div className="mt-3 text-sm space-y-2">
                <p className="text-gray-700"><strong>Purpose:</strong> Let clients discover broker capabilities</p>
                <div className="bg-gray-50 p-2 rounded font-mono text-xs">
                  Produce: [0, 11]<br/>
                  Fetch: [1, 16]<br/>
                  ApiVersions: [18, 18]<br/>
                  DescribeTopicPartitions: [75, 75]
                </div>
              </div>
            </details>
          </div>
        </div>
      )
    },
    
    metadata: {
      title: "Metadata Management",
      icon: Database,
      content: (
        <div className="space-y-4">
          <h3 className="text-xl font-bold text-gray-800">Cluster Metadata Loading</h3>
          
          <div className="bg-gray-100 p-4 rounded">
            <p className="font-mono text-sm text-gray-700">/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log</p>
          </div>

          <div className="space-y-3">
            <h4 className="font-semibold text-gray-800">Parsing Process:</h4>
            <div className="flex items-start space-x-3">
              <div className="bg-blue-500 text-white rounded-full w-8 h-8 flex items-center justify-center flex-shrink-0">1</div>
              <div>
                <p className="font-medium">Read Record Batches</p>
                <p className="text-sm text-gray-600">Parse batch header: base offset, length, magic byte, CRC, attributes, timestamps, record count</p>
              </div>
            </div>
            
            <div className="flex items-start space-x-3">
              <div className="bg-blue-500 text-white rounded-full w-8 h-8 flex items-center justify-center flex-shrink-0">2</div>
              <div>
                <p className="font-medium">Extract TopicRecord (Type 2)</p>
                <p className="text-sm text-gray-600">Parse topic name (COMPACT_STRING) and UUID (16 bytes)</p>
              </div>
            </div>
            
            <div className="flex items-start space-x-3">
              <div className="bg-blue-500 text-white rounded-full w-8 h-8 flex items-center justify-center flex-shrink-0">3</div>
              <div>
                <p className="font-medium">Extract PartitionRecord (Type 3)</p>
                <p className="text-sm text-gray-600">Parse partition ID, topic ID, replicas (COMPACT_ARRAY), ISR, leader, leader epoch</p>
              </div>
            </div>
            
            <div className="flex items-start space-x-3">
              <div className="bg-blue-500 text-white rounded-full w-8 h-8 flex items-center justify-center flex-shrink-0">4</div>
              <div>
                <p className="font-medium">Build In-Memory Metadata</p>
                <p className="text-sm text-gray-600">Store in TopicsMetadata map for fast lookups</p>
              </div>
            </div>
          </div>

          <div className="bg-purple-50 border-l-4 border-purple-500 p-4">
            <h4 className="font-semibold text-purple-900 mb-2">Data Structures</h4>
            <pre className="text-xs bg-white p-2 rounded overflow-x-auto">
{`type TopicMetadata struct {
    Name       string
    TopicID    [16]byte  // UUID
    Partitions []PartitionMetadata
}

type PartitionMetadata struct {
    PartitionIndex int32
    LeaderID       int32
    ReplicaNodes   []int32
    IsrNodes       []int32  // In-Sync Replicas
}`}
            </pre>
          </div>

          <div className="bg-green-50 border-l-4 border-green-500 p-4">
            <h4 className="font-semibold text-green-900 mb-2">Validation Functions</h4>
            <ul className="text-sm text-green-800 space-y-1">
              <li>✓ <strong>ValidateTopicExists</strong>: Check if topic in metadata</li>
              <li>✓ <strong>ValidatePartitionExists</strong>: Check if partition exists for topic</li>
              <li>✓ Both re-read metadata if not found in memory (handles updates)</li>
            </ul>
          </div>
        </div>
      )
    },
    
    quiz: {
      title: "Knowledge Check",
      icon: CheckCircle,
      content: <QuizSection quizAnswers={quizAnswers} setQuizAnswers={setQuizAnswers} showResults={showResults} setShowResults={setShowResults} />
    }
  };

  const Section = sections[activeSection];
  const Icon = Section.icon;

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 p-6">
      <div className="max-w-6xl mx-auto">
        <div className="bg-white rounded-lg shadow-xl overflow-hidden">
          {/* Header */}
          <div className="bg-gradient-to-r from-blue-600 to-indigo-600 p-6 text-white">
            <h1 className="text-3xl font-bold mb-2">Kafgo - Interview Revision</h1>
            <p className="text-blue-100">Master your Kafka-like broker implementation</p>
          </div>

          {/* Navigation */}
          <div className="bg-gray-50 border-b border-gray-200 p-4">
            <div className="flex flex-wrap gap-2">
              {Object.keys(sections).map((key) => {
                const SectionIcon = sections[key].icon;
                return (
                  <button
                    key={key}
                    onClick={() => setActiveSection(key)}
                    className={`flex items-center space-x-2 px-4 py-2 rounded-lg transition-all ${
                      activeSection === key
                        ? 'bg-blue-600 text-white shadow-md'
                        : 'bg-white text-gray-700 hover:bg-gray-100'
                    }`}
                  >
                    <SectionIcon size={18} />
                    <span className="text-sm font-medium">{sections[key].title}</span>
                  </button>
                );
              })}
            </div>
          </div>

          {/* Content */}
          <div className="p-6">
            <div className="flex items-center space-x-3 mb-6">
              <Icon className="text-blue-600" size={32} />
              <h2 className="text-2xl font-bold text-gray-800">{Section.title}</h2>
            </div>
            {Section.content}
          </div>
        </div>

        {/* Quick Tips Footer */}
        <div className="mt-6 bg-white rounded-lg shadow-lg p-6">
          <h3 className="font-bold text-gray-800 mb-3 flex items-center">
            <span className="bg-yellow-400 text-yellow-900 rounded-full w-6 h-6 flex items-center justify-center mr-2 text-sm">!</span>
            Interview Tips
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
            <div className="bg-blue-50 p-3 rounded">
              <p className="font-semibold text-blue-900">Be Ready to Explain</p>
              <p className="text-blue-700">Why you chose Go, how concurrency works (goroutines), why big-endian encoding</p>
            </div>
            <div className="bg-green-50 p-3 rounded">
              <p className="font-semibold text-green-900">Know the Tradeoffs</p>
              <p className="text-green-700">Why no transactions? Why single broker? What would you add for production?</p>
            </div>
            <div className="bg-purple-50 p-3 rounded">
              <p className="font-semibold text-purple-900">Deep Dive Ready</p>
              <p className="text-purple-700">Be ready to walk through the Produce flow end-to-end on a whiteboard</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

const CompleteFlowSection = () => {
  const [activeStep, setActiveStep] = useState(0);

  const metadataFlow = [
    {
      title: "Broker Startup - Load Metadata",
      description: "When Kafgo starts, it loads cluster metadata BEFORE accepting any connections",
      details: [
        "Opens file: /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log",
        "Reads record batches sequentially",
        "Each batch has: BaseOffset, BatchLength, Magic, CRC, RecordCount, Records[]",
        "Parses individual records from batch.Records byte array"
      ],
      code: `// In main.go
metadata.LoadClusterMetadata()  // Load BEFORE accepting connections
listener, err := net.Listen("tcp", "0.0.0.0:9092")`,
      color: "blue"
    },
    {
      title: "Parse TopicRecord (Type 2)",
      description: "Extract topic information from metadata log",
      details: [
        "Read record value from batch",
        "Check if record type = 2 (TopicRecord)",
        "Parse TAG_BUFFER (1 byte)",
        "Parse topic name (COMPACT_STRING using varint)",
        "Parse topic UUID (16 bytes)",
        "Store in TopicsMetadata map"
      ],
      code: `TopicsMetadata[name] = &TopicMetadata{
    Name:    "my-topic",
    TopicID: [16]byte{...},  // UUID
    Partitions: []PartitionMetadata{},
}`,
      color: "purple"
    },
    {
      title: "Parse PartitionRecord (Type 3)",
      description: "Extract partition information and link to topic",
      details: [
        "Check if record type = 3 (PartitionRecord)",
        "Parse partition ID (INT32)",
        "Parse topic ID UUID (16 bytes)",
        "Parse replicas (COMPACT_ARRAY of INT32)",
        "Parse ISR - In-Sync Replicas (COMPACT_ARRAY of INT32)",
        "Parse leader ID (INT32)",
        "Find topic by UUID and append partition to it"
      ],
      code: `topic.Partitions = append(topic.Partitions, PartitionMetadata{
    PartitionIndex: 0,
    LeaderID:      1,
    ReplicaNodes:  []int32{1},
    IsrNodes:      []int32{1},
})`,
      color: "indigo"
    },
    {
      title: "In-Memory Storage Structure",
      description: "Metadata is stored in a map for fast lookup",
      details: [
        "TopicsMetadata is a map[string]*TopicMetadata",
        "Key = topic name (e.g., 'my-topic')",
        "Value = pointer to TopicMetadata struct",
        "Each TopicMetadata has array of PartitionMetadata",
        "No database - all metadata is in RAM"
      ],
      code: `var TopicsMetadata = make(map[string]*TopicMetadata)

// Access metadata
topic := TopicsMetadata["my-topic"]
partitions := topic.Partitions  // []PartitionMetadata`,
      color: "green"
    }
  ];

  const completeFlow = [
    {
      phase: "1. Client Connects",
      icon: "🔌",
      steps: [
        "Client establishes TCP connection to broker at 0.0.0.0:9092",
        "Broker accepts connection: conn, err := listener.Accept()",
        "Broker spawns new goroutine: go server.HandleConnection(conn)",
        "Connection is now ready to handle requests in parallel with other clients"
      ],
      color: "blue"
    },
    {
      phase: "2. Discover API Capabilities",
      icon: "🔍",
      steps: [
        "Client sends ApiVersions request (API Key 18)",
        "Request format: [4-byte size][API key=18][version][correlation_id][client_id][TAG_BUFFER]",
        "Broker receives and parses request header",
        "Broker responds with supported API keys and version ranges",
        "Response includes: Produce [0,11], Fetch [1,16], ApiVersions [18,18], DescribeTopicPartitions [75,75]"
      ],
      code: `Client → Broker: ApiVersions Request
┌─────────┬────────┬─────────┬──────────────┬──────────┐
│ Size(4) │ Key=18 │ Ver(2)  │ CorrID(4)    │ ClientID │
└─────────┴────────┴─────────┴──────────────┴──────────┘

Broker → Client: ApiVersions Response
{
  Produce: [0, 11],
  Fetch: [1, 16],
  ...
}`,
      color: "purple"
    },
    {
      phase: "3. Discover Topics",
      icon: "📋",
      steps: [
        "Client sends DescribeTopicPartitions request (API Key 75)",
        "If topics array is empty → broker returns ALL topics (sorted alphabetically)",
        "If specific topics requested → broker returns only those",
        "Broker looks up metadata from TopicsMetadata map (loaded at startup)",
        "For each topic: returns name, UUID, partitions with leader, replicas, ISR",
        "Error code 3 if topic not found"
      ],
      code: `// Broker looks up metadata
topicsMetadata := metadata.GetTopicMetadata()
topic := topicsMetadata["my-topic"]

Response per topic:
- TopicName: "my-topic"
- TopicID: [16]byte UUID
- Partitions: [
    {Index: 0, Leader: 1, Replicas: [1], ISR: [1]},
    {Index: 1, Leader: 1, Replicas: [1], ISR: [1]}
  ]`,
      color: "indigo"
    },
    {
      phase: "4. Produce Data (Write)",
      icon: "✍️",
      steps: [
        "Client sends Produce request (API Key 0) with topic, partition, and records",
        "Request includes: TransactionalID, Acks, TimeoutMs, TopicData[]",
        "Each TopicData has: Name (COMPACT_STRING), PartitionData[]",
        "Each PartitionData has: Index (INT32), Records (COMPACT_RECORDS as bytes)",
        "Broker validates topic exists: metadata.ValidateTopicExists(topicName)",
        "Broker validates partition exists: metadata.ValidatePartitionExists(topicName, partitionIndex)",
        "If valid: writes raw record bytes to log file",
        "Log file path: /tmp/kraft-combined-logs/{topic}-{partition}/00000000000000000000.log",
        "Opens file in append mode, writes records, closes file",
        "Returns response with BaseOffset=0, LogStartOffset=0, ErrorCode=0 (success)",
        "If invalid: returns ErrorCode=3 (UNKNOWN_TOPIC_OR_PARTITION)"
      ],
      code: `// Validation
if !metadata.ValidateTopicExists("my-topic") {
    return ErrorCode = 3  // UNKNOWN_TOPIC_OR_PARTITION
}
if !metadata.ValidatePartitionExists("my-topic", 0) {
    return ErrorCode = 3
}

// Write to log
logFile := "/tmp/kraft-combined-logs/my-topic-0/00000000000000000000.log"
file.Write(recordBytes)  // Append mode

// Response
{
    ErrorCode: 0,
    BaseOffset: 0,
    LogStartOffset: 0
}`,
      color: "green"
    },
    {
      phase: "5. Fetch Data (Read)",
      icon: "📖",
      steps: [
        "Client sends Fetch request (API Key 1) with topic UUID, partition, and fetch offset",
        "Request includes: MaxWaitMs, MinBytes, MaxBytes, SessionID, Topics[]",
        "Each Topic has: TopicID (UUID 16 bytes), Partitions[]",
        "Each Partition has: Partition (INT32), FetchOffset (INT64), PartitionMaxBytes",
        "Broker looks up topic by UUID from topicsByID map",
        "Validates partition exists in topic.Partitions",
        "If invalid: returns ErrorCode=100 (UNKNOWN_TOPIC_ID)",
        "If valid: calls LoadPartitionMetadata(topicID, partition)",
        "Reads entire log file for that partition",
        "Wraps log data in COMPACT_RECORDS format (varint length + data)",
        "Returns response with HighWatermark, LastStableOffset, LogStartOffset, and record batches",
        "Record batches include all the raw bytes from the log file"
      ],
      code: `// Lookup by UUID
topicMeta := topicsByID[topicUUID]
validPartitions := map of partition indices

// Validate partition
if partition not in validPartitions {
    return ErrorCode = 100  // UNKNOWN_TOPIC_ID
}

// Read partition log
logPath := "/tmp/kraft-combined-logs/my-topic-0/00000000000000000000.log"
logData := ReadFile(logPath)

// Wrap in COMPACT_RECORDS
recordsLength := len(logData) + 1
response = AppendUvarint(recordsLength)
response = append(logData)

// Response structure
{
    ErrorCode: 0,
    HighWatermark: fetchOffset,
    LogStartOffset: logStartOffset,
    Records: [raw batch bytes from log file]
}`,
      color: "orange"
    },
    {
      phase: "6. Response & Correlation",
      icon: "🔄",
      steps: [
        "Every response includes the correlation ID from the request",
        "Client can match async responses to requests using correlation ID",
        "Response format: [4-byte total size][4-byte correlation_id][response body]",
        "Connection remains open for more requests (persistent connection)",
        "Each request-response cycle is independent",
        "Goroutine handles multiple sequential requests from same client"
      ],
      code: `// Request
CorrelationID: 12345

// Response structure
┌─────────────┬─────────────┬──────────────────┐
│ Size (4)    │ CorrID=12345│ Response Body    │
└─────────────┴─────────────┴──────────────────┘

// Client matches:
responses[12345] = this response`,
      color: "pink"
    }
  ];

  return (
    <div className="space-y-8">
      {/* Metadata Storage & Retrieval */}
      <div>
        <h3 className="text-2xl font-bold text-gray-800 mb-4">📦 Metadata Storage & Retrieval</h3>
        <p className="text-gray-700 mb-6">Metadata is loaded once at startup and stored in memory for fast access.</p>
        
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
          {metadataFlow.map((step, idx) => (
            <button
              key={idx}
              onClick={() => setActiveStep(idx)}
              className={`text-left p-4 rounded-lg border-2 transition-all ${
                activeStep === idx
                  ? `border-${step.color}-500 bg-${step.color}-50 shadow-lg`
                  : 'border-gray-200 bg-white hover:border-gray-300'
              }`}
            >
              <div className={`font-semibold text-${step.color}-900 mb-1`}>
                Step {idx + 1}
              </div>
              <div className="text-sm text-gray-700">{step.title}</div>
            </button>
          ))}
        </div>

        <div className="bg-white border-2 border-gray-200 rounded-lg p-6 shadow-lg">
          <div className={`inline-block px-3 py-1 bg-${metadataFlow[activeStep].color}-100 text-${metadataFlow[activeStep].color}-900 rounded-full text-sm font-semibold mb-3`}>
            Step {activeStep + 1} of {metadataFlow.length}
          </div>
          <h4 className="text-xl font-bold text-gray-800 mb-2">{metadataFlow[activeStep].title}</h4>
          <p className="text-gray-600 mb-4">{metadataFlow[activeStep].description}</p>
          
          <div className="bg-gray-50 rounded-lg p-4 mb-4">
            <h5 className="font-semibold text-gray-800 mb-2">Details:</h5>
            <ul className="space-y-2">
              {metadataFlow[activeStep].details.map((detail, idx) => (
                <li key={idx} className="flex items-start">
                  <span className={`text-${metadataFlow[activeStep].color}-500 mr-2`}>▸</span>
                  <span className="text-gray-700 text-sm">{detail}</span>
                </li>
              ))}
            </ul>
          </div>

          <div className="bg-gray-900 text-green-400 p-4 rounded-lg font-mono text-sm overflow-x-auto">
            <pre>{metadataFlow[activeStep].code}</pre>
          </div>

          <div className="flex justify-between mt-4">
            <button
              onClick={() => setActiveStep(Math.max(0, activeStep - 1))}
              disabled={activeStep === 0}
              className="px-4 py-2 bg-gray-200 text-gray-700 rounded disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-300"
            >
              ← Previous
            </button>
            <button
              onClick={() => setActiveStep(Math.min(metadataFlow.length - 1, activeStep + 1))}
              disabled={activeStep === metadataFlow.length - 1}
              className="px-4 py-2 bg-blue-600 text-white rounded disabled:opacity-50 disabled:cursor-not-allowed hover:bg-blue-700"
            >
              Next →
            </button>
          </div>
        </div>
      </div>

      {/* Complete Client Flow */}
      <div>
        <h3 className="text-2xl font-bold text-gray-800 mb-4">🔄 Complete Client Flow: Connect → Discover → Produce → Fetch</h3>
        <p className="text-gray-700 mb-6">Follow the complete journey of data from initial connection to reading it back.</p>
        
        <div className="space-y-4">
          {completeFlow.map((phase, idx) => (
            <details key={idx} className="bg-white border-2 border-gray-200 rounded-lg shadow-md" open={idx === 0}>
              <summary className={`cursor-pointer p-4 font-semibold text-lg text-gray-800 hover:bg-gray-50 rounded-lg bg-${phase.color}-50 border-l-4 border-${phase.color}-500`}>
                <span className="text-2xl mr-3">{phase.icon}</span>
                {phase.phase}
              </summary>
              <div className="p-6 space-y-3 border-t-2 border-gray-100">
                <ol className="space-y-3">
                  {phase.steps.map((step, stepIdx) => (
                    <li key={stepIdx} className="flex items-start">
                      <span className={`inline-block w-6 h-6 rounded-full bg-${phase.color}-500 text-white text-center text-sm font-bold mr-3 flex-shrink-0`}>
                        {stepIdx + 1}
                      </span>
                      <span className="text-gray-700 pt-0.5">{step}</span>
                    </li>
                  ))}
                </ol>
                
                {phase.code && (
                  <div className="mt-4 bg-gray-900 text-green-400 p-4 rounded-lg font-mono text-xs overflow-x-auto">
                    <pre>{phase.code}</pre>
                  </div>
                )}
              </div>
            </details>
          ))}
        </div>
      </div>

      {/* Quick Reference */}
      <div className="bg-gradient-to-r from-blue-600 to-indigo-600 text-white p-6 rounded-lg shadow-lg">
        <h3 className="text-xl font-bold mb-4">⚡ Quick Reference: File Paths</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 font-mono text-sm">
          <div className="bg-white bg-opacity-20 p-3 rounded">
            <div className="font-semibold mb-1">Cluster Metadata:</div>
            <div className="text-blue-100">/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log</div>
          </div>
          <div className="bg-white bg-opacity-20 p-3 rounded">
            <div className="font-semibold mb-1">Partition Data:</div>
            <div className="text-blue-100">/tmp/kraft-combined-logs/{'<topic>-<partition>'}/00000000000000000000.log</div>
          </div>
        </div>
      </div>
    </div>
  );
};

const QuizSection = ({ quizAnswers, setQuizAnswers, showResults, setShowResults }) => {
  const questions = [
    {
      id: 'q1',
      question: 'What is the purpose of the correlation ID in Kafka protocol?',
      options: [
        'To encrypt the message',
        'To match requests with responses in async communication',
        'To identify the client uniquely',
        'To set the message priority'
      ],
      correct: 1,
      explanation: 'The correlation ID is sent by the client in the request and echoed back in the response, allowing the client to match asynchronous responses to their original requests.'
    },
    {
      id: 'q2',
      question: 'In Kafgo, what error code is returned when a topic or partition doesn\'t exist in a Produce request?',
      options: [
        'Error code 0 (success)',
        'Error code 3 (UNKNOWN_TOPIC_OR_PARTITION)',
        'Error code 100 (UNKNOWN_TOPIC_ID)',
        'Error code 35 (unsupported version)'
      ],
      correct: 1,
      explanation: 'Error code 3 (UNKNOWN_TOPIC_OR_PARTITION) is returned when trying to produce to a topic or partition that doesn\'t exist in the metadata.'
    },
    {
      id: 'q3',
      question: 'How does Kafgo handle concurrent client connections?',
      options: [
        'Using a thread pool',
        'Single-threaded event loop',
        'Spawning a goroutine for each connection',
        'Using select() system call'
      ],
      correct: 2,
      explanation: 'Kafgo spawns a new goroutine for each client connection in the main loop, allowing concurrent handling of multiple clients efficiently using Go\'s lightweight concurrency model.'
    },
    {
      id: 'q4',
      question: 'What does a compact array value of 0x01 represent?',
      options: [
        '1 element',
        'Empty array (0 elements)',
        'Null array',
        'Invalid value'
      ],
      correct: 1,
      explanation: 'In Kafka\'s compact array encoding, the value represents length + 1. So 0x01 means 1 - 1 = 0 elements (empty array).'
    },
    {
      id: 'q5',
      question: 'Where does Kafgo load cluster metadata from at startup?',
      options: [
        'A PostgreSQL database',
        'Environment variables',
        '/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log',
        'An HTTP API endpoint'
      ],
      correct: 2,
      explanation: 'Kafgo reads the cluster metadata from the Kafka metadata log file at /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log, parsing TopicRecord and PartitionRecord entries.'
    },
    {
      id: 'q6',
      question: 'What is the record type identifier for a TopicRecord in the metadata log?',
      options: [
        'Type 0',
        'Type 1',
        'Type 2',
        'Type 3'
      ],
      correct: 2,
      explanation: 'TopicRecord uses type 2, while PartitionRecord uses type 3. These are parsed from the value field of records in the cluster metadata log.'
    },
    {
      id: 'q7',
      question: 'What byte order (endianness) does Kafka protocol use for integers?',
      options: [
        'Little-endian',
        'Big-endian (network byte order)',
        'Native system endianness',
        'Variable depending on the field'
      ],
      correct: 1,
      explanation: 'Kafka protocol uses big-endian (network byte order) for all multi-byte integers to ensure cross-platform compatibility.'
    },
    {
      id: 'q8',
      question: 'In the Fetch API response, what does the HighWatermark represent?',
      options: [
        'The maximum message size',
        'The offset of the last committed message',
        'The network bandwidth limit',
        'The number of concurrent connections'
      ],
      correct: 1,
      explanation: 'HighWatermark indicates the offset of the last message that has been successfully replicated to all in-sync replicas. In Kafgo\'s simplified implementation, it\'s set to the fetch offset.'
    }
  ];

  const handleAnswer = (questionId, optionIndex) => {
    setQuizAnswers(prev => ({
      ...prev,
      [questionId]: optionIndex
    }));
  };

  const calculateScore = () => {
    let correct = 0;
    questions.forEach(q => {
      if (quizAnswers[q.id] === q.correct) correct++;
    });
    return correct;
  };

  const score = calculateScore();
  const percentage = Math.round((score / questions.length) * 100);

  return (
    <div className="space-y-6">
      <div className="bg-gradient-to-r from-purple-50 to-pink-50 p-4 rounded-lg border border-purple-200">
        <p className="text-purple-900 font-medium">Test your knowledge with these questions commonly asked in interviews about message broker implementations.</p>
      </div>

      {questions.map((q, idx) => (
        <div key={q.id} className="bg-white border border-gray-200 rounded-lg p-4 shadow-sm">
          <h4 className="font-semibold text-gray-800 mb-3">
            {idx + 1}. {q.question}
          </h4>
          <div className="space-y-2">
            {q.options.map((option, optIdx) => (
              <label
                key={optIdx}
                className={`flex items-center space-x-3 p-3 rounded cursor-pointer transition-all ${
                  quizAnswers[q.id] === optIdx
                    ? showResults
                      ? optIdx === q.correct
                        ? 'bg-green-100 border-2 border-green-500'
                        : 'bg-red-100 border-2 border-red-500'
                      : 'bg-blue-100 border-2 border-blue-500'
                    : showResults && optIdx === q.correct
                    ? 'bg-green-50 border-2 border-green-300'
                    : 'bg-gray-50 hover:bg-gray-100 border border-gray-200'
                }`}
              >
                <input
                  type="radio"
                  name={q.id}
                  checked={quizAnswers[q.id] === optIdx}
                  onChange={() => handleAnswer(q.id, optIdx)}
                  disabled={showResults}
                  className="w-4 h-4"
                />
                <span className="text-gray-700">{option}</span>
                {showResults && optIdx === q.correct && (
                  <CheckCircle className="ml-auto text-green-600" size={20} />
                )}
                {showResults && quizAnswers[q.id] === optIdx && optIdx !== q.correct && (
                  <XCircle className="ml-auto text-red-600" size={20} />
                )}
              </label>
            ))}
          </div>
          {showResults && (
            <div className="mt-3 p-3 bg-blue-50 border-l-4 border-blue-500 text-sm text-blue-900">
              <strong>Explanation:</strong> {q.explanation}
            </div>
          )}
        </div>
      ))}

      <div className="flex items-center justify-between">
        <button
          onClick={() => setShowResults(!showResults)}
          disabled={Object.keys(quizAnswers).length !== questions.length}
          className={`px-6 py-3 rounded-lg font-semibold transition-all ${
            Object.keys(quizAnswers).length === questions.length
              ? 'bg-blue-600 text-white hover:bg-blue-700 shadow-md'
              : 'bg-gray-300 text-gray-500 cursor-not-allowed'
          }`}
        >
          {showResults ? 'Hide Results' : 'Show Results'}
        </button>
        
        {showResults && (
          <div className={`text-right px-6 py-3 rounded-lg font-bold text-lg ${
            percentage >= 80 ? 'bg-green-100 text-green-800' :
            percentage >= 60 ? 'bg-yellow-100 text-yellow-800' :
            'bg-red-100 text-red-800'
          }`}>
            Score: {score}/{questions.length} ({percentage}%)
          </div>
        )}
      </div>

      {Object.keys(quizAnswers).length !== questions.length && (
        <p className="text-center text-gray-500 text-sm">
          Answer all questions to see your results
        </p>
      )}
    </div>
  );
};

export default KafgoRevision;