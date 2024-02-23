# Hitchhikers guide to the Ignite 3 threads

Which threads are passed by a 'request' made by a user.

This does not include background threads that are not included in the chain of threads servicing a user's request.

This is an attempt to use Sequence Diagrams where our components act as participants, and the messages they exchange are names of thread pools in which execution happens between (or inside) the components. The boundaries might not be exact, the main focus is to demonstrate thread switching.

If a pool has something in parentheses (like `partition-operations(N)`), this is to highlight that a switch happens to the same pool, but (most likely) to another thread of it.

Traces collected on Ignite 3 corresponding to commit https://github.com/apache/ignite-3/commit/bc0d6e919830b366ceb2cfa28d437d6d2bb95197

## Normal cases

Normal (or simple) cases are collected here. That is, no cases like 'a `metastorage-watch` thread is highjacked due to a Schema Sync/Primary await/Transactional locks wait/etc).

### Embedded mode

#### KV get (embedded mode) on partition colocated with current node

```mermaid
sequenceDiagram
participant View as KVView
participant InternalTable
participant RepService
participant MsgService
participant RepManager
participant PRL as PartitionReplicaListener

View ->> RepManager: User thread
RepManager ->> PRL: partition-operations
PRL ->> PRL: partition-operations
PRL ->> View: partition-operations
```

#### KV put (embedded mode) on partition colocated with current node

```mermaid
sequenceDiagram
participant View as *KVView
participant InternalTable
participant RepService
participant MsgService
participant RepManager
participant PRL as PartitionReplicaListener

View ->> RepManager: User thread
RepManager ->> ActionRequestProcessor: partition-operations
ActionRequestProcessor ->> NodeImpl: JRaft-Request-Processor
NodeImpl ->> LogManager: JRaft-NodeImpl-Disruptor
LogManager ->> PartitionListener: JRaft-LogManager-Disruptor
PartitionListener ->> PartitionListener: JRaft-FSMCaller-Disruptor
PartitionListener ->> PRL: JRaft-FSMCaller-Disruptor
PRL ->> PRL: partition-operations
PRL ->> View: partition-operations
```

#### SQL get by PK (embedded mode) on partition colocated with current node

```mermaid
sequenceDiagram
participant SQLProc as SqlQueryProc
participant ParserService
participant PrepareService
participant ExecutionService
participant KVGetPlan
participant InternalTable
participant RepService
participant MsgService
participant RepManager
participant PRL as PartitionReplicaListener

SQLProc ->> ParserService: User thread
ParserService ->> PrepareService: sql-execution-pool
PrepareService ->> PrepareService: sql-planning-pool(X)
PrepareService ->> KVGetPlan: sql-planning-pool(Y)
KVGetPlan ->> RepManager: tableManager-io
RepManager ->> PRL: partition-operations
PRL ->> PRL: partition-operations
PRL ->> SQLProc: partition-operations
```

#### SQL table scan (embedded mode) on partitions colocated with current node

```mermaid
sequenceDiagram
participant User
participant SQLProc as SqlQueryProc
participant ParserService
participant PrepareService
participant ExecutionService
participant MultiStepPlan
participant InternalTable
participant RepService
participant MsgService
participant RepManager
participant PRL as PartitionReplicaListener

User ->> ParserService: User thread
ParserService ->> PrepareService: sql-execution-pool
PrepareService ->> PrepareService: sql-planning-pool(X)
PrepareService ->> MultiStepPlan: sql-planning-pool(Y)
MultiStepPlan ->> RepManager: sql-execution-pool
RepManager ->> PRL: partition-operations(1)
PRL ->> PRL: partition-operations(1)
PRL ->> MultiStepPlan: partition-operations(1)
loop Once per each partition after first (each time on new thread in partition-operations)
  MultiStepPlan ->> RepManager: partition-operations(N-1)
  RepManager ->> PRL: partition-operations(N)
  PRL ->> PRL: partition-operations(N)
  PRL ->> MultiStepPlan: partition-operations(N)
end
MultiStepPlan ->> SQLProc: partition-operations(N)
SQLProc ->> User: sql-execution-pool
```

## Known anomalies

 1. In `*TableView`, after a Schema Sync execution may be switched to `metastorage-watch`
 2. Same thing in `SqlQueryProcessor`
 3. In `InternalTableImpl`, after waiting for a primary replica, execution may be switched to `metastorage-watch`
 4. In `PartitionReplicaListener`, after obtaining a transactional lock, execution might be switched to another thread of `partition-operations` (which is probably ok) or to the common FJP (which is not fine)

## Cases to cover

 * DMLs
 * DDLs?
 * Network communication
 * Non-embedded cases
