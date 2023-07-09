# Motivation

Let's describe the current naive architecture at the moment by the example.
- We have a cluster with the `type=OLTP` nodes `(A, B, C)` and with the `type=OLAP` nodes `(D, E, F)`.
- Also OLTP nodes have configured `aipersist` storages, OLAP nodes have `columnar` storages.
- Take the table `t1` for example
``` sql
create zone z1 with partitions=1, replicas=3, data_nodes_filter='$[?(@.type == "OLTP")]';
create zone z2 with partitions=1, replicas=3, data_nodes_filter='$[?(@.type == "OLAP")]';
create table t1 with primary_zone=z1, secondary_zone=z2, dataStorage='aipersist', secondaryStorage='columnar';
```
So, from the point of view partition replication groups we will have the following raft groups:
- <picture will be here> (f1-f2-f3-l1-l2-l3) where followers placed on the (A, B, C) and learners placed on the (D, E, F).
The main idea of the current solution is built around the requirement, that **primary and secondary zone has the same number of partitions**.

So, let's imagine that we want to create one more table `t2` in another primary zone and use the same secondary zone `z2`
``` sql
create zone z3 with partitions=1, replicas=3, data_nodes_filter='$[?(@.type == "OLTP")]';
create table t2 with primary_zone=z3, secondary_zone=z2, dataStorage='aipersist', secondaryStorage='columnar';
```
And here we will have the following issues:
- We can't choose another number of partitions for `z3` already
- z1, z2 and z3 will share the one raft group for every partition, which is even more rigid restriction. Let's call this case **transitive collocation**.

So, if we want to break these rules we should spam the zones scope with the artificial zones just to change the number of partitions and decouple raft groups of tables, which we don't want to collocate.

# Design 

In general we want to keep the idea about transparent data synchronization through the RAFT protocol by learners. But the different primary zones, which is using in tables with different primary zones must be not force to collocate.

To implement this approach we need to create the separate layer, which will reuse the same secondary data partitions by different learners of different groups.

## Step 1
Let's fix the issue with **transitive collocation** firstly, but for the simplicity leave the same number of partitions requirement.

So, let's imagine the following zone-tables distribution:

``` sql
create zone z1 with partitions=1, replicas=1, data_nodes_filter='$[?(@.type == "OLTP")]';
create zone z2 with partitions=1, replicas=1, data_nodes_filter='$[?(@.type == "OLTP")]';
create zone z3 with partitions=1, replicas=1, data_nodes_filter='$[?(@.type == "OLAP")]';
create table t1 with primary_zone=z1, secondary_zone=z3, dataStorage='aipersist', secondaryStorage='columnar';
create table t2 with primary_zone=z1, secondary_zone=z3, dataStorage='aipersist', secondaryStorage='columnar';
```
Again, for the simplicity we will review firstly the replicas=1 and later will develop our thoughts ouf of this restriction.

So, as a result we will have the following structure from the RAFT groups point of view (jamboard 2). Where L11 is the learner of (z1, p1) raft group and L21 is the learner of (z2, p1) raft group. Both learner write the data to partition `p1` of the secondary zone `z3`.

In that place we need to update the interfaces for secondary partition storage to support the partitions from different tables inside the one secondary storage partitions:
- Every secondary storage partition learner has the separate `SecondaryPartitonListener` with it's own `SecondaryStorage`
> Questions
>
> How we will distribute this listeners on the cluster? Look at the `AffinityUtils#calculateAssignments` and the `withSecondary` flag
- `SecondaryStorage` has the API, which operates on the rows for the partition of any one table. We need to enrich this API to store the rows from the separate tables.
```
SecondaryStorage:
-void writeBatch(int tableId, Set<BinaryRowAndRowId> rows, HybridTimestamp timestamp)
+void writeBatch(int tableId, Set<BinaryRowAndRowId> rows, HybridTimestamp timestamp)

-BinaryRow read(BinaryRow key, HybridTimestamp timestamp)
+BinaryRow read(int tableId, BinaryRow key, HybridTimestamp timestamp)

-Cursor<BinaryRow> scan(HybridTimestamp timestamp)
+Cursor<BinaryRow> scan(int tableId, HybridTimestamp timestamp)

-Cursor<BinaryRow> scan(HybridTimestamp timestamp, @Nullable BitSet columnsToInclude)
+Cursor<BinaryRow> scan(int tableId, HybridTimestamp timestamp, @Nullable BitSet columnsToInclude)

-Cursor<BinaryRow> scanWithOperation(HybridTimestamp timestamp, StorageOperation operation) throws StorageException;
+Cursor<BinaryRow> scanWithOperation(int TableId, HybridTimestamp timestamp, StorageOperation operation) throws StorageException;
```
> Questions
> 
> Do we really need to have the tableId for scans?

The same trick should be implemented for the `UpdateStorage` which persists the uncommitted transactions per (table, partition) at the moment
```
-void onNewWrite(UUID txId, RowId rowId, BinaryRow row);
+void onNewWrite(int tableId, UUID txId, RowId rowId, BinaryRow row);

void onTransactionAborted(UUID txId);

void onTransactionCommitted(UUID txId, HybridTimestamp commitTimestamp);

void dropTransactionData(HybridTimestamp upperBound);

void dropTransactionData(Collection<TransactionInfo> ids);

long lastAppliedIndex();

long lastAppliedTerm();

void lastApplied(long lastAppliedIndex, long lastAppliedTerm);

void updateConfiguration(long lastAppliedIndex, long lastAppliedTerm, byte[] configuration);

long persistedIndex();

byte @Nullable [] configuration();

Cursor<TransactionInfo> getCommittedTransactionIds(HybridTimestamp upperBound);

Cursor<BinaryRowAndRowId> getTransactionData(UUID txId);

CompletableFuture<Void> flush();
```
> Questions
> 
> What about the last* and other methods without diffs? should they be tableId-aware also? flush?

What about the opposite part, reading from this storage, we also need to update the `PartitionReplicaListenerWithSecondaryStorage`
- At the current time is has the hard coupling with the table replica in general, despite of the plans under the closed GG-35623. So, we need to create the separate `SecondaryReplicaListener` which will be decoupled from the replicas of primary zone at all.
> Questions
> 
> At the moment the routing between the primary and secondary Replicas based on the `request.usePrimary()` check inside the PartitionReplicaListenerWithSecondaryStorage, but how and where should we route the messages after the listeners separation?

- Each `SecondaryReplicaListeter` represents the full (table, partition) mapping. So, when this replica call the underlying secondary partition storage to receive the data it must provide the appropriate tableId as well. (see jamboard 3)
> TODO
> 
> The example with replicas > 2 for secondary zone must be provided

- The actual creation of the `SecondaryPartitionListener/SecondaryReplicaListener`.

## Step 2
So, after the step 1 implemented we already have the separate `SecondaryPartitionListener` and `SecondaryReplicaListener` for each (table, partition) pair and the issue with **transitive collocation**.
The next our goal is to support the primary-secondary zone connection between the zones with different number of partitions.

For example, we let's take:
``` sql
create zone z1 with partitions=1, replicas=1, data_nodes_filter='$[?(@.type == "OLTP")]';
create zone z2 with partitions=3, replicas=1, data_nodes_filter='$[?(@.type == "OLAP")]';
create table t1 with primary_zone=z1, secondary_zone=z3, dataStorage='aipersist', secondaryStorage='columnar';
```

First of all let's see at the data ingestion path for this type of distribution:
- Current `AffinityUtils.calculateAssignments` must be enriched by the logic which will really calculate assignments for the learners.
- Than every node, which receive the table created event and host the appropriate secondary zone must start appropriate `SecondaryPartitionListener/SecondaryReplicaListener`

The tricky point, that the one partition from the primary zone now ingest the data to the some secondary partitions (jamboard 4), so we need to:
- Filter on the learner part only needed rows
- Event better to introduce the algorithm skip these rows on the peers part

On the data read part it looks like enough to:
- Provide the separate way to lookup for needed partition by the `IgniteUtils.safeAbs(row.colocationHash()) % partitions` where partitions must be received from the **secondary zone** configuration







