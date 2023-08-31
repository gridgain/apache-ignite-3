# Motivation
This document is just a sync point in the questions of table-node-zone-storage relations. We need to prepare the usable and simple enough approach for the storage configurations from the user point of view in the scope of zone-based collocation.

# Problem statement
At the moment, each table has it's own storage configuration, the same time zones know nothing about the storage configuration. Also, every node supports all types of storages, which it has in the classpath (actually now all nodes support all storages in the default build).

But we want to introduce 2 changes, which will affect this simple picture:
- Each node will have its own local storage configuration. So, any node will support any random list of storages, potentially.
- Partitions will be transformed from the table oriented to the zone's one. It means, that
  - one zone partition will host multiple table partitions from this zone by design, to support the collocation for tables inside the one zone
  -> affinity function calculate the one affinity for multiple partitions of different tables. But different tables can have different storage types
  -> we need to find the way to choose the suitable list of nodes, which can host tables with the different storages. Take in mind - that at any moment, user can add the table with any storage to the zone.

Let's take some examples to understand the current issues better, in all examples we will assume, that we have one zone, which included all nodes in cluster: Also, the number of replicas=1.
```sql
create zone z1 with replicas=1, partitions=1;
```

Examples:
1. Example, where obviously we can't collocate these tables
```
nodes: A(aipersist), B(rocksdb)
tables to create: t(rocksdb), t(aipersist)
```
2. Example, where we can collocate tables theoretically, but the affinity function can calculate distribution in this way, that in fact, tables cannot be collocated
```
nodes: A(aipersist, rocksdb), B(rocksdb)
tables to create: t(rocksdb), t(aipersist)
```

To handle this kind of issues we can offer two solutions:
- Introduce the special checks during the zone lifecycle
- Add any type of additional filter to support in the zone only the nodes with the predefined list of storages

# Approach 1. Special checks during the zone lifecycle.
At the moment, on the `create table ...` we return control to the user, immediately when appropriate configuration change applied (technically we are doing some another actions before, but doesn't matter for the current discussion).

So, let's think how example 1 can be resolved step by step:
- on creating the zone affinity function `aff(replicas=1, partitions=1, nodes=[A, B])` calculate the nodes `[A]` for the partition 1 distribution
- on creating the table `t(rocksdb)` at the moment we will receive success from the API, but the table will never receive enough node to init the replicas, because current zone distribution doesn't have rocksdb nodes at all.

To resolve this issue we can:
- Introduce the special check on table creation before the user receive control back:
  - Check if all zone data nodes have the appropriate storage. But we need to find the way how this check can be implemented atomically with the table creation operation.

TODO: Alexey, please fill the gap - how this check must be implemented for all invariants from examples (1,2)

# Approach 2. Zone filter for the storage types
Due to the fact, that nodes will have the local configuration for supported storage types, we can lift this fact as a node attribute to the node attributes list. But the storage types - is not the whole story. Also, we need to have an ability to set the detailed storage configuration to check if table really can operate on this node.

Let's extend our examples with detailed configurations for node storages:
```
A:
rocksDb:
  flushDelayMillis: 1000
  regions:
    lruRegion:
      cache: lru
      size: 256
    clockRegion:
      cache: clock
      size: 512
      
aipersist:
  checkpoint:
    checkpointDelayMillis: 100
  regions:
    segmentedRegion:
      replacementMode: SEGMENTED_LRU
    clockRegion:
      replacementMode: CLOCK
      
B:
aipersist:
```

After that, we can use this attribute as a required part of zone filter, like:
```sql
create zone z1 with replica=1, partitions=1, store = '{rocksDb: {regions.lruRegion.size: 256}}, aipersist: regions. segmentedRegion.replacementMode: SEGMENTED_LRU}}}}'
```

By this query user obviously define the zone, which support the storage types `aipersist` and `rocksdb` with the defined params. Therefore each node in this zone support **both** storage types.

So, if the user will create the table with storage `aipersist` with particular options, he/she just need to check if the target zone has this storage configuration in the `store` attribute.

However, the disadvantage of this approach - we introduce additional filter by the store attribute as a first citizen. It looks awkward, because we already have another standard `data_nodes_filter` filters for nodes.

Let's extend our examples and make some more to show this approach in practice
```
create zone z1 with replica=1, partitions=1, store = '{rocksDb: {regions.lruRegion.size: 256}}, aipersist: regions. segmentedRegion.replacementMode: SEGMENTED_LRU}}}}' // only node A is suitable for this zone, because node B has no aipersist at all

create table t with primary_storage='rocksdb' using zone=z1; // table successfully created
create table t with primary_storage='aipersist' using zone=z1; // table successfully created
create table t with primary_storage='{rocksdb.flushDelayMillies: 1000}' using zone=z1; // table successfully created

create table t with primary_storage='{rocksdb.flushDelayMillies: 2000}' using zone=z1; // table creation failed
create table t with primary_storage='{rocksdb.regions: {lfuRegion: {}}}' using zone=z1; // table creation failed
```
So, the general rules for table storage attributes and the zone store filter at the same time:
- if any params is omitted - any value of params is ok
- named lists is a weakness point, because if we want to check that entity with this name just exists we should create the syntax for it, like  `{rocksdb.regions: {lfuRegion: {}}}`
