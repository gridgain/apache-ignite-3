# Motivation
This document is just a sync point in the questions of table-node-zone-storage relations. We need to prepare the usable and simple enough approach for the storage configurations from the user point of view in the scope of zone-based collocation.

# Problem statement
At the moment, each table has it's own storage configuration, the same time zones know nothing about the storage configuration. Also, every node supports all types of storages, which it has in the classpath (actually now all nodes support all storages in the default build).

But we want to introduce 2 changes, which will affect this simple picture:
- Each node will have its own local storage configuration. So, any node will support any random list of storages and its regions/instances, potentially.
- Partitions will be transformed from the table oriented to the zone's one. It means, that
  - one zone partition will host multiple table partitions from this zone by design, to support the collocation for tables inside the one zone
  - -> affinity function calculate the one affinity for multiple partitions of different tables. But different tables can have different storage types
  - -> we need to find the way to choose the suitable list of nodes, which can host tables with the different storages. Take in mind - that at any moment, user can add the table with any storage to the zone.

# Node configuration
Let's take some nodes `(A,B)` with storages configurations:
```
A:
rocksDb:
  flushDelayMillis: 1000
  profiles:
    lru:
      cache: lru
      size: 256
    clock:
      cache: clock
      size: 512
      
aipersist:
  checkpoint:
    checkpointDelayMillis: 100
  profiles:
    segmented:
      replacementMode: SEGMENTED_LRU
    clock:
      replacementMode: CLOCK
      
B:
aipersist:
  profiles:
    clock:
      replacementMode: CLOCK 
```

Profiles it the new primitive, which introduced to generalize the idea of different data spaces inside the one storage type. So, each storage has the number of profiles, which can define the concrete data spaces in appropriate way for this storage.

# Zone filter for the storage types and profiles
Due to the fact, that nodes will have the local configuration for supported storage types, we can **lift this fact as a node attribute to the node attributes list**. But the storage types - is not the whole story. Also, we need to have an ability to set the preferred data profiles, suitable for concrete tables.

After that, we can use this attribute as a required part of zone filter, like:
```sql
create zone z1 with replica=1, partitions=1, storages='{rocksDb.profiles = [lru], aipersist.profiles = [segmented]}'
```

By this query user obviously defines the zone, which support the storage types `aipersist` and `rocksdb` with the defined data profiles. Therefore each node in this zone support **both** `(storage, profile)` pairs. Because we need to have guarantee, that any node in the zone can host any table of the same zone.

So, if the user will create the table with `(aipersist, clock)`, they just need to check if the target zone has this pair configured in the `store` attribute.

Let's take some examples and to show this approach in practice
```
create zone z1 with replica=1, partitions=1, storages='{rocksDb.profiles = [lru], aipersist.profiles = [segmented]}'; // only node A 
// is suitable for this zone,
// because node B has no aipersist segmented region

create table t with storage='rocksdb', storage_profile='lru' using zone=z1; // table successfully created 
create table t with storage='aipersist', storage_profile='segmented' using zone=z1; // table successfully created

create table t with storage='rocksdb', storage_profile='clock' using zone=z1; // table creation failed.
// actually node A has this profile, but the zone filter can't guerantee, that this profile exists on all nodes.

create table t with storage='rocksdb', storage_profile='lfu' using zone=z1; // table creation failed

```

## Issues
Weak points of this design:
- Our zones already has the `data_nodes_filter` param, which filter the zones by the any valid JSONPath filter. But these filters have too flexible syntax, and we can write something like `$[?(@.storage == "aipersist" || @.storage == "rocksdb"]`, which means, that the zone will have the nodes with `aipersist` or `rocksdb` nodes. But we always want to have only the "**both**" modifier, to guarantee that the table can be deployed on any node from the zone. So that, we are introducing the separate filter for storages, which looks as a bad design.

## Other thoughts
We can try to introduce the `TABLESPACE` entity which will combine the storage and profile. This entity can be supported by the zone nodes and required by the tables. Example:

```sql
create tablespace rocks_lru storage 'rocksdb' profile 'lru';
    
create zone z1 with replica=1, partitions=1, tablespaces (rocks_lru);
    
create table t tablespace rocks_lru using zone=z1;
```
Moreover with `tablespace` we can abort the reuqirements to have the general `profile` attribute for all storages, and `aipersist` can have `regions`, but rocksdb can have `instances` for example. These differences will be hidden under the `tablespace` entity and showed only on `tablespace` creation:
```sql
create tablespace rocks_lru storage 'rocksdb' instance 'lru';
// but
create tablespace aipersist_lru storage 'aipersist' region 'lru';
```
