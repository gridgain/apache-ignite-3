Nodes configuration
```
A:
rocksDb:
  flushDelayMillis: 1000
  instances:
    lru:
      cache: lru
      size: 256
    clock:
      cache: clock
      size: 512
      
aipersist:
  checkpoint:
    checkpointDelayMillis: 100
  regions:
    segmented:
      replacementMode: SEGMENTED_LRU
    clock:
      replacementMode: CLOCK
      
B:
aipersist:
  regions:
    clock:
      replacementMode: CLOCK 
```

# 1. Tablespaces
```
create tablespace rocks_lru storage 'rocksdb' instance 'lru';

create zone z1 with replica=1, partitions=1, tablespaces (rocks_lru), data_nodes_filter='$[?(@.rack == "r1")]';

create table t tablespace rocks_lru using zone=z1;
```

Implementation notes:
- We will need to clarify which params for different storages can be used for tablespace create (region for aipersist, instance for rocksdb and etc.).
- Node storage configurations will not lifted as usual node attributes, but can be lifted as a special internal attributes, suitable only for tablespaces.

# 2. Storage profiles and separate  storages filter for zone
Prerequisites:
- Each storage has a unified `profile` attribute. For example rocks instances renamed to profiles, aipersist regions renamed the same way and etc.
```
create zone z1 with replica=1, partitions=1, storage_profiles='{rocksDb = [lru], aipersist = [segmented]}', data_nodes_filter='$[?(@.rack == "r1")]';

create table t with storage_profile='rocksDb.lru' using zone=z1;
```

Implementation notes:
- We need to clarify the trivial language for zone description from the point of view supported storage profiles. Here I just use simple json.
- Node storage configurations will not be lifted as usual node attributes.

# 3. Storage profiles and rework of zone filter 'data_nodes_filter'
Prerequisites:
- Each storage has a unified `profile` attribute. For example rocks instances renamed to profiles, aipersist regions renamed the same way and etc.

```
create zone z1 with replica=1, partitions=1, data_nodes_filter='[+rack=r1, +storage_profile=rocksDb.lru, +storage_profile=rocksDb.lfu]';

create table t with storage_profile='rocksDb.lru' using zone=z1;
```

Implementation notes:
- We need to clarify the another language for general zone filters. The current one 'svistnut' from one another kindred database.
- Node storage configurations will not be lifted as usual node attributes.

