# Apache Ignite YCSB module
Bindings for YCSB to run Ignite3 benchmarks

Working dir:

PROJECT_HOME/modules/ycsb

Main class: 
site.ycsb.Client

Params: 
-db site.ycsb.db.ignite3.IgniteClient -p hosts=127.0.0.1 -s -P ./workloads/workloadc -threads 4 -p dataintegrity=true -p operationcount=500000 -p recordcount=500000 -p disableFsync=true -p useEmbedded=true -load