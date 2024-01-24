# Gamera

## Overview

Gamera is a metamorphic testing approach and benchmark to uncover logic bugs in Graph Database Systems (GDBs). We design three classes of novel graph-aware Metamorphic Relations (MRs) based on the graph native structures. Gamera would generate a set of queries according to the graph-aware MRs to test diverse and complex GDB operations, and check whether the GDB query results conform to the chosen MRs.

We thoroughly evaluated the effectiveness of Gamera on seven widely-used GDBs, including two Cypher-based GDBs, i.e., Neo4j, RedisGraph, and five Gremlin-based GDBs, i.e., OrientDB, JanusGraph, HugeGraph, TinkerGraph, and ArcadeDB.

## Code Structure

The artifact contains two components, one for testing Cypher-based GDBs, and another for testing Gremlin-based GDBs. Both `CypherChecker` and `GremlinChecker` follow the testing workflow of Gamera, including graph schema and graph data generation, query generation based on graph-aware MRs, graph data mutation, query execution, and query result checking.

## Setup

### Requirements
- Java 8 or Java 17
- Maven 3 (e.g., Maven 3.8.6)
- Operating System: Mac, Linux

### Setup and Build

Follow the details of README in [CypherChecker](CypherChecker/README.md) and [GremlinChecker](GremlinChecker/README.md) respectively. You can reproduce the experiments.

## Found Bugs

Gamera in total detected 39 bugs and 5 new bugs from FalkorDB (, which is related to RedisGraph). All the results are listed here: [results.md](results.md).


## Publication
You can find more details in our VLDB 2024 paper:
[Testing Graph Database Systems via Graph-Aware Metamorphic Relations](https://www.vldb.org/pvldb/vol17/p836-zhuang.pdf)
```
@article{zhuang2024gamera,
  title   = {Testing Graph Database Systems via Graph-aware Metamorphic Relations},
  author  = {Zhuang, Zeyang and Li, Penghui and Ma, Pingchuan and Meng, Wei and Wang, Shuai},
  journal = {Proceedings of the VLDB Endowment},
  volume  = {17},
  number  = {4},
  pages   = {836--848},
  year    = {2023},
  publisher = {VLDB Endowment}
}
```

## Contacts

- Zeyang Zhuang (zyzhuang22@cse.cuhk.edu.hk)
