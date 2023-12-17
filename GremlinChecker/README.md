# GremlinChecker

GremlinChecker is the component of Gamera for finding logic bugs in Gremlin-based Graph Database Systems (GDBs).

## Getting Started

### Requirements
- Java 8
- Maven 3 (e.g., Maven 3.8.6)
- Operating System: Mac, Linux

### Setting GDBs
1. GremlinChecker takes [JanusGraph](https://janusgraph.org), [TinkerGraph](https://github.com/tinkerpop/blueprints/wiki/tinkergraph), [HugeGraph](https://hugegraph.github.io/hugegraph-doc/), [OrientDB](http://orientdb.org/) and [ArcadeDB](https://arcadedb.com/) as examples to detect logic bugs.
2. Download the GDBs from official websites. Specifically, find the `gremlin-server.yaml` in each GDB, and replace the port number in it following the `*.yaml` file in `conf` directory we offer. For example, the port number of JanusGraph is 8185.
3. Start up the GDBs.

### Running
1. In `src/main/java/org/gdbtesting/Starter.java` file, start the project.
2. The parameters in our `Starter.java` are as follows. You can modify and change these parameters as needed.
- QueryDepth, the max length of the query we generated, e.g., 5.
- VerMaxNum, the maximum number of the Vertex in the generated graph, e.g., 100.
- EdgeMaxNum, the maximum number of the Edge in the generated graph, e.g., 200.
- VerLabelNum, the maximum number of the vertex label in the generated graph, e.g., 10.
- EdgeLabelNum, the maximum number of the edge label in the generated graph, e.g., 20.
- QueryNum, the number of query generated in a test round, e.g., 1000.
3. The `log*` directory stores information related to the detected logic bugs and reproducible test cases.
