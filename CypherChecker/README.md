# CypherChecker

CypherChecker is the component of Gamera for finding logic bugs in Cypher-based Graph Database Systems (GDBs).

## Getting Started

### Requirements
- Java 17
- Maven 3 (e.g., Maven 3.8.6)
- Operating System: Mac, Linux

### Setting GDBs
1. CypherChecker takes [Neo4j](https://neo4j.com/) and [RedisGraph](https://redis.io/docs/stack/graph/) as examples to detect logic bugs.
2. Download the GDBs from official websites.
3. Start up the GDBs.

### Running
1. In `src/main/java/ch/cypherchecker/Main.java` file, start the project.
2. The `log*` directory stores information related to the detected logic bugs and reproducible test cases.
