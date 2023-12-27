# Found Bugs

Gamera in total detected 44 bugs.

## Neo4j
1. Neo4j could not deal with correct path traversal results: https://github.com/neo4j/neo4j/issues/13057
2. Deleting node throws a QueryExecutionException: https://github.com/neo4j/neo4j/issues/13064
3. Incorrect path traversal results after adding a ring: https://github.com/neo4j/neo4j/issues/13058
4. Match query with add throws a QueryExecutionException: https://github.com/neo4j/neo4j/issues/13059
5. Match query with IsNaN throws a QueryExecutionException: https://github.com/neo4j/neo4j/issues/13060
6. Match query with size("") throws a QueryExecutionException: https://github.com/neo4j/neo4j/issues/13061
7. Match query with <> throws a QueryExecutionException: https://github.com/neo4j/neo4j/issues/13062
8. Match query throws a QueryExecutionException related to NullInNullOutExpression: https://github.com/neo4j/neo4j/issues/13063
9. Match query throws a QueryExecutionException: https://github.com/neo4j/neo4j/issues/13065
10. Match query selecting edge throws a QueryExecutionException: https://github.com/neo4j/neo4j/issues/13066

## RedisGraph
1. False results with nodes k-hop relations: https://github.com/RedisGraph/RedisGraph/issues/2934
2. Float overflow throws AssertionError: https://github.com/RedisGraph/RedisGraph/issues/2931
3. MATCH (n) RETURN n throws AssertionError: https://github.com/RedisGraph/RedisGraph/issues/2930
4. Throw exceptions for FOREACH: https://github.com/RedisGraph/RedisGraph/issues/2965
5. Query with OR logical operators returns false results: https://github.com/RedisGraph/RedisGraph/issues/2933
6. Query partitioning returns false results: https://github.com/RedisGraph/RedisGraph/issues/2932
7. Incorrect path traversal results after adding a node: https://github.com/RedisGraph/RedisGraph/issues/2964
8. Incorrect path traversal results after adding a ring: https://github.com/RedisGraph/RedisGraph/issues/2963
9. RedisGraph could not deal with correct path traversal results: https://github.com/RedisGraph/RedisGraph/issues/2929

## FalkorDB
1. MATCH (n) RETURN n throws AssertionError: https://github.com/FalkorDB/FalkorDB/issues/371
2. Incorrect path traversal results after adding a node: https://github.com/FalkorDB/FalkorDB/issues/380
3. Incorrect path traversal results after adding a ring: https://github.com/FalkorDB/FalkorDB/issues/379
4. False results with nodes k-hop relations: https://github.com/FalkorDB/FalkorDB/issues/372
5. RedisGraph could not deal with correct path traversal results: https://github.com/FalkorDB/FalkorDB/issues/370

## JanusGraph
1. Throw ExecutionException for E() syntax: https://lists.lfaidata.foundation/g/janusgraph-users/message/6724
2. Throw ClassCastException: https://lists.lfaidata.foundation/g/janusgraph-users/message/6718
3. Throw ExecutionException: https://lists.lfaidata.foundation/g/janusgraph-users/message/6717
4. Union of Nodes. Merged query using logical operator OR returns false results: https://lists.lfaidata.foundation/g/janusgraph-users/message/6716
5. Union of Edges: https://lists.lfaidata.foundation/g/janusgraph-users/message/6716

## TinkerGraph
1. Throw ExecutionException for E() syntax: https://issues.apache.org/jira/browse/TINKERPOP-2910
2. Throw ClassCastException: https://issues.apache.org/jira/browse/TINKERPOP-2909
3. Throw ExecutionException for property: https://issues.apache.org/jira/browse/TINKERPOP-2908
4. Throw ExecutionException for property: https://issues.apache.org/jira/browse/TINKERPOP-2907

## HugeGraph
1. Throw MissingMethodException for E() syntax: https://github.com/apache/incubator-hugegraph/issues/2171
2. New support for gte(text) syntax: https://github.com/apache/incubator-hugegraph/issues/2159
3. Throw ClassCastException: https://github.com/apache/incubator-hugegraph/issues/2170
4. Throw MultipleCompilationErrorsException: https://github.com/apache/incubator-hugegraph/issues/2169
5. Throw java.lang.NumberFormatException without outside(): https://github.com/apache/incubator-hugegraph/issues/216x8
6. Throw java.lang.IllegalArgumentException: https://github.com/apache/incubator-hugegraph/issues/2167
7. Query partitioning return false results: https://github.com/apache/incubator-hugegraph/issues/2157

## ArcadeDB
1. Throw ExecutionException for E() syntax: https://github.com/ArcadeData/arcadedb/issues/946

## OrientDB
1. Throw ExecutionException for E() syntax: https://github.com/orientechnologies/orientdb/issues/9945
2. OrientDB could not deal with correct path traversal results: https://github.com/orientechnologies/orientdb/issues/9937
3. Query partitioning return false results: https://github.com/orientechnologies/orientdb/issues/9942