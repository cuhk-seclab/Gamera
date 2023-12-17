package ch.cypherchecker.memgraph;

import org.neo4j.driver.*;
import ch.cypherchecker.common.Connection;
import org.neo4j.driver.Record;

import java.util.*;
import java.util.stream.Collectors;

public class MemgraphConnection implements Connection {

    private Driver driver;

    @Override
    public void connect() throws Exception {
        driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("", ""));

    }

    public List<Map<String, Object>> execute(ch.cypherchecker.common.Query<MemgraphConnection> query) {
        List<Map<String, Object>> resultRows = new ArrayList<>();
        List<Record> resultRecords = new ArrayList<>();

        try (var session = driver.session()) {
            Result response = session.executeWrite(transaction -> {
               var execute = new Query(query.getQuery());
               var result = transaction.run(execute);
               return result;
            });
            resultRecords = response.list();
            System.out.println("Execute successfully. Check resultRecords:\n");

            int count = 0;
            for (Record record : resultRecords) {
                System.out.println(String.format("Count %s is: %s\n", count, record.toString()));
                count += 1;
            }
            // Align resultRows with response
            System.out.println("DEBUG");
        } catch (Exception e) {
            System.out.println("Execute with exception.");
//            e.printStackTrace();
        }

        return resultRows;
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }

    // Check Memgraph name indices support
    public Set<String> getIndexNames() {
        return Collections.emptySet();
    }
}
