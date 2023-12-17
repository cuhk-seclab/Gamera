package org.gdbtesting;

public interface GraphDBConnection extends AutoCloseable {

    String getDatabaseVersion() throws Exception;

}
