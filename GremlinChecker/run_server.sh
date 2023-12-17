#!/usr/bin/env bash

bash /opt/janusgraph-0.6.2/bin/janusgraph-server.sh restart
bash /opt/apache-tinkerpop-gremlin-server-3.4.10/bin/gremlin-server.sh restart
bash /opt/hugegraph-0.12.0/bin/stop-hugegraph.sh
bash /opt/hugegraph-0.12.0/bin/init-store.sh
bash /opt/hugegraph-0.12.0/bin/start-hugegraph.sh
echo -n "Initiate gdb server and data."