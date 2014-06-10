package com.netflix.spinnaker.orca.cassandra.config;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.test.EmbeddedCassandra;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class EmbeddedCassandraRunner {
    private static final Logger log = LoggerFactory.getLogger(EmbeddedCassandraRunner.class);

    private final Keyspace keyspace;
    private final int port;
    private final String host;
    private EmbeddedCassandra embeddedCassandra;

    public EmbeddedCassandraRunner(Keyspace keyspace, int port, String host) {
        this.keyspace = keyspace;
        this.port = port;
        this.host = host;
    }

    @PostConstruct
    public void init() throws Exception {
        embeddedCassandra = new EmbeddedCassandra("build/cassandra-test");
        embeddedCassandra.start();
        log.info("Waiting for Embedded Cassandra instance...");
        Future<Object> waitForCassandraFuture = Executors.newSingleThreadExecutor().submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                while (true) {
                    try {
                        new Socket(host, port);
                        break;
                    } catch (IOException ignore) {
                        Thread.sleep(1000);
                    }
                }
                return null;
            }
        });
        waitForCassandraFuture.get(60, TimeUnit.SECONDS);
        log.info("Embedded cassandra started.");
        try {
            keyspace.describeKeyspace();
        } catch (ConnectionException e) {
            Map<String, Object> options = ImmutableMap.<String, Object>builder()
                .put("name", keyspace.getKeyspaceName())
                .put("strategy_class", "SimpleStrategy")
                .put("strategy_options", ImmutableMap.of("replication_factor", "1"))
                .build();
            keyspace.createKeyspace(options);
        }
    }

    @PreDestroy
    public void destroy() throws Exception {
        embeddedCassandra.stop();
    }
}
