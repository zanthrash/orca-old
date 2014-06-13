package com.netflix.spinnaker.orca.cassandra.config;

import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.spinnaker.kork.astyanax.AstyanaxKeyspaceFactory;
import com.netflix.spinnaker.kork.astyanax.DefaultAstyanaxKeyspaceFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass(AstyanaxConfiguration.class)
public class AstyanaxComponents {

    @Bean
    public AstyanaxConfiguration astyanaxConfiguration() {
        return new AstyanaxConfigurationImpl()
            .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
            .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
            .setCqlVersion("3.0.0")
            .setTargetCassandraVersion("1.2");
    }

    @Bean
    public ConnectionPoolMonitor connectionPoolMonitor() {
        return new CountingConnectionPoolMonitor();
    }

    @Bean
    public ConnectionPoolConfiguration connectionPoolConfiguration(@Value("9160") int port, @Value("127.0.0.1") String seeds, @Value("3") int maxConns) {
        return new ConnectionPoolConfigurationImpl("cpConfig").setPort(port).setSeeds(seeds).setMaxConns(maxConns);
    }

    @Bean
    public AstyanaxKeyspaceFactory keyspaceFactory(AstyanaxConfiguration config,
                                                   ConnectionPoolConfiguration poolConfig,
                                                   ConnectionPoolMonitor poolMonitor) {
        return new DefaultAstyanaxKeyspaceFactory(config, poolConfig, poolMonitor);
    }

//    @Bean
//    public EmbeddedCassandraRunner embeddedCassandra(Keyspace keyspace, @Value("9160") int port, @Value("127.0.0.1") String host) {
//        return new EmbeddedCassandraRunner(keyspace, port, host);
//    }

}
