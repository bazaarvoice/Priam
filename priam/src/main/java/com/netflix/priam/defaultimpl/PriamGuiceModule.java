/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.defaultimpl;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.netflix.priam.aws.AWSMembership;
import com.netflix.priam.aws.SDBInstanceRegistry;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.config.MonitoringConfiguration;
import com.netflix.priam.config.PriamConfiguration;
import com.netflix.priam.config.ZooKeeperConfiguration;
import com.netflix.priam.dropwizard.Port;
import com.netflix.priam.dropwizard.managers.ServiceRegistryManager;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceRegistry;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.ThreadSleeper;
import com.netflix.priam.utils.TokenManager;
import com.netflix.priam.utils.TokenManagerProvider;
import com.sun.jersey.api.client.Client;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.server.SimpleServerFactory;
import io.dropwizard.setup.Environment;
import org.apache.curator.framework.CuratorFramework;

import java.util.Collections;
import java.util.List;

public class PriamGuiceModule extends AbstractModule {
    private final PriamConfiguration priamConfiguration;
    private final Environment environment;

    public PriamGuiceModule(PriamConfiguration priamConfiguration, Environment environment) {
        this.priamConfiguration = priamConfiguration;
        this.environment = environment;
    }

    @Override
    protected void configure() {
        // Configuration bindings
        bind(PriamConfiguration.class).toInstance(priamConfiguration);
        bind(CassandraConfiguration.class).toInstance(priamConfiguration.getCassandraConfiguration());
        bind(AmazonConfiguration.class).toInstance(priamConfiguration.getAmazonConfiguration());
        bind(BackupConfiguration.class).toInstance(priamConfiguration.getBackupConfiguration());
        bind(ZooKeeperConfiguration.class).toInstance(priamConfiguration.getZooKeeperConfiguration());
        bind(MonitoringConfiguration.class).toInstance(priamConfiguration.getMonitoringConfiguration());

        bind(IPriamInstanceRegistry.class).to(SDBInstanceRegistry.class).asEagerSingleton();
        bind(IMembership.class).to(AWSMembership.class).asEagerSingleton();
        bind(AWSCredentialsProvider.class).to(DefaultAWSCredentialsProviderChain.class).asEagerSingleton();

        bind(TokenManager.class).toProvider(TokenManagerProvider.class);
        bind(Sleeper.class).to(ThreadSleeper.class).asEagerSingleton();

        bind(ServiceRegistryManager.class).asEagerSingleton();
        bind(MetricRegistry.class).toInstance(environment.metrics());
    }

    @Provides
    @Singleton
    Optional<CuratorFramework> provideCurator() {
        ZooKeeperConfiguration zkConfiguration = priamConfiguration.getZooKeeperConfiguration();
        if (!zkConfiguration.isEnabled()) {
            return Optional.absent();
        }
        CuratorFramework curator = zkConfiguration.newManagedCurator(environment);
        curator.start();
        return Optional.of(curator);
    }

    @Provides
    @Singleton
    Client provideJerseyClient(MetricRegistry metricRegistry) {
        return new JerseyClientBuilder(metricRegistry)
                .using(priamConfiguration.getHttpClientConfiguration())
                .using(environment)
                .build("priam");
    }

    @Provides
    @Singleton
    @Port
    Integer providePort(PriamConfiguration configuration) {
        ServerFactory serverFactory = configuration.getServerFactory();

        // Our method for obtaining connector factories from the server factory varies depending on the latter's type
        List<ConnectorFactory> connectorFactories;
        if (serverFactory instanceof DefaultServerFactory) {
            connectorFactories = ((DefaultServerFactory) serverFactory).getApplicationConnectors();
        } else if (serverFactory instanceof SimpleServerFactory) {
            connectorFactories = Collections.singletonList(((SimpleServerFactory) serverFactory).getConnector());
        } else {
            throw new IllegalStateException("Encountered an unexpected ServerFactory type");
        }

        // find the first connector that matches and return its port information (in practice there should
        // be one, and just one, match)
        for (ConnectorFactory connector : connectorFactories) {
            if (connector.getClass().isAssignableFrom(HttpConnectorFactory.class)) {
                HttpConnectorFactory httpConnectorFactory = ((HttpConnectorFactory) connector);
                return httpConnectorFactory.getPort();
            }
        }

        throw new IllegalStateException("Did not find a valid HttpConnector for the server");
    }
}
