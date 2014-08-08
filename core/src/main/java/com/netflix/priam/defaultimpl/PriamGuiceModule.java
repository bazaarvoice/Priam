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
import com.netflix.priam.dropwizard.managers.ServiceRegistryManager;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceRegistry;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.ThreadSleeper;
import com.netflix.priam.utils.TokenManager;
import com.netflix.priam.utils.TokenManagerProvider;
import com.sun.jersey.api.client.Client;
import com.yammer.dropwizard.client.JerseyClientBuilder;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.config.HttpConfiguration;
import org.apache.curator.framework.CuratorFramework;

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
        bind(HttpConfiguration.class).toInstance(priamConfiguration.getHttpConfiguration());
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
    Client provideJerseyClient() {
        return new JerseyClientBuilder()
                .using(priamConfiguration.getHttpClientConfiguration())
                .using(environment)
                .build();
    }
}
