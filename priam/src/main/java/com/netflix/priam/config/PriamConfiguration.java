package com.netflix.priam.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Singleton;
import io.dropwizard.Configuration;
import io.dropwizard.client.JerseyClientConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;

@Singleton
public class PriamConfiguration extends Configuration {
    @JsonProperty
    @NotNull
    @Valid
    private CassandraConfiguration cassandra = new CassandraConfiguration();

    @JsonProperty
    @Valid
    private AmazonConfiguration amazon = new AmazonConfiguration();

    @JsonProperty
    @NotNull
    @Valid
    private BackupConfiguration backup = new BackupConfiguration();

    @JsonProperty
    @NotNull
    @Valid
    private ZooKeeperConfiguration zooKeeper = new ZooKeeperConfiguration();

    @JsonProperty
    @NotNull
    @Valid
    private JerseyClientConfiguration httpClient = new JerseyClientConfiguration();

    @JsonProperty
    @NotNull
    @Valid
    private MonitoringConfiguration monitoring = new MonitoringConfiguration();

    @JsonProperty
    private List<String> ostrichServiceNames = Collections.emptyList();

    @JsonProperty
    private int jvmMutexPort = 8086;

    public CassandraConfiguration getCassandraConfiguration() {
        return cassandra;
    }

    public AmazonConfiguration getAmazonConfiguration() {
        return amazon;
    }

    public BackupConfiguration getBackupConfiguration() {
        return backup;
    }

    public ZooKeeperConfiguration getZooKeeperConfiguration() {
        return zooKeeper;
    }

    public JerseyClientConfiguration getHttpClientConfiguration() {
        return httpClient;
    }

    public MonitoringConfiguration getMonitoringConfiguration() {
        return monitoring;
    }

    public int getJvmMutexPort() {
        return jvmMutexPort;
    }

    public List<String> getOstrichServiceNames() {
        return ostrichServiceNames;
    }
}
