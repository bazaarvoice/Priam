package com.netflix.priam.utils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.scheduler.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Singleton
public class TuneCassandra extends Task {
    private static final Logger LOGGER = LoggerFactory.getLogger(TuneCassandra.class);

    private final CassandraConfiguration cassandraConfiguration;
    private final AmazonConfiguration amazonConfiguration;
    private final CassandraTuner tuner;

    @Inject
    public TuneCassandra(CassandraConfiguration cassandraConfiguration,
                         AmazonConfiguration amazonConfiguration,
                         CassandraTuner tuner) {
        this.cassandraConfiguration = cassandraConfiguration;
        this.amazonConfiguration = amazonConfiguration;
        this.tuner = tuner;
    }

    @Override
    public void execute() {
        boolean isDone = false;

        while (!isDone) {
            try {
                tuner.writeAllProperties(cassandraConfiguration.getYamlLocation(),
                        amazonConfiguration.getPrivateIP(),
                        cassandraConfiguration.getSeedProviderClassName());
                isDone = true;
            } catch (IOException e) {
                LOGGER.info("Fail writing cassandra.yml file. Retry again!");
            }
        }
    }

    @Override
    public String getName() {
        return "Tune-Cassandra";
    }

    public String getTriggerName() {
        return "tunecassandra-trigger";
    }
}
