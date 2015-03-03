package com.netflix.priam;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.noderepair.NodeRepair;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.utils.TuneCassandra;
import io.dropwizard.lifecycle.Managed;

/**
 * Start all tasks here - Property update task - Node repair
 */
@Singleton
public class PriamServer implements Managed {
    private final PriamScheduler scheduler;
    private final CassandraConfiguration cassandraConfig;
    private final NodeRepair nodeRepair;
    private final InstanceIdentity id;
    private final ICassandraProcess cassProcess;

    @Inject
    public PriamServer(CassandraConfiguration cassandraConfig,
                       PriamScheduler scheduler,
                       NodeRepair nodeRepair,
                       InstanceIdentity id,
                       ICassandraProcess cassProcess) {
        this.cassandraConfig = cassandraConfig;
        this.scheduler = scheduler;
        this.nodeRepair = nodeRepair;
        this.id = id;
        this.cassProcess = cassProcess;
    }

    public InstanceIdentity getInstanceIdentity() {
        return id;
    }

    @Override
    public void start() throws Exception {
        if (id.getInstance().isOutOfService()) {
            return;
        }

        // Start the quartz job scheduler.
        scheduler.start();

        // Run the task to tune Cassandra, write cassandra.yaml.
        scheduler.runTaskNow(TuneCassandra.class);

        // Start cassandra.
        cassProcess.start(true);

        // Schedule Node Repair
        if (cassandraConfig.isNodeRepairEnabled()) {
            scheduler.addTask(nodeRepair.getJobDetail(), nodeRepair.getCronTimeTrigger());
        }
    }

    @Override
    public void stop() throws Exception {
        scheduler.shutdown();
    }
}
