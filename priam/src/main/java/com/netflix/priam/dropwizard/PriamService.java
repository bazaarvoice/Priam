package com.netflix.priam.dropwizard;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.PriamServer;
import com.netflix.priam.config.PriamConfiguration;
import com.netflix.priam.defaultimpl.PriamGuiceModule;
import com.netflix.priam.dropwizard.managers.ManagedCloseable;
import com.netflix.priam.dropwizard.managers.ServiceMonitorManager;
import com.netflix.priam.dropwizard.managers.ServiceRegistryManager;
import com.netflix.priam.resources.CassandraAdminResource;
import com.netflix.priam.resources.CassandraConfigResource;
import com.netflix.priam.resources.MonitoringEnablementResource;
import com.netflix.priam.resources.PriamInstanceResource;
import com.netflix.priam.tools.CopyInstanceData;
import com.netflix.priam.tools.DeleteInstanceData;
import com.netflix.priam.tools.ListClusters;
import com.netflix.priam.tools.ListInstanceData;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PriamService extends Application<PriamConfiguration> {
    protected static final Logger logger = LoggerFactory.getLogger(PriamService.class);

    public static void main(String[] args) throws Exception {
        new PriamService().run(args);
    }

    @Override
    public void initialize(Bootstrap<PriamConfiguration> bootstrap) {
        bootstrap.addCommand(new ListClusters());
        bootstrap.addCommand(new ListInstanceData());
        bootstrap.addCommand(new CopyInstanceData());
        bootstrap.addCommand(new DeleteInstanceData());
    }

    @Override
    public void run(PriamConfiguration config, Environment environment) throws Exception {
        // Protect from running multiple copies of Priam at the same time.  Jetty will enforce this because only one
        // instance can listen on 8080, but that check doesn't occur until the end of initialization which is too late.
        environment.lifecycle().manage(new ManagedCloseable(new JvmMutex(config.getJvmMutexPort())));

        // Don't ping www.terracotta.org on startup (Quartz).
        System.setProperty("org.terracotta.quartz.skipUpdateCheck", "true");

        Injector injector = Guice.createInjector(new PriamGuiceModule(config, environment));
        try {
            environment.lifecycle().manage(injector.getInstance(PriamServer.class));
            environment.lifecycle().manage(injector.getInstance(ServiceRegistryManager.class));
            environment.lifecycle().manage(injector.getInstance(ServiceMonitorManager.class));

            environment.jersey().register(injector.getInstance(CassandraAdminResource.class));
            environment.jersey().register(injector.getInstance(CassandraConfigResource.class));
            environment.jersey().register(injector.getInstance(PriamInstanceResource.class));
            environment.jersey().register(injector.getInstance(MonitoringEnablementResource.class));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
