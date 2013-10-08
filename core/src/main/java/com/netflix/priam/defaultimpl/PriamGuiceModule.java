package com.netflix.priam.defaultimpl;

import com.google.common.base.Optional;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.netflix.priam.ICredential;
import com.netflix.priam.aws.*;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.compress.SnappyCompression;
import com.netflix.priam.config.*;
import com.netflix.priam.dropwizard.managers.ServiceRegistryManager;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceRegistry;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.ThreadSleeper;
import com.netflix.priam.utils.TokenManager;
import com.netflix.priam.utils.TokenManagerProvider;
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

        bind(IPriamInstanceRegistry.class).to(SDBInstanceRegistry.class);
        bind(IMembership.class).to(AWSMembership.class);
        bind(ICredential.class).to(DefaultCredentials.class);

        if (priamConfiguration.getBackupConfiguration().getBackupTarget().equals("ebs")) {
            bind(IBackupFileSystem.class).to(EBSFileSystem.class);
            bind(AbstractBackupPath.class).to(EBSBackupPath.class);
        } else { // default to s3 backups
            bind(IBackupFileSystem.class).to(S3FileSystem.class);
            bind(AbstractBackupPath.class).to(S3BackupPath.class);
        }

        bind(ICompression.class).to(SnappyCompression.class);
        bind(TokenManager.class).toProvider(TokenManagerProvider.class);
        bind(Sleeper.class).to(ThreadSleeper.class);

        bind(ServiceRegistryManager.class).asEagerSingleton();
    }

    @Provides @Singleton
    Optional<CuratorFramework> provideCurator() {
        ZooKeeperConfiguration zkConfiguration = priamConfiguration.getZooKeeperConfiguration();
        if (!zkConfiguration.isEnabled()) {
            return Optional.absent();
        }
        CuratorFramework curator = zkConfiguration.newManagedCurator(environment);
        curator.start();
        return Optional.of(curator);
    }
}
