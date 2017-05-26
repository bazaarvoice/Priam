package com.netflix.priam;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.identity.ConfigFileLocation;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceRegistry;
import com.netflix.priam.identity.Location;
import com.netflix.priam.utils.FakeSleeper;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.TokenManager;
import com.netflix.priam.utils.TokenManagerProvider;
import org.junit.Ignore;

@Ignore
public class TestModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(CassandraConfiguration.class).toInstance(new TestCassandraConfiguration("fake-app"));
        bind(AmazonConfiguration.class).toInstance(new TestAmazonConfiguration("fake-app", "fake-region", "az1", "fakeInstance1"));
        bind(BackupConfiguration.class).toInstance(new TestBackupConfiguration());
        bind(IPriamInstanceRegistry.class).to(FakePriamInstanceRegistry.class).asEagerSingleton();
        bind(IMembership.class).toInstance(new FakeMembership(ImmutableList.of("fakeInstance1", "fakeInstance2", "fakeInstance3")));
        bind(AWSCredentialsProvider.class).toInstance(new StaticCredentialsProvider(new AnonymousAWSCredentials()));
        bind(TokenManager.class).toProvider(TokenManagerProvider.class);
        bind(Sleeper.class).to(FakeSleeper.class).asEagerSingleton();
        bind(Location.class).to(ConfigFileLocation.class).asEagerSingleton();
    }
}
