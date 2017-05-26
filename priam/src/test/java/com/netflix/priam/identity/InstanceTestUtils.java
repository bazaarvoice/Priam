package com.netflix.priam.identity;

import com.google.common.base.Strings;
import com.netflix.priam.FakeMembership;
import com.netflix.priam.FakePriamInstanceRegistry;
import com.netflix.priam.TestAmazonConfiguration;
import com.netflix.priam.TestBackupConfiguration;
import com.netflix.priam.TestCassandraConfiguration;
import com.netflix.priam.utils.BigIntegerTokenManager;
import com.netflix.priam.utils.FakeSleeper;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.TokenManager;
import org.junit.Before;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.List;

@Ignore
public abstract class InstanceTestUtils {

    List<String> instances = new ArrayList<>();
    IMembership membership;
    TestCassandraConfiguration cassandraConfiguration;
    TestAmazonConfiguration amazonConfiguration;
    TestBackupConfiguration backupConfiguration;
    IPriamInstanceRegistry instanceRegistry;
    InstanceIdentity identity;
    TokenManager tokenManager;
    Sleeper sleeper;
    Location location;

    @Before
    public void setup() {
        instances.add("fakeinstance1");
        instances.add("fakeinstance2");
        instances.add("fakeinstance3");
        instances.add("fakeinstance4");
        instances.add("fakeinstance5");
        instances.add("fakeinstance6");
        instances.add("fakeinstance7");
        instances.add("fakeinstance8");
        instances.add("fakeinstance9");

        membership = new FakeMembership(instances);
        cassandraConfiguration = new TestCassandraConfiguration("fake-app");
        amazonConfiguration = new TestAmazonConfiguration("fake-app", "fake", "az1", "fakeinstance1");
        backupConfiguration = new TestBackupConfiguration();
        location = new SimpleLocation(amazonConfiguration.getRegionName(), Strings.nullToEmpty(cassandraConfiguration.getDataCenterSuffix()));
        instanceRegistry = new FakePriamInstanceRegistry(location);
        tokenManager = BigIntegerTokenManager.forRandomPartitioner();
        sleeper = new FakeSleeper();
    }

    public void createInstances() throws Exception {
        createInstanceIdentity("az1", "fakeinstance1");
        createInstanceIdentity("az1", "fakeinstance2");
        createInstanceIdentity("az1", "fakeinstance3");
        // try next region
        createInstanceIdentity("az2", "fakeinstance4");
        createInstanceIdentity("az2", "fakeinstance5");
        createInstanceIdentity("az2", "fakeinstance6");
        // next region
        createInstanceIdentity("az3", "fakeinstance7");
        createInstanceIdentity("az3", "fakeinstance8");
        createInstanceIdentity("az3", "fakeinstance9");

        // Additionally simulate rings with the same configuration in different data centers in the same cluster with:
        // 1) different region
        // 2) same region, different data center suffix
        Location altRegionLocation = new SimpleLocation("fake-remote", "");
        Location altSuffixLocation = new SimpleLocation("fake", "alt");

        for (int i=0; i < 2; i++) {
            String azPrefix = i == 0 ? "az" : "bz";
            Location location = i == 0 ? altRegionLocation : altSuffixLocation;
            int slot = 0;
            int idOffset = TokenManager.locationOffset(location);

            for (int az=1; az <= 3; az++) {
                for (int c=0; c < 3; c++) {
                    String instanceId = "fake-nonlocal-instance-" + c + az;
                    String azName = azPrefix + az;
                    String token = tokenManager.createToken(slot, 3, 3, location);
                    PriamInstance instance = PriamInstance.from("fake-app", slot + idOffset, instanceId, instanceId, instanceId, azName, null, token, location);
                    instanceRegistry.update(instance);
                    slot += 1;
                }
            }
        }
    }

    protected InstanceIdentity createInstanceIdentity(String zone, String instanceId) throws Exception {
        amazonConfiguration.setAvailabilityZone(zone);
        amazonConfiguration.setInstanceID(instanceId);
        amazonConfiguration.setPrivateHostName(instanceId);
        return new InstanceIdentity(cassandraConfiguration, amazonConfiguration, instanceRegistry, membership, tokenManager, sleeper, location);
    }
}
