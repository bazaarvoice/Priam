package com.netflix.priam.identity;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.netflix.priam.utils.TokenManager;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DoubleRingTest extends InstanceTestUtils {

    @Test
    public void testDouble() throws Exception {
        createInstances();
        Multimap<Location, PriamInstance> originalInstances = getInstancesByLocation();
        // Should have been three distinct rings in the cluster
        assertEquals(3, originalInstances.keySet().size());
        int originalSize = originalInstances.get(location).size();
        assertEquals(9, originalSize);

        new DoubleRing(cassandraConfiguration, amazonConfiguration, instanceRegistry, tokenManager, location).doubleSlots();
        Multimap<Location, PriamInstance> doubledInstances = getInstancesByLocation();

        List<PriamInstance> doubled = Ordering.natural().immutableSortedCopy(doubledInstances.get(location));
        assertEquals(originalSize * 2, doubled.size());
        validate(doubled);

        // Verify instances in remote locations are unchanged
        for (Location remoteLocation : originalInstances.keySet()) {
            if (!location.equals(remoteLocation)) {
                assertEquals(originalInstances.get(remoteLocation), doubledInstances.get(remoteLocation));
            }
        }
    }

    private void validate(List<PriamInstance> doubled) {
        List<String> validator = Lists.newArrayList();
        for (int i = 0; i < doubled.size(); i++) {
            validator.add(tokenManager.createToken(i, doubled.size(), location));

        }

        int numZones = amazonConfiguration.getUsableAvailabilityZones().size();

        for (int i = 0; i < doubled.size(); i++) {
            PriamInstance ins = doubled.get(i);
            assertEquals(validator.get(i), ins.getToken());
            int id = ins.getId() - TokenManager.locationOffset(location);
            //System.out.println(ins);
            if (0 != id % 2) {
                assertEquals(ins.getInstanceId(), PriamInstance.NEW_INSTANCE_PLACEHOLDER_ID);
            }
            // Verify that instances are spread across AZs evenly and in sequence.
            assertEquals(ins.getAvailabilityZone(), doubled.get((i + numZones) % numZones).getAvailabilityZone());
        }
    }

    @Test
    public void testBR() throws Exception {
        createInstances();
        Multimap<Location, PriamInstance> originalInstances = getInstancesByLocation();
        int initialClusterSize = originalInstances.size();
        int initialRingSize = originalInstances.get(location).size();
        DoubleRing ring = new DoubleRing(cassandraConfiguration, amazonConfiguration, instanceRegistry, tokenManager, location);
        ring.backup();
        ring.doubleSlots();
        // Only the local ring will have doubled; remote rings will be unchanged
        assertEquals(initialClusterSize + initialRingSize, instanceRegistry.getAllIds(cassandraConfiguration.getClusterName()).size());
        ring.restore();
        assertEquals(initialClusterSize, instanceRegistry.getAllIds(cassandraConfiguration.getClusterName()).size());
    }

    private Multimap<Location, PriamInstance> getInstancesByLocation() {
        return Multimaps.index(instanceRegistry.getAllIds(cassandraConfiguration.getClusterName()), new Function<PriamInstance, Location>() {
            @Nullable
            @Override
            public Location apply(PriamInstance instance) {
                return instance.getLocation();
            }
        });
    }
}
