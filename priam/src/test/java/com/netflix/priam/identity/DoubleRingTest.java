package com.netflix.priam.identity;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.netflix.priam.utils.TokenManager;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class DoubleRingTest extends InstanceTestUtils {

    @Test
    public void testDouble() throws Exception {
        createInstances();
        int originalSize = instanceRegistry.getAllIds(cassandraConfiguration.getClusterName()).size();
        new DoubleRing(cassandraConfiguration, amazonConfiguration, instanceRegistry, tokenManager).doubleSlots();
        List<PriamInstance> doubled = instanceRegistry.getAllIds(cassandraConfiguration.getClusterName());
        doubled = Ordering.natural().immutableSortedCopy(doubled);

        assertEquals(originalSize * 2, doubled.size());
        validate(doubled);
    }

    private void validate(List<PriamInstance> doubled) {
        List<String> validator = Lists.newArrayList();
        for (int i = 0; i < doubled.size(); i++) {
            validator.add(tokenManager.createToken(i, doubled.size(), amazonConfiguration.getRegionName()));

        }

        int numZones = amazonConfiguration.getUsableAvailabilityZones().size();

        for (int i = 0; i < doubled.size(); i++) {
            PriamInstance ins = doubled.get(i);
            assertEquals(validator.get(i), ins.getToken());
            int id = ins.getId() - TokenManager.regionOffset(amazonConfiguration.getRegionName());
            //System.out.println(ins);
            if (0 != id % 2) {
                assertEquals(ins.getInstanceId(), "new_slot");
            }
            // Verify that instances are spread across AZs evenly and in sequence.
            assertEquals(ins.getAvailabilityZone(), doubled.get((i + numZones) % numZones).getAvailabilityZone());
        }
    }

    @Test
    public void testBR() throws Exception {
        createInstances();
        int intialSize = instanceRegistry.getAllIds(cassandraConfiguration.getClusterName()).size();
        DoubleRing ring = new DoubleRing(cassandraConfiguration, amazonConfiguration, instanceRegistry, tokenManager);
        ring.backup();
        ring.doubleSlots();
        assertEquals(intialSize * 2, instanceRegistry.getAllIds(cassandraConfiguration.getClusterName()).size());
        ring.restore();
        assertEquals(intialSize, instanceRegistry.getAllIds(cassandraConfiguration.getClusterName()).size());
    }
}
