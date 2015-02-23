package com.netflix.priam.identity;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.netflix.priam.utils.ThreadSleeper;
import com.netflix.priam.utils.TokenManager;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class InstanceIdentityTest extends InstanceTestUtils {

    @Test
    public void testCreateToken() throws Exception {

        identity = createInstanceIdentity("az1", "fakeinstance1");
        int hash = TokenManager.regionOffset(amazonConfiguration.getRegionName());
        assertEquals(0, identity.getInstance().getId() - hash);

        identity = createInstanceIdentity("az1", "fakeinstance2");
        assertEquals(3, identity.getInstance().getId() - hash);

        identity = createInstanceIdentity("az1", "fakeinstance3");
        assertEquals(6, identity.getInstance().getId() - hash);

        // try next region
        identity = createInstanceIdentity("az2", "fakeinstance4");
        assertEquals(1, identity.getInstance().getId() - hash);

        identity = createInstanceIdentity("az2", "fakeinstance5");
        assertEquals(4, identity.getInstance().getId() - hash);

        identity = createInstanceIdentity("az2", "fakeinstance6");
        assertEquals(7, identity.getInstance().getId() - hash);

        // next
        identity = createInstanceIdentity("az3", "fakeinstance7");
        assertEquals(2, identity.getInstance().getId() - hash);

        identity = createInstanceIdentity("az3", "fakeinstance8");
        assertEquals(5, identity.getInstance().getId() - hash);

        identity = createInstanceIdentity("az3", "fakeinstance9");
        assertEquals(8, identity.getInstance().getId() - hash);
    }

    @Test
    public void testDeadInstance() throws Exception {
        createInstances();
        instances.remove("fakeinstance4");
        identity = createInstanceIdentity("az2", "fakeinstancex");
        int hash = TokenManager.regionOffset(amazonConfiguration.getRegionName());
        assertEquals(1, identity.getInstance().getId() - hash);
    }

    @Test
    public void testDeadInstanceContention() throws Exception {
        createInstances();
        instances.remove("fakeinstance4");
        instances.remove("fakeinstance5");
        instances.add("fakeinstancex0");
        instances.add("fakeinstancex1");

        sleeper = new ThreadSleeper();

        final PriamInstance[] priamInstances = new PriamInstance[3];
        final List<Future<PriamInstance>> scheduledFutures = Lists.newArrayList();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        for (int i = 0; i < 2; ++i) {
            final Integer threadId = new Integer(i);
            scheduledFutures.add(executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        InstanceIdentity threadIdentity = createInstanceIdentity("az2", "fakeinstancex" + threadId);
                        priamInstances[threadId] = threadIdentity.getInstance();
                    } catch (Exception e) {
                        assertTrue(false);
                    }
                }
            }, priamInstances[2]));
        }

        // wait for our tasks to complete
        for (Future future : scheduledFutures) {
            future.get();
        }

        assertNotEquals(priamInstances[0], null);
        assertNotEquals(priamInstances[1], null);
        assertNotEquals(priamInstances[0].getId(), priamInstances[1].getId());
    }

    @Test
    public void testGetSeeds() throws Exception {
        createInstances();
        identity = createInstanceIdentity("az1", "fakeinstancex");
        assertEquals(2, identity.getSeeds().size());

        identity = createInstanceIdentity("az1", "fakeinstance1");
        assertEquals(2, identity.getSeeds().size());
    }

    @Test
    public void testDoubleSlots() throws Exception {
        createInstances();
        int before = instanceRegistry.getAllIds("fake-app").size();
        new DoubleRing(cassandraConfiguration, amazonConfiguration, instanceRegistry, tokenManager).doubleSlots();
        List<PriamInstance> lst = instanceRegistry.getAllIds(cassandraConfiguration.getClusterName());
        // sort it so it will look good if you want to print it.
        lst = Ordering.natural().immutableSortedCopy(lst);
        for (int i = 0; i < lst.size(); i++) {
            //System.out.println(lst.get(i));
            if (0 == i % 2) {
                continue;
            }
            assertEquals("new_slot", lst.get(i).getInstanceId());
        }
        assertEquals(before * 2, lst.size());
    }

    @Test
    public void testDoubleGrap() throws Exception {
        createInstances();
        new DoubleRing(cassandraConfiguration, amazonConfiguration, instanceRegistry, tokenManager).doubleSlots();
        amazonConfiguration.setAvailabilityZone("az1");
        amazonConfiguration.setInstanceID("fakeinstancex");
        int hash = TokenManager.regionOffset(amazonConfiguration.getRegionName());
        identity = createInstanceIdentity("az1", "fakeinstancex");
        printInstance(identity.getInstance(), hash);
    }

    public void printInstance(PriamInstance ins, int hash) {
        //System.out.println("ID: " + (ins.getInstanceIdentity() - hash));
        //System.out.println("PayLoad: " + ins.getToken());

    }

}
