package com.netflix.priam;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.identity.IPriamInstanceRegistry;
import com.netflix.priam.identity.Location;
import com.netflix.priam.identity.PriamInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FakePriamInstanceRegistry implements IPriamInstanceRegistry {
    private final Map<Integer, PriamInstance> instances = Maps.newHashMap();
    private final Location location;

    @Inject
    public FakePriamInstanceRegistry(Location location) {
        this.location = location;
    }

    @Override
    public List<PriamInstance> getAllIds(String appName) {
        return new ArrayList<PriamInstance>(instances.values());
    }

    @Override
    public PriamInstance getInstance(String appName, int id) {
        return instances.get(id);
    }

    @Override
    public PriamInstance create(String app, int id, String instanceID, String hostname, String ip, String rac, Map<String, Object> volumes, String payload) {
        return acquireSlotId(id, null, app, instanceID, hostname, ip, rac, volumes, payload);
    }

    @Override
    public PriamInstance acquireSlotId(int slotId, String expectedInstanceId, String app, String instanceID, String hostname, String ip, String rac, Map<String, Object> volumes, String token) {
        // We can acquire the slot if 1.) the expected instance ID is null and the slot ID does not currently exist or
        // 2.) the expected instance ID is valid, the slot exists, and the values match
        boolean canAcquire = (expectedInstanceId == null && !instances.containsKey(slotId)) ||
                (expectedInstanceId != null && instances.containsKey(slotId) && instances.get(slotId).getInstanceId() == expectedInstanceId);
        if (canAcquire) {
            PriamInstance ins = new PriamInstance();
            ins.setApp(app);
            ins.setAvailabilityZone(rac);
            ins.setHost(hostname, ip);
            ins.setId(slotId);
            ins.setInstanceId(instanceID);
            ins.setToken(token);
            ins.setVolumes(volumes);
            ins.setLocation(location);

            instances.put(slotId, ins);
            return ins;
        } else {
            return null;
        }
    }

    @Override
    public void delete(PriamInstance inst) {
        instances.remove(inst.getId());
    }

    @Override
    public void update(PriamInstance inst) {
        instances.put(inst.getId(), inst);
    }
}
