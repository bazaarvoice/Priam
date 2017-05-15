/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.aws;

import com.amazonaws.AmazonServiceException;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.identity.IPriamInstanceRegistry;
import com.netflix.priam.identity.Location;
import com.netflix.priam.identity.PriamInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * SimpleDB based instance factory. Requires 'InstanceIdentity' domain to be
 * created ahead of time.
 */
@Singleton
public class SDBInstanceRegistry implements IPriamInstanceRegistry {
    private static final Logger logger = LoggerFactory.getLogger(SDBInstanceRegistry.class);

    private final Location location;
    private final SDBInstanceData dao;

    @Inject
    public SDBInstanceRegistry(Location location, SDBInstanceData dao) {
        this.location = location;
        this.dao = dao;
    }

    @Override
    public List<PriamInstance> getAllIds(String appName) {
        return Ordering.natural().immutableSortedCopy(dao.getAllIds(appName, true));
    }

    @Override
    public PriamInstance getInstance(String appName, int id) {
        return dao.getInstance(appName, id);
    }

    @Override
    public PriamInstance create(String app, int id, String instanceID, String hostname, String ip, String rac, Map<String, Object> volumes, String token) {
        return acquireSlotId(id, null, app, instanceID, hostname, ip, rac, volumes, token);
    }

    @Override
    public PriamInstance acquireSlotId(int slotId, String expectedInstanceId, String app, String instanceID, String hostname, String ip, String rac, Map<String, Object> volumes, String token) {
        try {
            PriamInstance ins = PriamInstance.from(app, slotId, instanceID, hostname, ip, rac, volumes, token, location);
            dao.registerInstance(ins, expectedInstanceId);
            // Only return a result if we successfully overwrote the previous value; otherwise return null to indicate failure
            PriamInstance storedInstance = dao.getInstance(ins.getApp(), ins.getId(), true);
            if (storedInstance.getInstanceId().equals(ins.getInstanceId())) {
                return ins;
            } else {
                return null;
            }
        } catch (AmazonServiceException e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Unable to update/create priam instance", e);
        }
    }

    @Override
    public void delete(PriamInstance inst) {
        try {
            dao.deregisterInstance(inst);
        } catch (AmazonServiceException e) {
            throw new RuntimeException("Unable to deregister priam instance", e);
        }
    }

    @Override
    public void update(PriamInstance inst) {
        try {
            dao.createInstance(inst);
        } catch (AmazonServiceException e) {
            throw new RuntimeException("Unable to update/create priam instance", e);
        }
    }
}
