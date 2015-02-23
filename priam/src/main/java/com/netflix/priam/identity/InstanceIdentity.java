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
package com.netflix.priam.identity;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.utils.JMXNodeTool;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.TokenManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkState;

/**
 * This class provides the central place to create and consume the identity of
 * the instance - token, seeds etc.
 */
@Singleton
public class InstanceIdentity {
    private static final Logger logger = LoggerFactory.getLogger(InstanceIdentity.class);
    private final ListMultimap<String, PriamInstance> instancesByAvailabilityZoneMultiMap = ArrayListMultimap.create();
    private final IPriamInstanceRegistry instanceRegistry;
    private final IMembership membership;
    private final CassandraConfiguration cassandraConfiguration;
    private final AmazonConfiguration amazonConfiguration;
    private final TokenManager tokenManager;
    private final Sleeper sleeper;

    private PriamInstance myInstance;
    private boolean isReplace = false;
    private String replacedIp = "";

    @Inject
    public InstanceIdentity(CassandraConfiguration cassandraConfiguration, AmazonConfiguration amazonConfiguration,
                            IPriamInstanceRegistry instanceRegistry, IMembership membership, TokenManager tokenManager, Sleeper sleeper) throws Exception {
        this.instanceRegistry = instanceRegistry;
        this.membership = membership;
        this.cassandraConfiguration = cassandraConfiguration;
        this.amazonConfiguration = amazonConfiguration;
        this.tokenManager = tokenManager;
        this.sleeper = sleeper;
        init();
    }

    public PriamInstance getInstance() {
        return myInstance;
    }

    public void init() throws Exception {
        // try to grab the token which was already assigned
        myInstance = new GetOwnToken().call();

        // If no token has already been assigned to this instance, grab a token that belonged to an instance that is no longer present
        if (null == myInstance) {
            myInstance = new GetDeadToken().call();
        }

        // If no token has already been assigned, and there are no dead tokens to resurrect, allocate a new token
        if (null == myInstance) {
            myInstance = new GetNewToken().call();
        }

        logger.info("My token: {}", myInstance.getToken());
    }

    public class GetOwnToken extends RetryableCallable<PriamInstance> {
        @Override
        public PriamInstance retriableCall() throws Exception {
            // Look to see if an instance with the same instanceID is already part of the cluster.  If so, use it.
            for (PriamInstance ins : instanceRegistry.getAllIds(cassandraConfiguration.getClusterName())) {
                logger.debug("Iterating through the hosts: {}", ins.getInstanceId());
                if (ins.getInstanceId().equals(amazonConfiguration.getInstanceID())) {
                    return ins;
                }
            }
            return null;
        }
    }

    public class GetDeadToken extends RetryableCallable<PriamInstance> {
        @Override
        public PriamInstance retriableCall() throws Exception {
            List<PriamInstance> priamInstances = instanceRegistry.getAllIds(cassandraConfiguration.getClusterName());
            List<String> asgInstanceIDs = membership.getAutoScaleGroupMembership();
            // Sleep random interval - 10 to 15 sec
            sleeper.sleep(new Random().nextInt(5000) + 10000);

            // Build a list of "dead" instances that we might replace (those that are in our availability zone but not associated with an instance active in our ASG)
            List<PriamInstance> deadInstances = Lists.newArrayList();
            for (final PriamInstance instance : priamInstances) {
                // Only consider instances that are in the same availability zone but not in the auto-scale group
                if (instance.getAvailabilityZone().equals(amazonConfiguration.getAvailabilityZone())
                        && !asgInstanceIDs.contains(instance.getInstanceId())) {
                    // *Clang* Bring out your dead!
                    deadInstances.add(instance);
                }
            }

            // No such instances available; return and try something else
            if (deadInstances.isEmpty()) {
                return null;
            }

            // At this point we have at least one slot with associated with an invalid instance. Unfortunately, deadInstances will be a sorted list - if every new instance tries to grab the first
            // entry on it then they'll all be contending for the same slot. We have mechanisms in place to ensure that this doesn't create an outright error condition, but it's still very
            // inefficient, as servers may have to retry multiple times before they can successfully claim a slot. To reduce this contention, we have each instance select an available deadInstance at
            // random, allowing for more than one instance to succeed on its first pass.
            String newInstanceId = amazonConfiguration.getInstanceID();
            int newInstanceHash = Math.abs(newInstanceId.hashCode());
            int randomIndex = (newInstanceHash % deadInstances.size());
            PriamInstance deadInstance = deadInstances.get(randomIndex);

            logger.info("Found dead instance {} with token {} - trying to grab its slot.", deadInstance.getInstanceId(), deadInstance.getToken());
            PriamInstance newInstance = instanceRegistry.acquireSlotId(deadInstance.getId(), deadInstance.getInstanceId(), cassandraConfiguration.getClusterName(),
                    newInstanceId, amazonConfiguration.getPrivateHostName(), amazonConfiguration.getPrivateIP(), amazonConfiguration.getAvailabilityZone(),
                    deadInstance.getVolumes(), deadInstance.getToken());
            if (newInstance != null) {
                isReplace = true;
                replacedIp = deadInstance.getHostIP();
                return newInstance;
            }

            // Failed to acquire the slot . . throw an exception so that we retry the operation
            logger.info("New instance {} failed to acquire slot {}", newInstanceId, deadInstance.getId());
            throw new Exception("Failed to acquire token");
        }

        public void forEachExecution() {
            populateInstancesByAvailabilityZoneMultiMap();
        }
    }

    public class GetNewToken extends RetryableCallable<PriamInstance> {
        public GetNewToken() {
            setRetries(100);
            setWaitTime(100);
        }

        @Override
        public PriamInstance retriableCall() throws Exception {
            logger.info("Generating my own and new token");
            // Sleep random interval - up to 15 sec
            sleeper.sleep(new Random().nextInt(15000));

            int hash = TokenManager.regionOffset(amazonConfiguration.getRegionName());

            // Use this hash so that the nodes are spread far away from the other regions.
            // A PriamInstance's id is the same as it's owning region's hash + an index counter.  This is
            // different from the token assignment.  For example:
            // - the hash for "us-east-1" is 1808575600
            // - the "id" for the first instance in that region is 1808575600
            // - the "id" for the second instance in that region is 1808575601
            // - the "id" for the third instance in that region is 1808575602
            // - and so on...
            // Iterate over all nodes in the cluster in the same availability zone and find the max "id"
            int max = hash;
            for (PriamInstance priamInstance : instanceRegistry.getAllIds(cassandraConfiguration.getClusterName())) {
                if (priamInstance.getAvailabilityZone().equals(amazonConfiguration.getAvailabilityZone())
                        && (priamInstance.getId() > max)) {
                    max = priamInstance.getId();
                }
            }

            // If the following instances started, this is how their slots would be calculated:
            // - us-east-1a1   max = 1808575600, maxSlot = 0 =====> mySlot = 0, id = 1808575600
            // - us-east-1a2   max = 1808575600, maxSlot = 0 =====> mySlot = 3, id = 1808575603
            // - us-east-1b1   max = 1808575600, maxSlot = 0 =====> mySlot = 1, id = 1808575601
            // - us-east-1b2   max = 1808575600, maxSlot = 1 =====> mySlot = 4, id = 1808575604
            // - us-east-1c1   max = 1808575600, maxSlot = 0 =====> mySlot = 2, id = 1808575602
            // - us-east-1c2   max = 1808575600, maxSlot = 2 =====> mySlot = 5, id = 1808575605

            int maxSlot = max - hash;
            int mySlot;
            if (hash == max && instancesByAvailabilityZoneMultiMap.get(amazonConfiguration.getAvailabilityZone()).size() == 0) {
                // This is the first instance in the region and first instance in its availability zone.
                int idx = amazonConfiguration.getUsableAvailabilityZones().indexOf(amazonConfiguration.getAvailabilityZone());
                checkState(idx >= 0, "Zone %s is not in usable availability zones: %s", amazonConfiguration.getAvailabilityZone(), amazonConfiguration.getUsableAvailabilityZones());
                mySlot = idx + maxSlot;
            } else {
                mySlot = amazonConfiguration.getUsableAvailabilityZones().size() + maxSlot;
            }

            logger.info("Trying to createToken with slot {} with rac count {} with rac membership size {} with dc {}",
                    mySlot, membership.getUsableAvailabilityZones(), membership.getAvailabilityZoneMembershipSize(), amazonConfiguration.getRegionName());
            String token = tokenManager.createToken(mySlot, membership.getUsableAvailabilityZones(), membership.getAvailabilityZoneMembershipSize(), amazonConfiguration.getRegionName());
            return instanceRegistry.create(cassandraConfiguration.getClusterName(), mySlot + hash, amazonConfiguration.getInstanceID(), amazonConfiguration.getPrivateHostName(), amazonConfiguration.getPrivateIP(), amazonConfiguration.getAvailabilityZone(), null, token);
        }

        public void forEachExecution() {
            populateInstancesByAvailabilityZoneMultiMap();
        }
    }

    private void populateInstancesByAvailabilityZoneMultiMap() {
        instancesByAvailabilityZoneMultiMap.clear();
        for (PriamInstance ins : instanceRegistry.getAllIds(cassandraConfiguration.getClusterName())) {
            instancesByAvailabilityZoneMultiMap.put(ins.getAvailabilityZone(), ins);
        }
    }

    public List<String> getSeeds() {
        populateInstancesByAvailabilityZoneMultiMap();
        List<String> seeds = new LinkedList<>();
        // Handle single zone deployment
        if (amazonConfiguration.getUsableAvailabilityZones().size() == 1) {
            // Return empty list if all nodes are not up
            List<PriamInstance> priamInstances = instancesByAvailabilityZoneMultiMap.get(myInstance.getAvailabilityZone());
            if (membership.getAvailabilityZoneMembershipSize() != priamInstances.size()) {
                return seeds;
            }
            // If seed node, return the next node in the list
            if (priamInstances.size() > 1 && priamInstances.get(0).getHostIP().equals(myInstance.getHostIP())) {
                seeds.add(priamInstances.get(1).getHostIP());
            }
        }
        logger.info("Retrieved seeds. My IP: {}, AZ-To-Instance-MultiMap: {}", myInstance.getHostIP(), instancesByAvailabilityZoneMultiMap);

        for (String loc : instancesByAvailabilityZoneMultiMap.keySet()) {
            seeds.add(instancesByAvailabilityZoneMultiMap.get(loc).get(0).getHostIP());
        }

        // Remove this node from the seed list so Cassandra auto-bootstrap will kick in.  Unless this is the only node in the cluster.
        if (seeds.size() > 1) {
            seeds.remove(myInstance.getHostIP());
        }

        return seeds;
    }

    public boolean isSeed() {
        populateInstancesByAvailabilityZoneMultiMap();
        String seedHostIPForAvailabilityZone = instancesByAvailabilityZoneMultiMap.get(myInstance.getAvailabilityZone()).get(0).getHostIP();
        return myInstance.getHostIP().equals(seedHostIPForAvailabilityZone);
    }

    public boolean isReplace() {
        return isReplace;
    }

    public String getReplacedIp() {
        return replacedIp;
    }

    /**
     * Updates the Priam instance registry (SimpleDB) with the token currently in use by Cassandra.  Call this after
     * moving a server to a new token or else the move may be reverted if/when the server is replaced and the
     * replacement assigns the old token from SimpleDB.
     */
    public void updateToken() throws Exception {
        JMXNodeTool nodetool = JMXNodeTool.instance(cassandraConfiguration);
        myInstance.setToken(tokenManager.sanitizeToken(nodetool.getTokens().get(0)));
        instanceRegistry.update(myInstance);
    }
}
