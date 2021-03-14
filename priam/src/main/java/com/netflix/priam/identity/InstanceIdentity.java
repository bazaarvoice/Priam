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

import com.google.common.base.Objects;
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
import com.netflix.priam.volume.IVolumeMetadataManager;
import com.netflix.priam.volume.VolumeMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    private final ListMultimap<LocationAZPair, PriamInstance> instancesByLocationAndAZMultiMap = ArrayListMultimap.create();
    private final IPriamInstanceRegistry instanceRegistry;
    private final IMembership membership;
    private final CassandraConfiguration cassandraConfiguration;
    private final AmazonConfiguration amazonConfiguration;
    private final IVolumeMetadataManager volumeMetadataManager;
    private final TokenManager tokenManager;
    private final Sleeper sleeper;
    private final Location location;

    private PriamInstance myInstance;
    private boolean isReplace = false;
    private String replacedIp = "";
    private boolean isUsingReplacedVolume;

    @Inject
    public InstanceIdentity(CassandraConfiguration cassandraConfiguration, AmazonConfiguration amazonConfiguration,
                            IVolumeMetadataManager volumeMetadataManager,
                            IPriamInstanceRegistry instanceRegistry, IMembership membership, TokenManager tokenManager,
                            Sleeper sleeper, Location location) throws Exception {
        this.instanceRegistry = instanceRegistry;
        this.membership = membership;
        this.cassandraConfiguration = cassandraConfiguration;
        this.amazonConfiguration = amazonConfiguration;
        this.volumeMetadataManager = volumeMetadataManager;
        this.tokenManager = tokenManager;
        this.sleeper = sleeper;
        this.location = location;
        init();
    }

    public PriamInstance getInstance() {
        return myInstance;
    }

    public void init() throws Exception {
        // try to grab the token which was already assigned
        myInstance = new GetOwnToken().call();

        // If no token has already been assigned to this instance, grab a token that belonged to an instance that is no
        // longer present
        if (null == myInstance) {
            myInstance = new GetDeadToken().call();
        }

        // If no token has already been assigned, and there are no dead tokens to resurrect, allocate a new token
        if (null == myInstance) {
            myInstance = new GetNewToken().call();
        }

        persistVolumeMetadata();

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
            // Get all instances and filter out those which are not in the local ring
            List<PriamInstance> priamInstances = filteredRemote(instanceRegistry.getAllIds(cassandraConfiguration.getClusterName()));
            List<String> asgInstanceIDs = membership.getAutoScaleGroupMembership();
            // Sleep random interval - 10 to 15 sec
            sleeper.sleep(new Random().nextInt(5000) + 10000);

            // Build a list of dead instances that we might replace
            boolean healthyNodePresent = false;
            List<PriamInstance> deadInstances = Lists.newArrayList();
            for (final PriamInstance instance : priamInstances) {
                if (!asgInstanceIDs.contains(instance.getInstanceId())) {
                    // The provided instance was not found in our ASG - it's dead, just do an additional check to see if
                    // it's in our availability zone (in which case we are eligible to replace it)
                    if (instance.getAvailabilityZone().equals(amazonConfiguration.getAvailabilityZone())) {
                        // *Clang* Bring out your dead!
                        deadInstances.add(instance);
                    }
                } else {
                    // Flag whether there is a healthy node present in our ASG (it doesn't matter if it's in our
                    // availability zone or not)
                    healthyNodePresent = true;
                }
            }

            // No dead instances available; return and try something else
            if (deadInstances.isEmpty()) {
                return null;
            }

            // Log an error message if our auto-scale group doesn't have any active nodes. At this point we're doomed to
            // fail, but instead of getting cute and trying to repair the state on our own, we alert the engineer so
            // that they can fix the underlying issue.
            if (healthyNodePresent == false) {
                logger.error("Attempting to replace dead tokens in a cluster where no healthy nodes exist. Cassandra is likely to deadlock at startup. Consider clearing the SimpleDB data " +
                        "for this cluster");
            }

            PriamInstance deadInstance = null;
            String newInstanceId = amazonConfiguration.getInstanceID();
            boolean usingVolumeFromDeadInstance = false;

            // It's possible that one of the dead instances mounted its Cassandra directory on an EBS volume and that
            // the EBS volume has now been attached to this instance.  If that's the case then it may be possible to
            // replace that instance without requiring a costly bootstrap from neighbors.
            VolumeMetadata volumeMetadata = volumeMetadataManager.getVolumeMetadata();
            if (volumeMetadata != null) {
                // All of the following must be true in order to utilize the existing Cassandra data on volume:
                // 1. The volume metadata must match the Cassandra cluster.
                // 2. The volume metadata must match the availability zone.
                // 3. The volume metadata must match one of the dead instance's token.

                if (volumeMetadata.getClusterName().equals(cassandraConfiguration.getClusterName()) &&
                        volumeMetadata.getAvailabilityZone().equals(amazonConfiguration.getAvailabilityZone())) {

                    for (PriamInstance candidateDeadInstance : deadInstances) {
                        if (volumeMetadata.getToken().equals(candidateDeadInstance.getToken())) {
                            // It should never be the case that an existing volume was found for a new instance placeholder
                            // from doubling the ring.  Cassandra will probably fail to start because of this, so let
                            // someone have an idea why.
                            if (candidateDeadInstance.getInstanceId().equals(PriamInstance.NEW_INSTANCE_PLACEHOLDER_ID)) {
                                logger.error("Volume found with data for a new instance.  This shouldn't happen and will likely cause Cassandra to fail to start.");
                            } else {
                                logger.info("Volume found with data matching dead instance {}", candidateDeadInstance.getInstanceId());
                            }

                            deadInstance = candidateDeadInstance;
                            usingVolumeFromDeadInstance = true;
                            break;
                        }
                    }
                }

                if (deadInstance == null) {
                    // No matching dead instance was found.  In this case if there is any existing data on the volume
                    // it is unusable and Cassandra will fail to start with the present data.  Rather than be destructive
                    // error out and let the admin know what to do to resolve the issue.
                    logger.error(
                            "Cassandra unable to start with existing data on the volume from an inconsistent instance. " +
                            "Either attach a fresh volume or delete the Cassandra data and volume metadata file.");
                    throw exceptionWithNoRetries(new IllegalStateException("Inconsistent volume data found"));
                }

                // Check whether the EBS volume ID matches the ID in the volume metadata.  Here's why:
                //
                // If it matches then the expected scenario is that the dead instance was terminated, the EBS volume
                // was detached and has now be attached to this instance.  In this case Cassandra will assume the
                // dead instance's host ID and any hints accumulated during the downtime will be delivered to
                // this instance, so no additional work is required to bring this instance up-to-date.
                //
                // If it doesn't match then this could mean one of several other scenarios, such as this volume having
                // been created from a snapshot of the dead instance's volume.  This doesn't prevent it from joining
                // the ring but it does mean that there may be missing data on this volume that won't be resolved
                // naturally, even it it assumes the instance's host ID.  Don't block because of this, but send out
                // an alert that a repair should be performed to get this volume up-to-date.

                String volumeId = volumeMetadataManager.getVolumeID();
                if (volumeId == null) {
                    logger.warn("No volume ID was found for the current volume, so the consistency of the existing " +
                            "Cassandra data on volume could not be verified.  Repairing this instance is strongly recommended.");
                } else if (!volumeId.equals(volumeMetadata.getVolumeId())) {
                    logger.warn("Volume ID from the existing volume metadata did not match, so the existing Cassandra data" +
                            "on volume is likely stale.  Repairing this instance is strongly recommended.");
                }
            }

            if (deadInstance == null) {
                // At this point we have at least one slot associated with an invalid instance and either the volume is
                // new or no EBS volume metadata was found, so choose any replaced IP.  Unfortunately, deadInstances
                // will be a sorted list, and if every new instance tries to grab the first entry on it then they'll all be
                // contending for the same slot. We have mechanisms in place to ensure that this doesn't create an outright
                // error condition, but it's still very inefficient, as servers may have to retry multiple times before they
                // can successfully claim a slot. To reduce this contention, we have each new instance select an available
                // deadInstance slot at random, allowing for more than one instance to succeed on its first try.
                int newInstanceHash = Math.abs(newInstanceId.hashCode());
                int randomIndex = (newInstanceHash % deadInstances.size());
                deadInstance = deadInstances.get(randomIndex);
            }

            logger.info("Found dead instance {} with token {} - trying to grab its slot.", deadInstance.getInstanceId(), deadInstance.getToken());
            PriamInstance newInstance = instanceRegistry.acquireSlotId(deadInstance.getId(), deadInstance.getInstanceId(), cassandraConfiguration.getClusterName(),
                    newInstanceId, amazonConfiguration.getPrivateHostName(), amazonConfiguration.getPrivateIP(), amazonConfiguration.getAvailabilityZone(),
                    deadInstance.getVolumes(), deadInstance.getToken());
            if (newInstance != null) {
                // We succeeded! Flag the instance we replaced (if we replaced an actual functioning instance and not
                // just a placeholder instance, such as those populated by /double_ring).
                if (!deadInstance.getInstanceId().equals(PriamInstance.NEW_INSTANCE_PLACEHOLDER_ID)) {
                    isReplace = true;
                    replacedIp = deadInstance.getHostIP();
                    isUsingReplacedVolume = usingVolumeFromDeadInstance;
                }
                return newInstance;
            }

            // Failed to acquire the slot . . throw an exception so that we retry the operation
            logger.info("New instance {} failed to acquire slot {}", newInstanceId, deadInstance.getId());
            throw new Exception("Failed to acquire token");
        }

        public void forEachExecution() {
            populateInstanceByLocationAndAZMultiMap();
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

            int hash = TokenManager.locationOffset(location);

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
            for (PriamInstance priamInstance : filteredRemote(instanceRegistry.getAllIds(cassandraConfiguration.getClusterName()))) {
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
            if (hash == max && instancesByLocationAndAZMultiMap.get(new LocationAZPair(location, amazonConfiguration.getAvailabilityZone())).size() == 0) {
                // This is the first instance in the location and first instance in its availability zone.
                int idx = amazonConfiguration.getUsableAvailabilityZones().indexOf(amazonConfiguration.getAvailabilityZone());
                checkState(idx >= 0, "Zone %s is not in usable availability zones: %s", amazonConfiguration.getAvailabilityZone(), amazonConfiguration.getUsableAvailabilityZones());
                mySlot = idx + maxSlot;
            } else {
                mySlot = amazonConfiguration.getUsableAvailabilityZones().size() + maxSlot;
            }

            logger.info("Trying to createToken with slot {} with rac count {} with rac membership size {} with dc {}",
                    mySlot, membership.getUsableAvailabilityZones(), membership.getAvailabilityZoneMembershipSize(), location);
            String token = tokenManager.createToken(mySlot, membership.getUsableAvailabilityZones(), membership.getAvailabilityZoneMembershipSize(), location);
            return instanceRegistry.create(cassandraConfiguration.getClusterName(), mySlot + hash,
                    amazonConfiguration.getInstanceID(), amazonConfiguration.getPrivateHostName(),
                    amazonConfiguration.getPrivateIP(), amazonConfiguration.getAvailabilityZone(), null, token);
        }

        public void forEachExecution() {
            populateInstanceByLocationAndAZMultiMap();
        }
    }

    private void populateInstanceByLocationAndAZMultiMap() {
        instancesByLocationAndAZMultiMap.clear();
        for (PriamInstance ins : instanceRegistry.getAllIds(cassandraConfiguration.getClusterName())) {
            instancesByLocationAndAZMultiMap.put(new LocationAZPair(ins.getLocation(), ins.getAvailabilityZone()), ins);
        }
    }

    public List<String> getSeeds() {
        populateInstanceByLocationAndAZMultiMap();
        List<String> seeds = new LinkedList<>();
        // Handle single zone deployment
        if (amazonConfiguration.getUsableAvailabilityZones().size() == 1) {
            // Return empty list if all nodes are not up
            List<PriamInstance> priamInstances = instancesByLocationAndAZMultiMap.get(new LocationAZPair(myInstance.getLocation(), myInstance.getAvailabilityZone()));
            if (membership.getAvailabilityZoneMembershipSize() != priamInstances.size()) {
                return seeds;
            }
            // If seed node, return the next node in the list
            if (priamInstances.size() > 1 && priamInstances.get(0).getHostIP().equals(myInstance.getHostIP())) {
                seeds.add(priamInstances.get(1).getHostIP());
            }
        }
        logger.info("Retrieved seeds. My IP: {}, AZ-To-Instance-MultiMap: {}", myInstance.getHostIP(), instancesByLocationAndAZMultiMap);

        for (LocationAZPair loc : instancesByLocationAndAZMultiMap.keySet()) {
            seeds.add(instancesByLocationAndAZMultiMap.get(loc).get(0).getHostIP());
        }

        // Remove this node from the seed list so Cassandra auto-bootstrap will kick in.  Unless this is the only node
        // in the cluster.
        if (seeds.size() > 1) {
            seeds.remove(myInstance.getHostIP());
        }

        return seeds;
    }

    public boolean isSeed() {
        populateInstanceByLocationAndAZMultiMap();
        String seedHostIPForAvailabilityZone = instancesByLocationAndAZMultiMap
                .get(new LocationAZPair(myInstance.getLocation(), myInstance.getAvailabilityZone()))
                .get(0)
                .getHostIP();
        return myInstance.getHostIP().equals(seedHostIPForAvailabilityZone);
    }

    public boolean isReplace() {
        return isReplace;
    }

    public String getReplacedIp() {
        return replacedIp;
    }

    public boolean isUsingReplacedVolume() {
        return isUsingReplacedVolume;
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
        persistVolumeMetadata();
    }

    // filter other DC's
    private List<PriamInstance> filteredRemote(List<PriamInstance> priamInstances) {
        List<PriamInstance> local = Lists.newArrayList();
        for (PriamInstance priamInstance : priamInstances) {
            if (priamInstance.getLocation().equals(location)) {
                local.add(priamInstance);
            }
        }
        return local;
    }

    private void persistVolumeMetadata() {
        try {
            String volumeId = volumeMetadataManager.getVolumeID();
            volumeMetadataManager.setVolumeMetadata(
                    new VolumeMetadata()
                            .setVolumeId(volumeId)
                            .setAvailabilityZone(amazonConfiguration.getAvailabilityZone())
                            .setClusterName(cassandraConfiguration.getClusterName())
                            .setToken(myInstance.getToken()));
        } catch (IOException e) {
            logger.warn("Failed to persist volume metadata", e);
        }
    }

    /**
     * Simple class for maintaining the pair of (location, availability zone), which can be used for grouping
     * instances in the same ring by availability zone.
     */
    private final class LocationAZPair {
        final Location location;
        final String availabilityZone;

        public LocationAZPair(Location location, String availabilityZone) {
            this.location = location;
            this.availabilityZone = availabilityZone;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LocationAZPair)) {
                return false;
            }
            LocationAZPair other = (LocationAZPair) o;
            return location.equals(other.location) && availabilityZone.equals(other.availabilityZone);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(location, availabilityZone);
        }
    }
}
