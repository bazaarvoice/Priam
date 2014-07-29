package com.netflix.priam.identity;

import java.util.List;

/**
 * Interface to manage membership meta information such as size of RAC, list of
 * nodes in RAC etc. Also perform ACL updates used in multi-regional clusters
 */
public interface IMembership {
    /**
     * Get a list of Instances in the current Auto Scale Group
     */
    List<String> getAutoScaleGroupMembership();

    /**
     * @return Size of current availability zone.
     */
    int getAvailabilityZoneMembershipSize();

    /**
     * Number of Availability Zones
     */
    int getUsableAvailabilityZones();
}