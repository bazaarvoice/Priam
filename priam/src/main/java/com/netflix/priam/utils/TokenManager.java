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
package com.netflix.priam.utils;

import com.netflix.priam.identity.Location;

import java.util.List;

public abstract class TokenManager {
    /**
     * Creates a token given the following parameter
     *
     * @param mySlot                         -- Slot where this instance has to be.
     * @param availabilityZones              -- The number of AvailabilityZones
     * @param availabilityZoneMembershipSize -- number of members in the availabilityZone
     * @param location                       -- location where this token is created.
     */
    public String createToken(int mySlot, int availabilityZones, int availabilityZoneMembershipSize, Location location) {
        return createToken(mySlot, availabilityZones * availabilityZoneMembershipSize, location);
    }

    public abstract String createToken(int mySlot, int totalCount, Location location);

    public abstract String findClosestToken(String tokenToSearch, List<String> tokenList);

    /**
     * Converts a token string returned by the JMX API into a token string that can be parsed by Cassandra.
     */
    public abstract String sanitizeToken(String jmxTokenString);

    /**
     * Create an offset to add to token values by hashing the location.
     */
    public static int locationOffset(Location location) {
        return Math.abs(location.hashCode());
    }
}
