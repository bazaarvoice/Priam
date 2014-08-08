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

import java.util.List;

public abstract class TokenManager {
    /**
     * Creates a token given the following parameter
     *
     * @param mySlot                         -- Slot where this instance has to be.
     * @param availabilityZones              -- The number of AvailabilityZones
     * @param availabilityZoneMembershipSize -- number of members in the availabilityZone
     * @param region                         -- name of the DC where it this token is created.
     */
    public String createToken(int mySlot, int availabilityZones, int availabilityZoneMembershipSize, String region) {
        return createToken(mySlot, availabilityZones * availabilityZoneMembershipSize, region);
    }

    public abstract String createToken(int mySlot, int totalCount, String region);

    public abstract String findClosestToken(String tokenToSearch, List<String> tokenList);

    /**
     * Converts a token string returned by the JMX API into a token string that can be parsed by Cassandra.
     */
    public abstract String sanitizeToken(String jmxTokenString);

    /**
     * Create an offset to add to token values by hashing the region name.
     */
    public static int regionOffset(String region) {
        return Math.abs(region.hashCode());
    }
}
