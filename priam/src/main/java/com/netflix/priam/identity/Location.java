package com.netflix.priam.identity;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Location is a representation of where this data center is within the cluster.  It contains two elements:
 *
 * <ol>
 *     <li>The AWS region</li>
 *     <li>A suffix for the data center, which can be used to create multiple distinct rings for the cluster
 *         within the same data center</li>
 * </ol>
 */
abstract public class Location implements Serializable {

    /**
     * Static helper method to create a Location from the string produced by {@link #toString()}.
     */
    public static Location from(String locationString) {
        String[] parts = locationString.split("/");
        checkArgument(parts.length <= 2, "Invalid location string, too many path elements");
        String regionName = parts[0];
        String dataCenterSuffix = parts.length == 1 ? "" : parts[1];
        return new SimpleLocation(regionName, dataCenterSuffix);
    }

    abstract public String getRegionName();

    abstract public String getDataCenterSuffix();

    @Override
    public String toString() {
        // If there is no data center suffix then the location is simply the region name.  This is both for readability
        // and to maintain backwards compatibility with clusters created using an older version of Priam which only
        // supported a single data center per region.
        if (getDataCenterSuffix().isEmpty()) {
            return getRegionName();
        }
        return Joiner.on('/').join(getRegionName(), getDataCenterSuffix());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Location)) {
            return false;
        }

        Location location = (Location) o;

        return getRegionName().equals(location.getRegionName()) && getDataCenterSuffix().equals(location.getDataCenterSuffix());
    }

    @Override
    public int hashCode() {
        // For backwards compatibility with pre-existing token logic if there is no data center suffix return the hash
        // of the region name.
        if (getDataCenterSuffix().isEmpty()) {
            return getRegionName().hashCode();
        }
        return Objects.hashCode(getRegionName(), getDataCenterSuffix());
    }
}
