package com.netflix.priam.identity;

import com.google.common.base.Strings;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Simple POJO Location implementation.
 */
public class SimpleLocation extends Location {

    private final String regionName;
    private final String dataCenterSuffix;

    public SimpleLocation(String regionName, String dataCenterSuffix) {
        checkArgument(!Strings.isNullOrEmpty(regionName), "Region name must be a non-empty string");
        checkArgument(dataCenterSuffix != null, "Data center suffix cannot be null");
        checkArgument(!regionName.contains("/"), "Region name cannot include '/'");
        checkArgument(!dataCenterSuffix.contains("/"), "Data center suffix cannot include '/'");

        this.regionName = regionName;
        this.dataCenterSuffix = dataCenterSuffix;
    }

    @Override
    public String getRegionName() {
        return regionName;
    }

    @Override
    public String getDataCenterSuffix() {
        return dataCenterSuffix;
    }
}
