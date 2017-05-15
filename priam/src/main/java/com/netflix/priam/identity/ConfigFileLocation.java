package com.netflix.priam.identity;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;

import java.io.IOException;
import java.io.ObjectOutputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Location implementation which gets its attributes directly from Priam configuration files.  This is necessary
 * because the Location is generated as part of Guice injection but the {@link AmazonConfiguration} singleton isn't
 * fully populated until after injection is complete.  Since the configuration's region name likely won't be set at that
 * construction an implementation is required which gets the region name from the configuration directly.
 */
public class ConfigFileLocation extends Location {

    private final AmazonConfiguration amazonConfiguration;
    private final String dataCenterSuffix;

    @Inject
    public ConfigFileLocation(AmazonConfiguration amazonConfiguration, CassandraConfiguration cassandraConfiguration) {
        this.amazonConfiguration = checkNotNull(amazonConfiguration, "Amazon configuration is required");
        checkNotNull(cassandraConfiguration, "Cassandra configuration is required");

        // Amazon config will ensure the region name is set, but validate the data center suffix.
        dataCenterSuffix = Strings.nullToEmpty(cassandraConfiguration.getDataCenterSuffix());
        checkArgument(!dataCenterSuffix.contains("/"), "Data center suffix cannot include '/'");
    }

    @Override
    public String getRegionName() {
        return amazonConfiguration.getRegionName();
    }

    @Override
    public String getDataCenterSuffix() {
        return dataCenterSuffix;
    }

    /**
     * Override serializer to write as a simple location.
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeObject(new SimpleLocation(getRegionName(), getDataCenterSuffix()));
    }
}
