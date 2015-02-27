package com.netflix.priam.local;

import com.google.inject.Inject;
import com.netflix.priam.config.PriamConfiguration;
import com.netflix.priam.identity.IMembership;

import java.util.Collections;
import java.util.List;

public class LocalMembership implements IMembership {

    private final PriamConfiguration priamConfiguration;

    @Inject
    LocalMembership(PriamConfiguration configuration) {
        this.priamConfiguration = configuration;
    }

    @Override
    public List<String> getAutoScaleGroupMembership() {
        return Collections.singletonList(priamConfiguration.getAmazonConfiguration().getInstanceID());
    }

    @Override
    public int getAvailabilityZoneMembershipSize() {
        return 1;
    }

    @Override
    public int getUsableAvailabilityZones() {
        return 3;
    }
}
