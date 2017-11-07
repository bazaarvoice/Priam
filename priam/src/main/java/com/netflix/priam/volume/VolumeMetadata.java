package com.netflix.priam.volume;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * Class for holding metadata about the contents of the Cassandra data volume.  This metadata can be used to assist in
 * transferring EBS volumes between Cassandra instances without the need to re-bootstrap.
 */
public class VolumeMetadata {

    @JsonProperty
    private String volumeId;

    @JsonProperty
    private String clusterName;

    @JsonProperty
    private String availabilityZone;

    @JsonProperty
    private String token;

    @Nullable
    public String getVolumeId() {
        return volumeId;
    }

    public VolumeMetadata setVolumeId(String volumeId) {
        this.volumeId = volumeId;
        return this;
    }

    public String getClusterName() {
        return clusterName;
    }

    public VolumeMetadata setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getAvailabilityZone() {
        return availabilityZone;
    }

    public VolumeMetadata setAvailabilityZone(String availabilityZone) {
        this.availabilityZone = availabilityZone;
        return this;
    }

    public String getToken() {
        return token;
    }

    public VolumeMetadata setToken(String token) {
        this.token = token;
        return this;
    }
}
