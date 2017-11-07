package com.netflix.priam.config;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.util.EC2MetadataUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;

public class AmazonConfiguration {
    @JsonProperty
    private String autoScaleGroupName;

    @JsonProperty
    private String regionName;

    @JsonProperty
    private String securityGroupName;

    @JsonProperty
    private String availabilityZone;

    @JsonProperty
    private String privateHostName;

    @JsonProperty
    private String privateIP;

    @JsonProperty
    private String instanceID;

    @JsonProperty
    private String instanceType;

    @JsonProperty
    private List<String> usableAvailabilityZones;

    @JsonProperty
    private String simpleDbDomain;

    @JsonProperty
    private String simpleDbRegion;   // Defaults to the current region.  Set explicitly for cross-dc rings.

    @JsonProperty
    private String cassandraVolumeBlockDevice;

    public String getAutoScaleGroupName() {
        return autoScaleGroupName;
    }

    public String getRegionName() {
        return regionName;
    }

    public Region getRegion() {
        return RegionUtils.getRegion(regionName);
    }

    public String getSecurityGroupName() {
        return securityGroupName;
    }

    public String getAvailabilityZone() {
        return availabilityZone;
    }

    public String getPrivateHostName() {
        return privateHostName;
    }

    public String getPrivateIP() {
        return privateIP;
    }

    public String getInstanceID() {
        return instanceID;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public List<String> getUsableAvailabilityZones() {
        return usableAvailabilityZones;
    }

    public String getSimpleDbDomain() {
        return simpleDbDomain;
    }

    public String getSimpleDbRegion() {
        return StringUtils.isNotBlank(simpleDbRegion) ? simpleDbRegion : "us-east-1";
    }

    public String getCassandraVolumeBlockDevice() {
        return cassandraVolumeBlockDevice;
    }

    public void setAutoScaleGroupName(String autoScaleGroupName) {
        this.autoScaleGroupName = autoScaleGroupName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public void setSecurityGroupName(String securityGroupName) {
        this.securityGroupName = securityGroupName;
    }

    public void setAvailabilityZone(String availabilityZone) {
        this.availabilityZone = availabilityZone;
    }

    public void setPrivateHostName(String privateHostName) {
        this.privateHostName = privateHostName;
    }

    public void setPrivateIP(String privateIP) {
        this.privateIP = privateIP;
    }

    public void setInstanceID(String instanceID) {
        this.instanceID = instanceID;
    }

    public void setInstanceType(String instanceType) {
        this.instanceType = instanceType;
    }

    public void setUsableAvailabilityZones(List<String> usableAvailabilityZones) {
        this.usableAvailabilityZones = usableAvailabilityZones;
    }

    public void setSimpleDbDomain(String simpleDbDomain) {
        this.simpleDbDomain = simpleDbDomain;
    }

    public void setSimpleDbRegion(String simpleDbRegion) {
        this.simpleDbRegion = simpleDbRegion;
    }

    public void setCassandraVolumeBlockDevice(String cassandraVolumeBlockDevice) {
        this.cassandraVolumeBlockDevice = cassandraVolumeBlockDevice;
    }
    
    public void discoverConfiguration(AWSCredentialsProvider credentialProvider) {
        if (StringUtils.isBlank(availabilityZone)) {
            availabilityZone = EC2MetadataUtils.getAvailabilityZone();
        }
        if (StringUtils.isBlank(regionName)) {
            regionName = availabilityZone.substring(0, availabilityZone.length() - 1);
        }
        if (StringUtils.isBlank(instanceID)) {
            instanceID = EC2MetadataUtils.getInstanceId();
        }
        if (StringUtils.isBlank(autoScaleGroupName)) {
            autoScaleGroupName = getAutoScaleGroupName(credentialProvider, regionName, instanceID);
        }
        if (StringUtils.isBlank(instanceType)) {
            instanceType = EC2MetadataUtils.getInstanceType();
        }
        if (StringUtils.isBlank(privateHostName)) {
            privateHostName = EC2MetadataUtils.getLocalHostName();
        }
        if (StringUtils.isBlank(privateIP)) {
            privateIP = EC2MetadataUtils.getPrivateIpAddress();
        }
        if (CollectionUtils.isEmpty(usableAvailabilityZones)) {
            usableAvailabilityZones = getUsableAvailabilityZones(credentialProvider, regionName);
        }
        if (StringUtils.isBlank(securityGroupName)) {
            securityGroupName = Iterables.getFirst(EC2MetadataUtils.getSecurityGroups(), null);
        }
    }

    /**
     * Query amazon to get ASG name. Currently not available as part of instance info api.
     */
    private String getAutoScaleGroupName(AWSCredentialsProvider credentialProvider, String region, String instanceId) {
        AmazonEC2 client = new AmazonEC2Client(credentialProvider);
        client.setRegion(RegionUtils.getRegion(region));
        try {
            DescribeInstancesRequest desc = new DescribeInstancesRequest().withInstanceIds(instanceId);
            DescribeInstancesResult res = client.describeInstances(desc);

            for (Reservation resr : res.getReservations()) {
                for (Instance ins : resr.getInstances()) {
                    for (Tag tag : ins.getTags()) {
                        if (tag.getKey().equals("aws:autoscaling:groupName")) {
                            return tag.getValue();
                        }
                    }
                }
            }
        } finally {
            client.shutdown();
        }
        return null;
    }

    /**
     * Returns the first 3 availability zones in the region.
     */
    private List<String> getUsableAvailabilityZones(AWSCredentialsProvider credentialProvider, String region) {
        List<String> zones = Lists.newArrayList();
        AmazonEC2 client = new AmazonEC2Client(credentialProvider);
        client.setRegion(RegionUtils.getRegion(region));
        try {
            DescribeAvailabilityZonesResult res = client.describeAvailabilityZones();
            for (AvailabilityZone zone : res.getAvailabilityZones()) {
                if (zone.getState().equals("available")) {
                    zones.add(zone.getZoneName());
                }
                if (zones.size() == 3) {
                    break;
                }
            }
        } finally {
            client.shutdown();
        }
        return zones;
    }
}
