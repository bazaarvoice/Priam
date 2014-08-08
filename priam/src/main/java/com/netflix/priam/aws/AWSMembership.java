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
package com.netflix.priam.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.Instance;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.identity.IMembership;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Class to query amazon ASG for its members to provide - Number of valid nodes
 * in the ASG - Number of zones - Methods for adding ACLs for the nodes
 */
public class AWSMembership implements IMembership {
    private static final Logger logger = LoggerFactory.getLogger(AWSMembership.class);
    private final AmazonConfiguration amazonConfiguration;
    private final AWSCredentialsProvider provider;

    @Inject
    public AWSMembership(AmazonConfiguration amazonConfiguration, AWSCredentialsProvider provider) {
        this.amazonConfiguration = amazonConfiguration;
        this.provider = provider;
    }

    @Override
    public List<String> getAutoScaleGroupMembership() {
        AmazonAutoScaling client = getAutoScalingClient();
        try {
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(amazonConfiguration.getAutoScaleGroupName());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);

            List<String> instanceIds = Lists.newArrayList();
            for (AutoScalingGroup asg : res.getAutoScalingGroups()) {
                for (Instance ins : asg.getInstances()) {
                    if (!(ins.getLifecycleState().equalsIgnoreCase("Terminating") || ins.getLifecycleState().equalsIgnoreCase("shutting-down") || ins.getLifecycleState()
                            .equalsIgnoreCase("Terminated"))) {
                        instanceIds.add(ins.getInstanceId());
                    }
                }
            }
            logger.info("Querying Amazon returned the following instances in the ASG: {} --> {}", amazonConfiguration.getAutoScaleGroupName(), StringUtils.join(instanceIds, ","));
            return instanceIds;
        } finally {
            client.shutdown();
        }
    }

    /**
     * Actual membership AWS source of truth...
     */
    @Override
    public int getAvailabilityZoneMembershipSize() {
        AmazonAutoScaling client = getAutoScalingClient();
        try {
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(amazonConfiguration.getAutoScaleGroupName());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);
            int size = 0;
            for (AutoScalingGroup asg : res.getAutoScalingGroups()) {
                size += asg.getMaxSize();
            }
            logger.info("Max size of ASG is {} instances", size);
            return size;
        } finally {
            client.shutdown();
        }
    }

    @Override
    public int getUsableAvailabilityZones() {
        return amazonConfiguration.getUsableAvailabilityZones().size();
    }

    /**
     * Adds an IP list to the SG.
     */
    public void addACL(Collection<String> listIPs, int fromPort, int toPort) {
        AmazonEC2 client = getEc2Client();
        try {
            List<IpPermission> ipPermissions = ImmutableList.of(
                    new IpPermission().withFromPort(fromPort).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(toPort));
            client.authorizeSecurityGroupIngress(new AuthorizeSecurityGroupIngressRequest(amazonConfiguration.getSecurityGroupName(), ipPermissions));
            logger.info("Done adding ACL to: {}", StringUtils.join(listIPs, ","));
        } finally {
            client.shutdown();
        }
    }

    /**
     * Removes an IP list from the SG
     */
    public void removeACL(Collection<String> listIPs, int fromPort, int toPort) {
        AmazonEC2 client = getEc2Client();
        try {
            List<IpPermission> ipPermissions = ImmutableList.of(
                    new IpPermission().withFromPort(fromPort).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(toPort));
            client.revokeSecurityGroupIngress(new RevokeSecurityGroupIngressRequest(amazonConfiguration.getSecurityGroupName(), ipPermissions));
        } finally {
            client.shutdown();
        }
    }

    /**
     * List SG ACL's
     */
    public List<String> listACL(int from, int to) {
        AmazonEC2 client = getEc2Client();
        try {
            List<String> ipPermissions = new ArrayList<>();
            DescribeSecurityGroupsRequest req = new DescribeSecurityGroupsRequest().withGroupNames(Arrays.asList(amazonConfiguration.getSecurityGroupName()));
            DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);
            for (SecurityGroup group : result.getSecurityGroups()) {
                for (IpPermission perm : group.getIpPermissions()) {
                    if (perm.getFromPort() == from && perm.getToPort() == to) {
                        ipPermissions.addAll(perm.getIpRanges());
                    }
                }
            }
            return ipPermissions;
        } finally {
            client.shutdown();
        }
    }

    protected AmazonAutoScaling getAutoScalingClient() {
        AmazonAutoScaling client = new AmazonAutoScalingClient(provider);
        client.setRegion(amazonConfiguration.getRegion());
        return client;
    }

    protected AmazonEC2 getEc2Client() {
        AmazonEC2 client = new AmazonEC2Client(provider);
        client.setRegion(amazonConfiguration.getRegion());
        return client;
    }
}
