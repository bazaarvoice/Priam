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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;
import com.amazonaws.services.simpledb.model.UpdateCondition;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.identity.PriamInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * DAO for handling Instance identity information such as token, zone, region
 */
@Singleton
public class SDBInstanceData {
    private static final Logger logger = LoggerFactory.getLogger(SDBInstanceData.class);

    public static class Attributes {
        public final static String APP_ID = "appId";
        public final static String ID = "id";
        public final static String INSTANCE_ID = "instanceId";
        public final static String TOKEN = "token";
        public final static String AVAILABILITY_ZONE = "availabilityZone";
        public final static String ELASTIC_IP = "elasticIP";
        public final static String UPDATE_TS = "updateTimestamp";
        public final static String LOCATION = "location";
        public final static String HOSTNAME = "hostname";
    }

    private final AWSCredentialsProvider provider;
    private final Region sdbRegion;
    private final String sdbDomain;

    @Inject
    public SDBInstanceData(AWSCredentialsProvider provider, AmazonConfiguration amazonConfiguration) {
        this.provider = provider;
        this.sdbRegion = RegionUtils.getRegion(amazonConfiguration.getSimpleDbRegion());
        this.sdbDomain = amazonConfiguration.getSimpleDbDomain();

        createDomain();  // This is idempotent and won't affect the domain if it already exists
    }

    private String getAllApplicationsQuery() {
        return String.format("select " + Attributes.APP_ID + " from %s", sdbDomain);
    }

    private String getAllQuery(String appId) {
        return String.format("select * from %s where " + Attributes.APP_ID + "='%s'", sdbDomain, appId);
    }

    private String getInstanceQuery(String appId, int id) {
        return String.format("select * from %s where " + Attributes.APP_ID + "='%s' and " + Attributes.ID + "='%d'", sdbDomain, appId, id);
    }

    private void createDomain() {
        logger.info("Creating SimpleDB domain '{}'", sdbDomain);
        AmazonSimpleDB simpleDBClient = getSimpleDBClient();
        CreateDomainRequest request = new CreateDomainRequest(sdbDomain);
        simpleDBClient.createDomain(request);
    }

    /**
     * Get the instance details from SimpleDB
     *
     * @param app Cluster name
     * @param id  Node ID
     * @return the node with the given {@code id}, or {@code null} if no such node exists
     */
    public PriamInstance getInstance(String app, int id) {
        return getInstance(app, id, false);
    }

    /**
     * Get the instance details from SimpleDB
     *
     * @param app Cluster name
     * @param id  Node ID
     * @param consistentRead  Whether to require strong consistency on the read
     * @return the node with the given {@code id}, or {@code null} if no such node exists
     */
    public PriamInstance getInstance(String app, int id, boolean consistentRead) {
        AmazonSimpleDB simpleDBClient = getSimpleDBClient();
        SelectRequest request = new SelectRequest(getInstanceQuery(app, id));
        request.setConsistentRead(consistentRead);
        SelectResult result = simpleDBClient.select(request);
        if (result.getItems().size() == 0) {
            return null;
        }
        PriamInstance priamInstance = transform(result.getItems().get(0));
        logger.info("Retrieved instance from SimpleDB: {}", priamInstance);
        return priamInstance;
    }

    /**
     * Get the set of all nodes in the cluster
     *
     * @param app Cluster name
     * @return the set of all instances in the given {@code app}
     */
    public Set<PriamInstance> getAllIds(String app) {
        return getAllIds(app, true);
    }

    /**
     * Get the set of all nodes in the cluster
     *
     * @param app Cluster name
     * @param consistentRead  Whether to require strong consistency on the read
     * @return the set of all instances in the given {@code app}
     */
    public Set<PriamInstance> getAllIds(String app, boolean consistentRead) {
        AmazonSimpleDB simpleDBClient = getSimpleDBClient();
        Set<PriamInstance> inslist = new HashSet<>();
        String nextToken = null;
        String allQuery = getAllQuery(app);
        do {
            SelectRequest request = new SelectRequest(allQuery);
            request.setConsistentRead(consistentRead);
            request.setNextToken(nextToken);
            SelectResult result = simpleDBClient.select(request);
            nextToken = result.getNextToken();
            for (Item item : result.getItems()) {
                PriamInstance priamInstance = transform(item);
                logger.info("Retrieved PriamInstance from app '{}': {}", app, priamInstance);
                inslist.add(priamInstance);
            }
        } while (nextToken != null);
        return inslist;
    }

    /**
     * Create a new instance entry in SimpleDB
     *
     * @throws AmazonServiceException
     */
    public void createInstance(PriamInstance instance) throws AmazonServiceException {
        logger.info("Creating PriamInstance in SimpleDB: {}", instance);
        AmazonSimpleDB simpleDBClient = getSimpleDBClient();
        PutAttributesRequest putReq = new PutAttributesRequest(sdbDomain, getKey(instance), createAttributesToRegister(instance));
        simpleDBClient.putAttributes(putReq);
    }

    /**
     * Register a new instance. Registration will fail if a prior entry exists
     *
     * @throws AmazonServiceException
     */
    public void registerInstance(PriamInstance instance, String expectedPreviousInstanceId) throws AmazonServiceException {
        logger.info("Registering PriamInstance in SimpleDB: {}", instance);
        AmazonSimpleDB simpleDBClient = getSimpleDBClient();
        PutAttributesRequest putReq = new PutAttributesRequest(sdbDomain, getKey(instance), createAttributesToRegister(instance));
        UpdateCondition expected = new UpdateCondition();
        expected.setName(Attributes.INSTANCE_ID);
        if (expectedPreviousInstanceId == null) {
            expected.setExists(false);
        } else {
            expected.setValue(expectedPreviousInstanceId);
        }
        putReq.setExpected(expected);
        simpleDBClient.putAttributes(putReq);
    }

    /**
     * Deregister instance (same as delete)
     *
     * @throws AmazonServiceException
     */
    public void deregisterInstance(PriamInstance instance) throws AmazonServiceException {
        logger.info("De-Registering PriamInstance from SimpleDB: {}", instance);
        AmazonSimpleDB simpleDBClient = getSimpleDBClient();
        DeleteAttributesRequest delReq = new DeleteAttributesRequest(sdbDomain, getKey(instance));
        simpleDBClient.deleteAttributes(delReq);
    }

    /**
     * List all the applications in SimpleDB.
     *
     * @throws AmazonServiceException
     */
    public Set<String> getAllAppIds() throws AmazonServiceException {
        logger.info("Listing all PriamInstance applications in SimpleDB.");
        AmazonSimpleDB simpleDBClient = getSimpleDBClient();
        Set<String> appIds = new HashSet<>();
        String nextToken = null;
        String allQuery = getAllApplicationsQuery();
        do {
            SelectRequest request = new SelectRequest(allQuery);
            request.setNextToken(nextToken);
            SelectResult result = simpleDBClient.select(request);
            nextToken = result.getNextToken();
            for (Item item : result.getItems()) {
                PriamInstance priamInstance = transform(item);
                appIds.add(priamInstance.getApp());
            }
        } while (nextToken != null);
        return appIds;
    }

    private List<ReplaceableAttribute> createAttributesToRegister(PriamInstance instance) {
        instance.setUpdatetime(new Date().getTime());
        List<ReplaceableAttribute> attrs = new ArrayList<>();
        attrs.add(new ReplaceableAttribute(Attributes.INSTANCE_ID, instance.getInstanceId(), true));
        attrs.add(new ReplaceableAttribute(Attributes.TOKEN, instance.getToken(), true));
        attrs.add(new ReplaceableAttribute(Attributes.APP_ID, instance.getApp(), true));
        attrs.add(new ReplaceableAttribute(Attributes.ID, Integer.toString(instance.getId()), true));
        attrs.add(new ReplaceableAttribute(Attributes.AVAILABILITY_ZONE, instance.getAvailabilityZone(), true));
        attrs.add(new ReplaceableAttribute(Attributes.ELASTIC_IP, instance.getHostIP(), true));
        attrs.add(new ReplaceableAttribute(Attributes.HOSTNAME, instance.getHostName(), true));
        attrs.add(new ReplaceableAttribute(Attributes.LOCATION, instance.getRegionName(), true));
        attrs.add(new ReplaceableAttribute(Attributes.UPDATE_TS, Long.toString(instance.getUpdatetime()), true));
        return attrs;
    }

    /**
     * Convert a simpledb item to PriamInstance
     */
    private PriamInstance transform(Item item) {
        PriamInstance ins = new PriamInstance();
        for (Attribute att : item.getAttributes()) {
            switch (att.getName()) {
                case Attributes.INSTANCE_ID:
                    ins.setInstanceId(att.getValue());
                    break;
                case Attributes.TOKEN:
                    ins.setToken(att.getValue());
                    break;
                case Attributes.APP_ID:
                    ins.setApp(att.getValue());
                    break;
                case Attributes.ID:
                    ins.setId(Integer.parseInt(att.getValue()));
                    break;
                case Attributes.AVAILABILITY_ZONE:
                    ins.setAvailabilityZone(att.getValue());
                    break;
                case Attributes.ELASTIC_IP:
                    ins.setHostIP(att.getValue());
                    break;
                case Attributes.HOSTNAME:
                    ins.setHost(att.getValue());
                    break;
                case Attributes.LOCATION:
                    ins.setRegionName(att.getValue());
                    break;
                case Attributes.UPDATE_TS:
                    ins.setUpdatetime(Long.parseLong(att.getValue()));
                    break;
            }
        }
        return ins;
    }

    private String getKey(PriamInstance instance) {
        return instance.getApp() + instance.getId();
    }

    private AmazonSimpleDB getSimpleDBClient() {
        //Create per request
        AmazonSimpleDB client = new AmazonSimpleDBClient(provider);
        client.setRegion(sdbRegion);
        return client;
    }
}
