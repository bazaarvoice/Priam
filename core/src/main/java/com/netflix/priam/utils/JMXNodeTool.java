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

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.CassandraConfiguration;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.HintedHandOffManagerMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;


/**
 * Class to get data out of Cassandra JMX
 */
@Singleton
public class JMXNodeTool extends NodeProbe {
    private static final Logger logger = LoggerFactory.getLogger(JMXNodeTool.class);
    private static volatile JMXNodeTool tool = null;
    private MBeanServerConnection mbeanServerConn = null;

    /**
     * Hostname and Port to talk to will be same server for now optionally we
     * might want the ip to poll.
     * <p/>
     * NOTE: This class shouldn't be a singleton and this shouldn't be cached.
     * <p/>
     * This will work only if cassandra runs.
     */
    public JMXNodeTool(String host, int port) throws IOException, InterruptedException {
        super(host, port);
    }

    @Inject
    public JMXNodeTool(CassandraConfiguration cassandraConfiguration) throws IOException, InterruptedException {
        super("localhost", cassandraConfiguration.getJmxPort());
    }

    /**
     * try to create if it is null.
     */
    public static JMXNodeTool instance(CassandraConfiguration config) throws JMXConnectionException {
        if (!testConnection()) {
            reconnect(config);
        }
        return tool;
    }

    /**
     * This method will test if you can connect and query something before handing over the connection,
     * This is required for our retry logic.
     */
    private static boolean testConnection() {
        // connecting first time hence return false.
        if (tool == null) {
            return false;
        }

        try {
            tool.isInitialized();
        } catch (Throwable ex) {
            SystemUtils.closeQuietly(tool);
            return false;
        }
        return true;
    }

    private static synchronized void reconnect(CassandraConfiguration config) throws JMXConnectionException {
        // Recheck connection in case we were beaten to the punch by another reconnect call.
        if (testConnection()) {
            return;
        }
        tool = connect(config);
    }

    public static synchronized JMXNodeTool connect(final CassandraConfiguration config) throws JMXConnectionException {
        try {
            return new BoundedExponentialRetryCallable<JMXNodeTool>() {
                @Override
                public JMXNodeTool retriableCall() throws Exception {
                    JMXNodeTool nodetool = new JMXNodeTool("localhost", config.getJmxPort());
                    Field fields[] = NodeProbe.class.getDeclaredFields();
                    for (Field field : fields) {
                        if (!field.getName().equals("mbeanServerConn")) {
                            continue;
                        }
                        field.setAccessible(true);
                        nodetool.mbeanServerConn = (MBeanServerConnection) field.get(nodetool);
                    }
                    return nodetool;
                }
            }.call();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new JMXConnectionException(e.toString());
        }
    }

    /**
     * You must do the compaction before running this to get an accurate number.  Otherwise the result
     * will likely significantly overestimate the actual number of keys.
     */
    public List<Map<String, Object>> estimateKeys(Optional<Collection<String>> keyspaces) {
        Iterator<Entry<String, ColumnFamilyStoreMBean>> it = getColumnFamilyStoreMBeanProxies();
        List<Map<String, Object>> list = Lists.newArrayList();
        while (it.hasNext()) {
            Entry<String, ColumnFamilyStoreMBean> entry = it.next();
            if (!keyspaces.isPresent() || keyspaces.get().contains(entry.getKey())) {
                list.add(ImmutableMap.<String, Object>builder()
                        .put("keyspace", entry.getKey())
                        .put("column_family", entry.getValue().getColumnFamilyName())
                        .put("estimated_size", entry.getValue().estimateKeys())
                        .build());
            }
        }
        return list;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> info() {
        logger.info("JMX info being called");
        Map<String, Object> object = Maps.newLinkedHashMap();
        object.put("gossip_active", isInitialized());
        object.put("thrift_active", isThriftServerRunning());
        object.put("token", getTokens().toString());
        object.put("load", getLoadString());
        object.put("generation_no", getCurrentGenerationNumber());
        object.put("uptime", getUptime() / 1000);
        MemoryUsage heapUsage = getHeapMemoryUsage();
        double memUsed = (double) heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double) heapUsage.getMax() / (1024 * 1024);
        object.put("heap_memory_mb", memUsed + "/" + memMax);
        object.put("data_center", getDataCenter());
        object.put("rack", getRack());
        logger.info(object.toString());
        return object;
    }

    @SuppressWarnings("unchecked")
    public long totalEndpointsPendingHints()
            throws MalformedObjectNameException {
        ObjectName name = new ObjectName("org.apache.cassandra.db:type=HintedHandoffManager");
        HintedHandOffManagerMBean hintedHandoffManager = JMX.newMBeanProxy(mbeanServerConn, name, HintedHandOffManagerMBean.class);
        long totalEndpointsPendingHints = hintedHandoffManager.listEndpointsPendingHints().size();
        logger.info("Total endpoints pending hints: {}", totalEndpointsPendingHints);
        return totalEndpointsPendingHints;
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> ring() {
        return ring(null);
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> ring(String keyspace) {
        logger.info("JMX ring being called");
        List<Map<String, Object>> ring = Lists.newArrayList();
        Map<String, String> tokenToEndpoint = getTokenToEndpointMap();
        List<String> sortedTokens = new ArrayList<>(tokenToEndpoint.keySet());

        Collection<String> liveNodes = getLiveNodes();
        Collection<String> deadNodes = getUnreachableNodes();
        Collection<String> joiningNodes = getJoiningNodes();
        Collection<String> leavingNodes = getLeavingNodes();
        Collection<String> movingNodes = getMovingNodes();
        Map<String, String> loadMap = getLoadMap();

        // Calculate per-token ownership of the ring
        Map<InetAddress, Float> ownerships;
        if (Strings.isNullOrEmpty(keyspace)) {
            ownerships = getOwnership();
        } else {
            ownerships = effectiveOwnership(keyspace);
        }

        for (String token : sortedTokens) {
            String primaryEndpoint = tokenToEndpoint.get(token);
            String dataCenter;
            try {
                dataCenter = getEndpointSnitchInfoProxy().getDatacenter(primaryEndpoint);
            } catch (UnknownHostException e) {
                dataCenter = "Unknown";
            }
            String rack;
            try {
                rack = getEndpointSnitchInfoProxy().getRack(primaryEndpoint);
            } catch (UnknownHostException e) {
                rack = "Unknown";
            }
            String status = liveNodes.contains(primaryEndpoint)
                    ? "Up"
                    : deadNodes.contains(primaryEndpoint)
                    ? "Down"
                    : "?";

            String state = "Normal";

            if (joiningNodes.contains(primaryEndpoint)) {
                state = "Joining";
            } else if (leavingNodes.contains(primaryEndpoint)) {
                state = "Leaving";
            } else if (movingNodes.contains(primaryEndpoint)) {
                state = "Moving";
            }

            String load = Objects.firstNonNull(loadMap.get(primaryEndpoint), "?");
            // TODO: ownerships is keyed by InetAddress, lookup is by String
            String owns = new DecimalFormat("##0.00%").format(Objects.firstNonNull(ownerships.get(primaryEndpoint), 0.0F));
            ring.add(createJson(primaryEndpoint, dataCenter, rack, status, state, load, owns, token));
        }
        logger.info(ring.toString());
        return ring;
    }

    private Map<String, Object> createJson(String primaryEndpoint, String dataCenter, String rack, String status, String state, String load, String owns, String token) {
        Map<String, Object> object = Maps.newLinkedHashMap();
        object.put("endpoint", primaryEndpoint);
        object.put("dc", dataCenter);
        object.put("rack", rack);
        object.put("status", status);
        object.put("state", state);
        object.put("load", load);
        object.put("owns", owns);
        object.put("token", token);
        return object;
    }

    public void compact() throws IOException, ExecutionException, InterruptedException {
        for (String keyspace : getKeyspaces()) {
            forceTableCompaction(keyspace);
        }
    }

    public void repair(boolean isSequential, boolean localDataCenterOnly, boolean primaryRange) throws IOException {
        for (String keyspace : getKeyspaces()) {
            repair(keyspace, isSequential, localDataCenterOnly, primaryRange);
        }
    }

    public void repair(String keyspace, boolean isSequential, boolean localDataCenterOnly, boolean primaryRange) throws IOException {
        if (primaryRange) {
            forceTableRepairPrimaryRange(keyspace, isSequential, localDataCenterOnly);
        } else {
            forceTableRepair(keyspace, isSequential, localDataCenterOnly);
        }
    }

    public void cleanup() throws IOException, ExecutionException, InterruptedException {
        for (String keyspace : getKeyspaces()) {
            if ("system".equalsIgnoreCase(keyspace)) {
                continue; // It is an error to attempt to cleanup the system column family.
            }
            forceTableCleanup(keyspace);
        }
    }

    public void flush() throws IOException, ExecutionException, InterruptedException {
        for (String keyspace : getKeyspaces()) {
            forceTableFlush(keyspace);
        }
    }

    public void refresh(List<String> keyspaces) throws IOException {
        Iterator<Entry<String, ColumnFamilyStoreMBean>> it = getColumnFamilyStoreMBeanProxies();
        while (it.hasNext()) {
            Entry<String, ColumnFamilyStoreMBean> entry = it.next();
            if (keyspaces.contains(entry.getKey())) {
                logger.info("Refreshing {} {}", entry.getKey(), entry.getValue().getColumnFamilyName());
                loadNewSSTables(entry.getKey(), entry.getValue().getColumnFamilyName());
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (JMXNodeTool.class) {
            tool = null;
            super.close();
        }
    }
}
