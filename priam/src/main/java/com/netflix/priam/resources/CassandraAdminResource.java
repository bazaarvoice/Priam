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
package com.netflix.priam.resources;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import com.netflix.priam.ICassandraProcess;
import com.netflix.priam.PriamServer;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.config.PriamConfiguration;
import com.netflix.priam.utils.JMXConnectionException;
import com.netflix.priam.utils.JMXNodeTool;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.GenericType;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamState;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Do general operations. Start/Stop and some JMX node tool commands
 */
@Path("/v1/cassadmin")
@Produces(MediaType.APPLICATION_JSON)
public class CassandraAdminResource {
    private static final Map<String, String> RESULT_OK = ImmutableMap.of("result", "ok");

    private static final Logger logger = LoggerFactory.getLogger(CassandraAdminResource.class);

    enum HintsState {OK, UNREACHABLE, ERROR}

    private final PriamServer priamServer;
    private final CassandraConfiguration cassandraConfiguration;
    private final PriamConfiguration priamConfiguration;
    private final ICassandraProcess cassProcess;
    private final Client jersey;
    private final Integer port;

    @Inject
    public CassandraAdminResource(PriamServer priamServer, CassandraConfiguration cassandraConfiguration,
                                  PriamConfiguration priamConfiguration, ICassandraProcess cassProcess, Client jersey, HostAndPort hostAndPort) {
        this.priamServer = priamServer;
        this.cassandraConfiguration = cassandraConfiguration;
        this.priamConfiguration = priamConfiguration;
        this.cassProcess = cassProcess;
        this.jersey = jersey;
        this.port = hostAndPort.getPort();
    }

    private JMXNodeTool getNodeTool() {
        try {
            return JMXNodeTool.instance(cassandraConfiguration);
        } catch (JMXConnectionException e) {
            throw new WebApplicationException(Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("JMXConnectionException")
                    .build());
        }
    }

    @GET
    @Path("/start")
    public Response cassStart() throws IOException, InterruptedException {
        cassProcess.start(true);
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/stop")
    public Response cassStop() throws IOException, InterruptedException {
        cassProcess.stop();
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/refresh")
    public Response cassRefresh(@QueryParam("keyspaces") String keyspaces) throws Exception {
        logger.info("node tool refresh is being called");
        if (StringUtils.isBlank(keyspaces)) {
            return Response.status(400).entity("Missing keyspace in request").build();
        }

        JMXNodeTool nodetool = getNodeTool();
        nodetool.refresh(Lists.newArrayList(keyspaces.split(",")));
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/info")
    public Response cassInfo() throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        logger.info("node tool info being called");
        return Response.ok(nodetool.info(), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/estimateKeys")
    public Response estimateKeys(@QueryParam("keyspaces") String keyspaces) throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        Optional<Collection<String>> keyspaceCollection = StringUtils.isBlank(keyspaces) ?
                Optional.<Collection<String>>absent() :
                Optional.<Collection<String>>of(Lists.newArrayList(keyspaces.split(",")));
        return Response.ok(nodetool.estimateKeys(keyspaceCollection), MediaType.APPLICATION_JSON).build();
    }

    /**
     * Returns hints info for the entire ring.
     * Includes all nodes in the ring along with their state, and total hints.
     *
     * @throws Exception
     */
    @GET
    @Path("/hints/ring")
    public Response cassHintsInRing() throws Exception {
        List<Map<String, Object>> ring = getNodeTool().ring();
        List<Map<String, Object>> hintsInfo = Lists.newArrayList();
        String selfIP = priamServer.getInstanceIdentity().getInstance().getHostIP();
        for (Map<String, Object> node : ring) {
            String endpoint = node.get("endpoint").toString();

            try {
                // Is this node down?
                if (!node.get("status").toString().equalsIgnoreCase("up")) {
                    hintsInfo.add(ImmutableMap.<String, Object>of(
                            "endpoint", endpoint,
                            "state", HintsState.UNREACHABLE));
                    continue;
                }

                Map<String, Object> fullNodeInfo = Maps.newLinkedHashMap();
                // Do not make an outbound request to yourself
                if (endpoint.equals(selfIP)) {
                    Map<String, Object> nodeResponse = endpointsPendingHints();
                    fullNodeInfo.putAll(nodeResponse);
                } else {
                    String url = String.format("http://%s:%s/v1/cassadmin/hints/node", endpoint, port);
                    Map<String, Object> nodeResponse = jersey.resource(url)
                            .get(new GenericType<Map<String, Object>>() {
                            });

                    fullNodeInfo.putAll(nodeResponse);
                }
                fullNodeInfo.put("endpoint", endpoint);
                fullNodeInfo.put("state", HintsState.OK);
                hintsInfo.add(fullNodeInfo);

            } catch (Exception e) {
                hintsInfo.add(ImmutableMap.<String, Object>of(
                        "endpoint", endpoint,
                        "state", HintsState.ERROR,
                        "exception", e.toString()));
            }
        }

        return Response.ok(hintsInfo, MediaType.APPLICATION_JSON).build();
    }

    /**
     * This method will return hints info for this node only
     *
     * @throws Exception
     */
    @GET
    @Path("/hints/node")
    public Response cassHintsInNode() throws Exception {
        return Response.ok(endpointsPendingHints(), MediaType.APPLICATION_JSON).build();
    }

    public Map<String, Object> endpointsPendingHints() throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        return ImmutableMap.<String, Object>of("totalEndpointsPendingHints", nodetool.totalEndpointsPendingHints());
    }

    @GET
    @Path("/ring/{keyspace}")
    public Response cassRing(@PathParam("keyspace") String keyspace) throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        logger.info("node tool ring being called");
        return Response.ok(nodetool.ring(keyspace), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/ring")
    public Response cassRingAllKeyspaces(@PathParam("keyspace") String keyspace) throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        logger.info("node tool ring being called");
        return Response.ok(nodetool.ring(), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/flush")
    public Response cassFlush() throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        logger.info("node tool flush being called");
        nodetool.flush();
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/compact")
    public Response cassCompact() throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        logger.info("node tool compact being called");
        nodetool.compact();
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/cleanup")
    public Response cassCleanup() throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        logger.info("node tool cleanup being called");
        nodetool.cleanup();
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/repair")
    public Response cassRepair(@QueryParam("sequential") boolean isSequential,
                               @QueryParam("localDC") boolean localDCOnly,
                               @QueryParam("primaryRange") boolean primaryRange) throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        logger.info("node tool repair being called");
        nodetool.repair(isSequential, localDCOnly, primaryRange);
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/version")
    public Response version() throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        return Response.ok(ImmutableMap.of("version", nodetool.getReleaseVersion()), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/compactionstats")
    public Response compactionStats() throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        Map<String, Object> rootObj = Maps.newLinkedHashMap();
        CompactionManagerMBean cm = nodetool.getCompactionManagerProxy();
        rootObj.put("pending tasks", cm.getCompactions());
        List<Map<String, Object>> compStats = Lists.newArrayList();
        for (Map<String, String> c : cm.getCompactions()) {
            Map<String, Object> cObj = Maps.newLinkedHashMap();
            cObj.put("id", c.get("id"));
            cObj.put("keyspace", c.get("keyspace"));
            cObj.put("columnfamily", c.get("columnfamily"));
            cObj.put("bytesComplete", c.get("bytesComplete"));
            cObj.put("totalBytes", c.get("totalBytes"));
            cObj.put("taskType", c.get("taskType"));
            String percentComplete = new Long(c.get("totalBytes")) == 0 ? "n/a" : new DecimalFormat("0.00").format((double) new Long(c.get("bytesComplete")) / new Long(c.get("totalBytes")) * 100) + "%";
            cObj.put("progress", percentComplete);
            compStats.add(cObj);
        }
        rootObj.put("compaction stats", compStats);
        return Response.ok(rootObj, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/disablegossip")
    public Response disablegossip() throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        nodetool.stopGossiping();
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/enablegossip")
    public Response enablegossip() throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        nodetool.startGossiping();
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/disablethrift")
    public Response disablethrift() throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        nodetool.stopThriftServer();
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/enablethrift")
    public Response enablethrift() throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        nodetool.startThriftServer();
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/statusthrift")
    public Response statusthrift() throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        return Response.ok(ImmutableMap.of("status", (nodetool.isThriftServerRunning() ? "running" : "not running")), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/pingthrift")
    public Response pingthrift() throws IOException {
        try {
            JMXNodeTool nodetool = getNodeTool();
            if (nodetool.isThriftServerRunning()) {
                return Response.ok().build();
            }
        } catch (Exception e) {
            // Fall through
        }
        return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }

    @GET
    @Path("/gossipinfo")
    public Response gossipinfo() throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        Map<String, Object> rootObj = Maps.newLinkedHashMap();
        String[] ginfo = nodetool.getGossipInfo().split("/");
        for (String info : ginfo) {
            String[] data = info.split("\n");
            String key = "";
            Map<String, Object> obj = Maps.newLinkedHashMap();
            for (String element : data) {
                String[] kv = element.split(":");
                if (kv.length == 1) {
                    key = kv[0];
                } else {
                    obj.put(kv[0], kv[1]);
                }
            }
            if (StringUtils.isNotBlank(key)) {
                rootObj.put(key, obj);
            }
        }
        return Response.ok(rootObj, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/netstats")
    public Response netstats() throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        Map<String, Object> rootObj = Maps.newLinkedHashMap();
        rootObj.put("mode", nodetool.getOperationMode());

        // Collect Sending Netstats
        Set<StreamState> netstats = nodetool.getStreamStatus();

        for (StreamState streamState : netstats) {
            final Set<SessionInfo> streamSessions = streamState.sessions;
            for (SessionInfo streamSession : streamSessions) {
                final Collection<ProgressInfo> sendingFiles = streamSession.getSendingFiles();
                final Collection<ProgressInfo> receivingFiles = streamSession.getReceivingFiles();
                final String connectingHost = streamSession.peer.getHostName();
                final Set<Collection<ProgressInfo>> streams = rootObj.containsKey(connectingHost) ? (Set<Collection<ProgressInfo>>) rootObj.get(connectingHost) : Sets.<Collection<ProgressInfo>>newHashSet();
                streams.add(sendingFiles);
                streams.add(receivingFiles);
                rootObj.put(connectingHost, streams);
            }
        }

        return Response.ok(rootObj, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/move")
    public Response moveToken(@QueryParam("token") String newToken)
            throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        nodetool.move(newToken);
        priamServer.getInstanceIdentity().updateToken();
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/scrub")
    public Response scrub(@QueryParam("keyspaces") String keyspaces, @QueryParam("cfnames") String cfnames)
            throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        String[] cfs = null;
        if (StringUtils.isNotBlank(cfnames)) {
            cfs = cfnames.split(",");
        }
        if (cfs == null) {
            nodetool.scrub(false, false, false, keyspaces);
        } else {
            nodetool.scrub(false, false, false, keyspaces, cfs);
        }
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/drain")
    public Response cassDrain() throws Exception {
        JMXNodeTool nodetool = getNodeTool();
        logger.debug("node tool drain being called");
        nodetool.drain();
        return Response.ok(RESULT_OK, MediaType.APPLICATION_JSON).build();
    }
}
