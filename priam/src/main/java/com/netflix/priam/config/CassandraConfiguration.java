package com.netflix.priam.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Strings;

import java.util.Map;

public class CassandraConfiguration {
    @JsonProperty
    private String partitioner = "org.apache.cassandra.dht.RandomPartitioner";

    @JsonProperty
    private boolean autoBootstrap = true;

    @JsonProperty
    private int tokenLength = 16;  // in bytes

    @JsonProperty
    private String minimumToken;

    @JsonProperty
    private String maximumToken;

    @JsonProperty
    private String endpointSnitch;

    @JsonProperty
    private String cassHome;

    @JsonProperty
    private String yamlLocation;

    @JsonProperty
    private String cassStartScript;

    @JsonProperty
    private String cassStopScript;

    @JsonProperty
    private String clusterName;

    @JsonProperty
    private String dataLocation;

    @JsonProperty
    private int sslStoragePort;

    @JsonProperty
    private int storagePort;

    @JsonProperty
    private boolean thriftEnabled;

    @JsonProperty
    private int thriftPort;

    @JsonProperty
    private boolean nativeTransportEnabled;

    @JsonProperty
    private int nativeTransportPort;

    @JsonProperty
    private int jmxPort;

    @JsonProperty
    private int compactionThroughputMBPerSec;

    @JsonProperty
    private int inMemoryCompactionLimitMB;

    @JsonProperty
    private Map<String, String> maxHeapSize;

    @JsonProperty
    private Map<String, String> maxNewGenHeapSize;

    @JsonProperty
    private String heapDumpLocation;

    @JsonProperty
    private Integer memtableTotalSpaceMB;

    @JsonProperty
    private Integer streamingThroughputMbps;

    @JsonProperty
    private long hintedHandoffThrottleKB;

    @JsonProperty
    private long readRequestTimeoutInMs = 10000L;

    @JsonProperty
    private long rangeRequestTimeoutInMs = 10000L;

    @JsonProperty
    private long writeRequestTimeoutInMs = 10000L;

    @JsonProperty
    private long requestTimeoutInMs = 10000L;

    @JsonProperty
    private long maxHintWindowMS;

    @JsonProperty
    private String rpcServerType;

    @JsonProperty
    private int indexInterval;

    @JsonProperty
    private String authenticator;

    @JsonProperty
    private String authorizer;

    @JsonProperty
    private String cacheLocation;

    @JsonProperty
    private String commitLogLocation;

    @JsonProperty
    private String seedProviderClassName;

    @JsonProperty
    private Integer keyCacheSizeInMB;

    @JsonProperty
    private Integer keyCacheKeysToSave;

    @JsonProperty
    private Integer rowCacheSizeInMB;

    @JsonProperty
    private Integer rowCacheKeysToSave;

    @JsonProperty
    private String internodeCompression;

    @JsonProperty
    private boolean interDcTcpNodelay;

    @JsonProperty
    private int concurrentReads;

    @JsonProperty
    private int concurrentWrites;

    @JsonProperty
    private Integer concurrentCompactors;

    @JsonProperty
    private boolean clientSslEnabled;

    @JsonProperty
    private String internodeEncryption;

    @JsonProperty
    private String nodeRepairTime;

    @JsonProperty
    private boolean nodeRepairEnabled;

    @JsonProperty
    private int nodeRepairMutexAcquireTimeOut;

    @JsonProperty
    private Map<String, String> extraConfigParams;

    public String getPartitioner() {
        return partitioner;
    }

    public boolean getAutoBootstrap() {
        return autoBootstrap;
    }

    public int getTokenLength() {
        return tokenLength;
    }

    public String getMinimumToken() {
        return Objects.firstNonNull(minimumToken, Strings.repeat("00", tokenLength));
    }

    public String getMaximumToken() {
        return Objects.firstNonNull(maximumToken, Strings.repeat("ff", tokenLength));
    }

    public String getEndpointSnitch() {
        return endpointSnitch;
    }

    public String getCassHome() {
        return cassHome;
    }

    public String getYamlLocation() {
        // Use a sensible default for the YAML location unless our configuration specifically overrides it
        return Objects.firstNonNull(yamlLocation, cassHome + "/conf/cassandra.yaml");
    }

    public String getCassStartScript() {
        return cassStartScript;
    }

    public String getCassStopScript() {
        return cassStopScript;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getDataLocation() {
        return dataLocation;
    }

    public int getSslStoragePort() {
        return sslStoragePort;
    }

    public int getStoragePort() {
        return storagePort;
    }

    public boolean isThriftEnabled() {
        return thriftEnabled;
    }

    public int getThriftPort() {
        return thriftPort;
    }

    public boolean isNativeTransportEnabled() {
        return nativeTransportEnabled;
    }

    public int getNativeTransportPort() {
        return nativeTransportPort;
    }

    public int getJmxPort() {
        return jmxPort;
    }

    public int getCompactionThroughputMBPerSec() {
        return compactionThroughputMBPerSec;
    }

    public int getInMemoryCompactionLimitMB() {
        return inMemoryCompactionLimitMB;
    }

    public Map<String, String> getMaxHeapSize() {
        return maxHeapSize;
    }

    public Map<String, String> getMaxNewGenHeapSize() {
        return maxNewGenHeapSize;
    }

    public String getHeapDumpLocation() {
        return heapDumpLocation;
    }

    public Integer getMemtableTotalSpaceMB() {
        return memtableTotalSpaceMB;
    }

    public Integer getStreamingThroughputMbps() {
        return streamingThroughputMbps;
    }

    public long getHintedHandoffThrottleKB() {
        return hintedHandoffThrottleKB;
    }

    public long getMaxHintWindowMS() {
        return maxHintWindowMS;
    }

    public String getRpcServerType() {
        return rpcServerType;
    }

    public int getIndexInterval() {
        return indexInterval;
    }

    public String getAuthenticator() {
        return authenticator;
    }

    public String getAuthorizer() {
        return authorizer;
    }

    public String getCacheLocation() {
        return cacheLocation;
    }

    public String getCommitLogLocation() {
        return commitLogLocation;
    }

    public String getSeedProviderClassName() {
        return seedProviderClassName;
    }

    public Integer getKeyCacheSizeInMB() {
        return keyCacheSizeInMB;
    }

    public Integer getKeyCacheKeysToSave() {
        return keyCacheKeysToSave;
    }

    public Integer getRowCacheSizeInMB() {
        return rowCacheSizeInMB;
    }

    public Integer getRowCacheKeysToSave() {
        return rowCacheKeysToSave;
    }

    public String getInternodeCompression() {
        return internodeCompression;
    }

    public boolean isInterDcTcpNodelay() {
        return interDcTcpNodelay;
    }

    public int getConcurrentReads() {
        return concurrentReads;
    }

    public int getConcurrentWrites() {
        return concurrentWrites;
    }

    public Integer getConcurrentCompactors() {
        return concurrentCompactors;
    }

    public boolean isClientSslEnabled() {
        return clientSslEnabled;
    }

    public String getInternodeEncryption() {
        return internodeEncryption;
    }

    public String getNodeRepairTime() {
        return nodeRepairTime;
    }

    public boolean isNodeRepairEnabled() {
        return nodeRepairEnabled;
    }

    public int getNodeRepairMutexAcquireTimeOut() {
        return nodeRepairMutexAcquireTimeOut;
    }

    public Map<String, String> getExtraConfigParams() {
        return extraConfigParams;
    }

    public void setPartitioner(String partitioner) {
        this.partitioner = partitioner;
    }

    public void setAutoBootstrap(boolean autoBootstrap) {
        this.autoBootstrap = autoBootstrap;
    }

    public void setTokenLength(int tokenLength) {
        this.tokenLength = tokenLength;
    }

    public void setMinimumToken(String minimumToken) {
        this.minimumToken = minimumToken;
    }

    public void setMaximumToken(String maximumToken) {
        this.maximumToken = maximumToken;
    }

    public void setEndpointSnitch(String endpointSnitch) {
        this.endpointSnitch = endpointSnitch;
    }

    public void setCassHome(String cassHome) {
        this.cassHome = cassHome;
    }

    public void setYamlLocation(String yamlLocation) {
        this.yamlLocation = yamlLocation;
    }

    public void setCassStartScript(String cassStartScript) {
        this.cassStartScript = cassStartScript;
    }

    public void setCassStopScript(String cassStopScript) {
        this.cassStopScript = cassStopScript;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setDataLocation(String dataLocation) {
        this.dataLocation = dataLocation;
    }

    public void setSslStoragePort(int sslStoragePort) {
        this.sslStoragePort = sslStoragePort;
    }

    public void setStoragePort(int storagePort) {
        this.storagePort = storagePort;
    }

    public void setThriftEnabled(boolean thriftEnabled) {
        this.thriftEnabled = thriftEnabled;
    }

    public void setThriftPort(int thriftPort) {
        this.thriftPort = thriftPort;
    }

    public void setNativeTransportEnabled(boolean nativeTransportEnabled) {
        this.nativeTransportEnabled = nativeTransportEnabled;
    }

    public void setNativeTransportPort(int nativeTransportPort) {
        this.nativeTransportPort = nativeTransportPort;
    }

    public void setJmxPort(int jmxPort) {
        this.jmxPort = jmxPort;
    }

    public void setCompactionThroughputMBPerSec(int compactionThroughputMBPerSec) {
        this.compactionThroughputMBPerSec = compactionThroughputMBPerSec;
    }

    public void setInMemoryCompactionLimitMB(int inMemoryCompactionLimitMB) {
        this.inMemoryCompactionLimitMB = inMemoryCompactionLimitMB;
    }

    public void setMaxHeapSize(Map<String, String> maxHeapSize) {
        this.maxHeapSize = maxHeapSize;
    }

    public void setMaxNewGenHeapSize(Map<String, String> maxNewGenHeapSize) {
        this.maxNewGenHeapSize = maxNewGenHeapSize;
    }

    public void setHeapDumpLocation(String heapDumpLocation) {
        this.heapDumpLocation = heapDumpLocation;
    }

    public void setMemtableTotalSpaceMB(Integer memtableTotalSpaceMB) {
        this.memtableTotalSpaceMB = memtableTotalSpaceMB;
    }

    public void setStreamingThroughputMbps(Integer streamingThroughputMbps) {
        this.streamingThroughputMbps = streamingThroughputMbps;
    }

    public void setHintedHandoffThrottleKB(long hintedHandoffThrottleKB) {
        this.hintedHandoffThrottleKB = hintedHandoffThrottleKB;
    }

    public void setMaxHintWindowMS(long maxHintWindowMS) {
        this.maxHintWindowMS = maxHintWindowMS;
    }

    public void setRpcServerType(String rpcServerType) {
        this.rpcServerType = rpcServerType;
    }

    public void setIndexInterval(int indexInterval) {
        this.indexInterval = indexInterval;
    }

    public void setAuthenticator(String authenticator) {
        this.authenticator = authenticator;
    }

    public void setAuthorizer(String authorizer) {
        this.authorizer = authorizer;
    }

    public void setCacheLocation(String cacheLocation) {
        this.cacheLocation = cacheLocation;
    }

    public void setCommitLogLocation(String commitLogLocation) {
        this.commitLogLocation = commitLogLocation;
    }

    public void setSeedProviderClassName(String seedProviderClassName) {
        this.seedProviderClassName = seedProviderClassName;
    }

    public void setKeyCacheSizeInMB(Integer keyCacheSizeInMB) {
        this.keyCacheSizeInMB = keyCacheSizeInMB;
    }

    public void setKeyCacheKeysToSave(Integer keyCacheKeysToSave) {
        this.keyCacheKeysToSave = keyCacheKeysToSave;
    }

    public void setRowCacheSizeInMB(Integer rowCacheSizeInMB) {
        this.rowCacheSizeInMB = rowCacheSizeInMB;
    }

    public void setRowCacheKeysToSave(Integer rowCacheKeysToSave) {
        this.rowCacheKeysToSave = rowCacheKeysToSave;
    }

    public void setInternodeCompression(String internodeCompression) {
        this.internodeCompression = internodeCompression;
    }

    public void setInterDcTcpNodelay(boolean interDcTcpNodelay) {
        this.interDcTcpNodelay = interDcTcpNodelay;
    }

    public void setConcurrentReads(int concurrentReads) {
        this.concurrentReads = concurrentReads;
    }

    public void setConcurrentWrites(int concurrentWrites) {
        this.concurrentWrites = concurrentWrites;
    }

    public void setConcurrentCompactors(Integer concurrentCompactors) {
        this.concurrentCompactors = concurrentCompactors;
    }

    public void setClientSslEnabled(boolean clientSslEnabled) {
        this.clientSslEnabled = clientSslEnabled;
    }

    public void setInternodeEncryption(String internodeEncryption) {
        this.internodeEncryption = internodeEncryption;
    }

    public void setNodeRepairEnabled(boolean nodeRepairEnabled) {
        this.nodeRepairEnabled = nodeRepairEnabled;
    }

    public void setNodeRepairMutexAcquireTimeOut(int nodeRepairMutexAcquireTimeOut) {
        this.nodeRepairMutexAcquireTimeOut = nodeRepairMutexAcquireTimeOut;
    }

    public void setNodeRepairTime(String nodeRepairTime) {
        this.nodeRepairTime = nodeRepairTime;
    }

    public void setExtraConfigParams(Map<String, String> extraConfigParams) {
        this.extraConfigParams = extraConfigParams;
    }

    public long getReadRequestTimeoutInMs() {
        return readRequestTimeoutInMs;
    }

    public long getRangeRequestTimeoutInMs() {
        return rangeRequestTimeoutInMs;
    }

    public long getWriteRequestTimeoutInMs() {
        return writeRequestTimeoutInMs;
    }

    public long getRequestTimeoutInMs() {
        return requestTimeoutInMs;
    }
}
