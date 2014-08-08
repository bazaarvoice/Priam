package com.netflix.priam;

import com.netflix.priam.config.CassandraConfiguration;

public class TestCassandraConfiguration extends CassandraConfiguration {

    public TestCassandraConfiguration(String clusterName) {
        setClusterName(clusterName);
        setDataLocation("target/data");
        setHeapDumpLocation("target/heaps");
        setCacheLocation("cass/caches");
        setJmxPort(7199);
        setThriftPort(9160);
        setStoragePort(7101);
        setSslStoragePort(7103);
        setEndpointSnitch("org.apache.cassandra.locator.SimpleSnitch");
        setSeedProviderClassName("org.apache.cassandra.locator.SimpleSeedProvider");
        setInMemoryCompactionLimitMB(8);
        setStreamingThroughputMbps(400);
        setCompactionThroughputMBPerSec(0);
        setCassStopScript("teststopscript");
        setMaxHintWindowMS(36000);
        setHintedHandoffThrottleKB(0);
        setMemtableTotalSpaceMB(0);
        setKeyCacheSizeInMB(16);
        setKeyCacheKeysToSave(32);
        setRowCacheSizeInMB(4);
        setRowCacheKeysToSave(4);
    }
}
