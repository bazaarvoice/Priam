package com.netflix.priam.defaultimpl;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.utils.CassandraTuner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class StandardTuner implements CassandraTuner {
    private static final Logger logger = LoggerFactory.getLogger(StandardTuner.class);
    private static final String CL_BACKUP_PROPS_FILE = "/conf/commitlog_archiving.properties";

    private final CassandraConfiguration cassandraConfiguration;
    private final BackupConfiguration backupConfiguration;
    private final AmazonConfiguration amazonConfiguration;

    @Inject
    public StandardTuner(CassandraConfiguration cassandraConfiguration, BackupConfiguration backupConfiguration, AmazonConfiguration amazonConfiguration) {
        this.cassandraConfiguration = cassandraConfiguration;
        this.backupConfiguration = backupConfiguration;
        this.amazonConfiguration = amazonConfiguration;
    }

    @Override
    public void writeAllProperties(String yamlLocation, String hostIp, String seedProvider) throws IOException {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        File yamlFile = new File(yamlLocation);
        Map<String, Object> map = load(yaml, yamlFile);

        put(map, "cluster_name", cassandraConfiguration.getClusterName());
        put(map, "storage_port", cassandraConfiguration.getStoragePort());
        put(map, "ssl_storage_port", cassandraConfiguration.getSslStoragePort());
        put(map, "start_rpc", cassandraConfiguration.isThriftEnabled());
        put(map, "rpc_port", cassandraConfiguration.getThriftPort());
        put(map, "start_native_transport", cassandraConfiguration.isNativeTransportEnabled());
        put(map, "native_transport_port", cassandraConfiguration.getNativeTransportPort());
        put(map, "listen_address", hostIp);
        put(map, "rpc_address", hostIp);
        put(map, "auto_bootstrap", cassandraConfiguration.getAutoBootstrap());
        put(map, "saved_caches_directory", cassandraConfiguration.getCacheLocation());
        put(map, "commitlog_directory", cassandraConfiguration.getCommitLogLocation());
        put(map, "data_file_directories", ImmutableList.of(cassandraConfiguration.getDataLocation()));
        put(map, "incremental_backups", backupConfiguration.isIncrementalBackupEnabledForCassandra());
        put(map, "endpoint_snitch", cassandraConfiguration.getEndpointSnitch());
        put(map, "in_memory_compaction_limit_in_mb", cassandraConfiguration.getInMemoryCompactionLimitMB());
        put(map, "compaction_throughput_mb_per_sec", cassandraConfiguration.getCompactionThroughputMBPerSec());
        put(map, "partitioner", derivePartitioner(map.get("partitioner").toString(), cassandraConfiguration.getPartitioner()));

        put(map, "memtable_total_space_in_mb", cassandraConfiguration.getMemtableTotalSpaceMB());
        put(map, "stream_throughput_outbound_megabits_per_sec", cassandraConfiguration.getStreamingThroughputMbps());

        put(map, "max_hint_window_in_ms", cassandraConfiguration.getMaxHintWindowMS());
        put(map, "hinted_handoff_throttle_in_kb", cassandraConfiguration.getHintedHandoffThrottleKB());
        put(map, "authenticator", cassandraConfiguration.getAuthenticator());
        put(map, "authorizer", cassandraConfiguration.getAuthorizer());
        put(map, "internode_compression", cassandraConfiguration.getInternodeCompression());
        put(map, "inter_dc_tcp_nodelay", cassandraConfiguration.isInterDcTcpNodelay());

        put(map, "concurrent_reads", cassandraConfiguration.getConcurrentReads());
        put(map, "concurrent_writes", cassandraConfiguration.getConcurrentWrites());
        put(map, "concurrent_compactors", cassandraConfiguration.getConcurrentCompactors());

        put(map, "rpc_server_type", cassandraConfiguration.getRpcServerType());
        put(map, "index_interval", cassandraConfiguration.getIndexInterval());  // Removed in Cassandra 2.1

        List<Map<String, Object>> seedp = get(map, "seed_provider");
        Map<String, Object> m = seedp.get(0);
        put(m, "class_name", seedProvider);

        configureSecurity(map);
        configureGlobalCaches(cassandraConfiguration, map);

        //force to 1 until vnodes are properly supported
        put(map, "num_tokens", 1);

        addExtraCassParams(map);

        logger.info(yaml.dump(map));
        yaml.dump(map, new FileWriter(yamlFile));

        configureCommitLogBackups();
    }

    /**
     * Setup the cassandra 1.1 global cache values
     */
    private void configureGlobalCaches(CassandraConfiguration cassandraConfiguration, Map<String, Object> yaml) {
        Integer keyCacheSize = cassandraConfiguration.getKeyCacheSizeInMB();
        if (keyCacheSize != null) {
            put(yaml, "key_cache_size_in_mb", keyCacheSize);
            put(yaml, "key_cache_keys_to_save", cassandraConfiguration.getKeyCacheKeysToSave());
        }

        Integer rowCacheSize = cassandraConfiguration.getRowCacheSizeInMB();
        if (rowCacheSize != null) {
            put(yaml, "row_cache_size_in_mb", rowCacheSize);
            put(yaml, "row_cache_keys_to_save", cassandraConfiguration.getRowCacheKeysToSave());
        }
    }

    private String derivePartitioner(String fromYaml, String fromConfig) {
        if (Strings.isNullOrEmpty(fromYaml)) {
            return fromConfig;
        }
        //this check is to prevent against overwriting an existing yaml file that has
        // a partitioner not RandomPartitioner or (as of cass 1.2) Murmur3Partitioner.
        //basically we don't want to hose existing deployments by changing the partitioner unexpectedly on them
        final String lowerCase = fromYaml.toLowerCase();
        if (lowerCase.contains("randomparti") || lowerCase.contains("murmur")) {
            return fromConfig;
        }
        return fromYaml;
    }

    private void configureSecurity(Map<String, Object> map) {
        //the client-side ssl settings
        Map<String, Object> clientEnc = get(map, "client_encryption_options");
        put(clientEnc, "enabled", cassandraConfiguration.isClientSslEnabled());

        //the server-side (internode) ssl settings
        Map<String, Object> serverEnc = get(map, "server_encryption_options");
        put(serverEnc, "internode_encryption", cassandraConfiguration.getInternodeEncryption());
    }

    private void configureCommitLogBackups() throws IOException {
        if (!backupConfiguration.isCommitLogBackupEnabled()) {
            return;
        }
        Properties props = new Properties();
        props.put("archive_command", backupConfiguration.getCommitLogBackupArchiveCmd());
        props.put("restore_command", backupConfiguration.getCommitLogBackupRestoreCmd());
        props.put("restore_directories", backupConfiguration.getCommitLogBackupRestoreFromDirs());
        props.put("restore_point_in_time", backupConfiguration.getCommitLogBackupRestorePointInTime());

        File commitLogProperties = new File(cassandraConfiguration.getCassHome() + CL_BACKUP_PROPS_FILE);
        try (FileOutputStream fos = new FileOutputStream(commitLogProperties)) {
            props.store(fos, "cassandra commit log archive props, as written by priam");
        }
    }

    @Override
    public void updateAutoBootstrap(String yamlFile, boolean autobootstrap) throws IOException {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        Map<String, Object> map = load(yaml, new File(yamlFile));

        put(map, "auto_bootstrap", autobootstrap); //Don't bootstrap in restore mode

        logger.info("Updating yaml {}", yaml.dump(map));
        yaml.dump(map, new FileWriter(yamlFile));
    }

    private void addExtraCassParams(Map<String, Object> map) {
        Map<String, String> params = cassandraConfiguration.getExtraConfigParams();
        if (params == null) {
            logger.info("Updating yaml: no extra cass params");
            return;
        }

        logger.info("Updating yaml: adding extra cass params");
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String cassKey = entry.getKey();
            String cassVal = entry.getValue();
            logger.info("Updating yaml: CassKey[{}], Val[{}]", cassKey, cassVal);
            put(map, cassKey, cassVal);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> load(Yaml yaml, File yamlFile) throws FileNotFoundException {
        return (Map<String, Object>) yaml.load(new FileInputStream(yamlFile));
    }

    @SuppressWarnings("unchecked")
    private <T> T get(Map<String, Object> map, String key) {
        return (T) map.get(key);
    }

    private void put(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }
}
