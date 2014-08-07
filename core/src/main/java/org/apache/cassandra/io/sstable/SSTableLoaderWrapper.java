package org.apache.cassandra.io.sstable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.utils.CassandraTuner;
import org.apache.cassandra.io.sstable.SSTableLoader.Client;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.streaming.FileStreamTask;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.streaming.PendingFile;
import org.apache.cassandra.streaming.StreamHeader;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Singleton
public class SSTableLoaderWrapper {
    private static final Logger logger = LoggerFactory.getLogger(SSTableLoaderWrapper.class);
    private static final Set<Component> allComponents = ImmutableSet.of(
            Component.COMPRESSION_INFO, Component.DATA, Component.FILTER, Component.PRIMARY_INDEX, Component.STATS, Component.DIGEST);

    private final CassandraConfiguration cassandraConfiguration;

    @Inject
    public SSTableLoaderWrapper(CassandraConfiguration cassandraConfiguration, CassandraTuner tuner) throws IOException {
        this.cassandraConfiguration = cassandraConfiguration;
        String srcCassYamlFile = cassandraConfiguration.getYamlLocation();
        String targetYamlLocation = "/tmp/";

        File sourceFile = new File(srcCassYamlFile);
        File targetFile = new File(targetYamlLocation + "incr-restore-cassandra.yaml");
        logger.info("Copying file : {} to --> {}", sourceFile.getName(), targetFile.getName());

        //copy file from one location to another
        Files.copy(sourceFile, targetFile);

        logger.info("Trying to load the yaml file from: {}", targetFile);
        tuner.writeAllProperties(targetFile.getPath(), "localhost", "org.apache.cassandra.locator.SimpleSeedProvider");
        System.setProperty("cassandra.config", "file:" + targetFile.getPath());
    }

    private final OutputHandler options = new OutputHandler() {
        public void output(String msg) {
            logger.info(msg + "\n");
        }

        public void debug(String msg) {
            logger.debug(msg + "\n");
        }

        public void warn(String msg) {
            logger.warn(msg + "\n");
        }

        public void warn(String msg, Throwable th) {
            logger.warn(msg + "\n", th);
        }
    };

    /**
     * Not multi-threaded intentionally.
     */
    public Collection<PendingFile> stream(File directory) throws UnknownHostException {
        Client client = new Client() {
            public boolean validateColumnFamily(String keyspace, String cfName) {
                return true;
            }

            public void init(String keyspace) {
            }
        };
        try {
            client.setPartitioner(cassandraConfiguration.getPartitioner());
        } catch (Exception e) {
            logger.error("Configuration Exception while setting partitioner", e);
        }

        SSTableLoader loader = new SSTableLoader(directory, client, options);

        Collection<PendingFile> pendingFiles = Lists.newArrayList();
        for (SSTableReader sstable : loader.openSSTables()) {
            Descriptor desc = sstable.descriptor;
            List<Pair<Long, Long>> sections = ImmutableList.of(Pair.create(0L, sstable.onDiskLength()));
            PendingFile pending = new PendingFile(sstable, desc, SSTable.COMPONENT_DATA, sections, OperationType.BULK_LOAD);
            StreamHeader header = new StreamHeader(directory.getName(), UUID.randomUUID(), pending, Collections.singleton(pending));
            logger.info("Streaming to {}", InetAddress.getLocalHost());
            new FileStreamTask(header, InetAddress.getLocalHost()).run();
            logger.info("Done Streaming: {}", pending);
            sstable.releaseReference();
            pendingFiles.add(pending);
        }
        return pendingFiles;
    }

    public void deleteCompleted(Collection<PendingFile> sstables) throws IOException {
        logger.info("Restored SST's Now Deleting: {}", StringUtils.join(sstables, ","));
        for (PendingFile file : sstables) {
            for (Component component : allComponents) {
                FileUtils.delete(file.sstable.descriptor.filenameFor(component));
            }
        }
    }
}
