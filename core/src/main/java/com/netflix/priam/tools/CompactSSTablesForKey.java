package com.netflix.priam.tools;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.netflix.priam.utils.JMXNodeTool;
import com.netflix.priam.utils.SystemUtils;
import com.yammer.dropwizard.cli.Command;
import com.yammer.dropwizard.config.Bootstrap;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.service.StorageService;

import java.io.File;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;

/**
 * Note: this works only for SizeTieredCompactionStrategy column families.
 *
 * Example:
 *  java -jar priam.jar compact-sstables-for-key --keyspace keyspace --columnfamily columnfamily --key hex-key --config /etc/cassandra/conf/cassandra.yaml
 */
public class CompactSSTablesForKey extends Command {

    public CompactSSTablesForKey() {
        super("compact-sstables-for-key", "Compact SSTables containing a specified key.");
    }

    @Override
    public void configure(Subparser subparser) {
        subparser.addArgument("--keyspace").required(true).help("Keyspace name");
        subparser.addArgument("--columnfamily").required(true).help("Column family name");
        subparser.addArgument("--key").nargs("+").type(String.class).help("One or more keys, hex-encoded");
        subparser.addArgument("--noop").action(Arguments.storeTrue()).help("Print what would be compacted");
        subparser.addArgument("--config").setDefault("/etc/cassandra/conf/cassandra.yaml").help("Path to cassandra.yaml");
    }

    @Override
    public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
        String keyspace = namespace.getString("keyspace");
        String columnFamily = namespace.getString("columnfamily");
        List<String> keys = namespace.getList("key");
        boolean noop = namespace.getBoolean("noop");
        String config = namespace.getString("config");

        // Initialize Cassandra, read cassandra.yaml.
        System.setProperty("cassandra.config", new File(config).toURI().toString());
        final IPartitioner partitioner = StorageService.getPartitioner();

        List<DecoratedKey<?>> decoratedKeys = Ordering.natural().sortedCopy(Iterables.transform(keys,
                new Function<String, DecoratedKey<?>>() {
                    @Override
                    public DecoratedKey<?> apply(String key) {
                        return partitioner.decorateKey(hexToBytes(key));
                    }
                }));

        Set<String> ssTables = Sets.newTreeSet();

        Directories.SSTableLister ssTableLister = Directories.create(keyspace, columnFamily)
                .sstableLister().skipCompacted(true).skipTemporary(true);
        for (Descriptor descriptor : ssTableLister.list().keySet()) {
            SSTableReader reader = SSTableReader.open(descriptor);
            SSTableScanner scanner = reader.getDirectScanner();
            for (DecoratedKey<?> decoratedKey : decoratedKeys) {
                if (reader.getPosition(decoratedKey, SSTableReader.Operator.EQ, false) > -1) {
                    ssTables.add(descriptor.filenameFor(Component.DATA));
                }
            }
            scanner.close();
        }

        if (ssTables.isEmpty()) {
            System.out.println("No SSTables found.");
            System.exit(0);
        }

        System.out.println(noop ? "Would compact:" : "Compacting:");
        for (String ssTable : ssTables) {
            System.out.println(format("  %s (%s)", ssTable, SystemUtils.formatSize(new File(ssTable).length())));
        }

        if (!noop) {
            JMXNodeTool nodeTool = new JMXNodeTool("127.0.0.1", 7199);
            try {
                nodeTool.compactSSTables(keyspace, Joiner.on(',').join(Iterables.transform(ssTables, new Function<String, String>() {
                    @Override
                    public String apply(String filename) {
                        return new File(filename).getName();
                    }
                })));
            } finally {
                nodeTool.close();
            }
        }
    }
}
