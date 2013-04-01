package com.netflix.priam.tools;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.netflix.priam.utils.SystemUtils;
import com.yammer.dropwizard.cli.Command;
import com.yammer.dropwizard.config.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.service.StorageService;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;

/**
 * Example:
 *  java -jar priam.jar get-sstables-for-key --keyspace keyspace --columnfamily columnfamily --key hex-key --config /etc/cassandra/conf/cassandra.yaml
 */
public class GetSSTablesForKey extends Command {

    public GetSSTablesForKey() {
        super("get-sstables-for-key", "List SSTables containing a specified key.");
    }

    @Override
    public void configure(Subparser subparser) {
        subparser.addArgument("--keyspace").required(true).help("Keyspace name");
        subparser.addArgument("--columnfamily").required(true).help("Column family name");
        subparser.addArgument("--key").nargs("+").type(String.class).help("One or more keys, hex-encoded");
        subparser.addArgument("--config").setDefault("/etc/cassandra/conf/cassandra.yaml").help("Path to cassandra.yaml");
    }

    @Override
    public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
        String keyspace = namespace.getString("keyspace");
        String columnFamily = namespace.getString("columnfamily");
        List<String> keys = namespace.getList("key");
        String config = namespace.getString("config");

        // Initialize Cassandra, read cassandra.yaml and load metadata about the active SSTables.
        System.setProperty("cassandra.config", new File(config).toURI().toString());
        DatabaseDescriptor.loadSchemas();

        List<DecoratedKey<?>> decoratedKeys = Ordering.natural().sortedCopy(Iterables.transform(keys,
                new Function<String, DecoratedKey<?>>() {
                    @Override
                    public DecoratedKey<?> apply(String key) {
                        return StorageService.getPartitioner().decorateKey(hexToBytes(key));
                    }
                }));

        Multimap<DecoratedKey<?>, Info> ssTablesMap = ArrayListMultimap.create();

        Set<Descriptor> descriptors = Directories.create(keyspace, columnFamily)
                .sstableLister().skipCompacted(true).skipTemporary(true).list().keySet();
        if (descriptors.isEmpty()) {
            System.err.println(format("Invalid keyspace/columnfamily: %s/%s", keyspace, columnFamily));
            System.exit(2);
        }

        for (Descriptor descriptor : descriptors) {
            SSTableReader reader = SSTableReader.open(descriptor);
            SSTableScanner scanner = reader.getDirectScanner();
            for (DecoratedKey<?> decoratedKey : decoratedKeys) {
                scanner.seekTo(decoratedKey);
                if (!scanner.hasNext())
                    continue;
                SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
                if (!row.getKey().equals(decoratedKey))
                    continue;
                int numColumns = row.getColumnCount();
                long numBytes = row.dataSize;
                boolean delete = row.getColumnFamily().isMarkedForDelete();
                ssTablesMap.put(decoratedKey, new Info(descriptor, numColumns, numBytes, delete));
            }
            scanner.close();
        }

        for (DecoratedKey<?> decoratedKey : decoratedKeys) {
            System.out.println("Key: " + bytesToHex(decoratedKey.key));

            Collection<Info> ssTables = ssTablesMap.get(decoratedKey);
            for (Info info : Ordering.natural().sortedCopy(ssTables)) {
                String dataFile = info.ssTable.filenameFor(Component.DATA);
                System.out.println(format("    %s (file=%s, row=%s, cols=%d%s)",
                        dataFile,
                        SystemUtils.formatSize(new File(dataFile).length()),
                        SystemUtils.formatSize(info.numBytes),
                        info.numColumns,
                        info.delete ? ", deleted" : ""));
            }
            if (ssTables.isEmpty()) {
                System.out.println("    <not-found>");
            }
        }
    }

    private static class Info implements Comparable<Info> {
        final Descriptor ssTable;
        final int numColumns;
        final long numBytes;
        final boolean delete;

        Info(Descriptor ssTable, int numColumns, long numBytes, boolean delete) {
            this.ssTable = ssTable;
            this.numColumns = numColumns;
            this.numBytes = numBytes;
            this.delete = delete;
        }

        @Override
        public int compareTo(Info info) {
            return Ints.compare(ssTable.generation, info.ssTable.generation);
        }
    }
}
