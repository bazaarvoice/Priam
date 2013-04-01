package com.netflix.priam.tools;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.netflix.priam.utils.SystemUtils;
import com.yammer.dropwizard.cli.Command;
import com.yammer.dropwizard.config.Bootstrap;
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
import java.util.Collection;
import java.util.List;

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

        Multimap<DecoratedKey<?>, String> ssTablesMap = ArrayListMultimap.create();

        Directories.SSTableLister ssTableLister = Directories.create(keyspace, columnFamily)
                .sstableLister().skipCompacted(true).skipTemporary(true);
        for (Descriptor descriptor : ssTableLister.list().keySet()) {
            SSTableReader reader = SSTableReader.open(descriptor);
            SSTableScanner scanner = reader.getDirectScanner();
            for (DecoratedKey<?> decoratedKey : decoratedKeys) {
                if (reader.getPosition(decoratedKey, SSTableReader.Operator.EQ, false) > -1) {
                    ssTablesMap.put(decoratedKey, descriptor.filenameFor(Component.DATA));
                }
            }
            scanner.close();
        }

        for (DecoratedKey<?> decoratedKey : decoratedKeys) {
            System.out.println("Key: " + bytesToHex(decoratedKey.key));

            Collection<String> ssTables = ssTablesMap.get(decoratedKey);
            for (String ssTable : Ordering.natural().sortedCopy(ssTables)) {
                System.out.println(format("    %s (%s)", ssTable, SystemUtils.formatSize(new File(ssTable).length())));
            }
            if (ssTables.isEmpty()) {
                System.out.println("    <not-found>");
            }
        }
    }

}
