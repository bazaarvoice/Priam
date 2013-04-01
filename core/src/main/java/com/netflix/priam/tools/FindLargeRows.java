package com.netflix.priam.tools;

import com.google.common.collect.Lists;
import com.yammer.dropwizard.cli.Command;
import com.yammer.dropwizard.config.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.lang.String.format;
import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;

/**
 * Example:
 *  java  -Dcassandra.config=file:///etc/cassandra/conf/cassandra.yaml -jar priam.jar /var/lib/cassandra/data
 */
public class FindLargeRows extends Command {

    public FindLargeRows() {
        super("find-large-rows", "Scan SSTables and print keys for rows larger than a threshold.");
    }

    @Override
    public void configure(Subparser subparser) {
        subparser.addArgument("sstable").nargs("+").help("File or directory containing SSTable data");
        subparser.addArgument("--columns").setDefault(100).type(Integer.class);
        subparser.addArgument("--bytes").setDefault(0x100000L).type(Long.class);
    }

    @Override
    public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
        List<String> ssTableFilenames = namespace.getList("sstable");
        int thresholdColumns = namespace.getInt("columns");
        long thresholdBytes = namespace.getLong("bytes");

        List<File> ssTableFiles = expand(ssTableFilenames);

        // Assumes specific system properties are set: TODO
        DatabaseDescriptor.loadSchemas();

        if (Schema.instance.getNonSystemTables().size() < 1) {
            System.err.println("No non-system tables are defined");
            System.exit(1);
        }

        for (File ssTableFile : ssTableFiles) {
            Descriptor descriptor = Descriptor.fromFilename(ssTableFile.getPath());

            String fileName = ssTableFile.getName();
            String ksName = descriptor.ksname;
            String cfName = descriptor.cfname;

            if (Schema.instance.getCFMetaData(descriptor) == null) {
                System.err.println(format("The SSTable %s is not part of this cassandra database: keyspace = %s, column family = %s",
                        ssTableFile, ksName, cfName));
                continue;
            }

            boolean emoMultiTenant = cfName.endsWith("_delta") || cfName.endsWith("_audit") || cfName.endsWith("_blob");

            SSTableReader reader = SSTableReader.open(descriptor);
            SSTableScanner scanner = reader.getDirectScanner();
            while (scanner.hasNext()) {
                SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();

                String rowKey = bytesToHex(row.getKey().key);

                int numColumns = row.getColumnCount();
                long numBytes = row.dataSize;
                if (numColumns < thresholdColumns && numBytes < thresholdBytes) {
                    continue;
                }

                if (emoMultiTenant) {
                    String emoTable = rowKey.substring(2, 18);
                    String emoKey = UTF8Type.instance.getString(hexToBytes(rowKey.substring(18)));
                    System.out.println(format("%6d  %9d  %s  %s  %s  %s",
                            numColumns, numBytes, fileName, emoTable, emoKey, rowKey));
                } else {
                    System.out.println(format("%6d  %9d  %s  %s",
                            numColumns, numBytes, fileName, rowKey));
                }
            }
            scanner.close();
        }
    }

    private List<File> expand(Collection<String> filenames) {
        List<File> files = Lists.newArrayList();
        for (String filename : filenames) {
            File file = new File(filename).getAbsoluteFile();
            if (file.isDirectory()) {
                walkDirectory(file, files);
            } else {
                files.add(file);
            }
        }
        Collections.sort(files);
        return files;
    }

    private void walkDirectory(File dir, Collection<File> list) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    walkDirectory(file, list);
                } else if (file.getName().endsWith("Data.db")) {
                    list.add(file);
                }
            }
        }
    }
}
