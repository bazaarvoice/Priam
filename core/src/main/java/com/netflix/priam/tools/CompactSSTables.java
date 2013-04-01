package com.netflix.priam.tools;

import com.google.common.base.Joiner;
import com.netflix.priam.utils.JMXNodeTool;
import com.yammer.dropwizard.cli.Command;
import com.yammer.dropwizard.config.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.util.List;

/**
 * Note: this works only for SizeTieredCompactionStrategy column families.
 *
 * Example:
 *  java -jar priam.jar compact-sstables --keyspace keyspace --sstables ks-cf-he-86-Data.db ks-cf-he-92-Data.db
 */
public class CompactSSTables extends Command {

    public CompactSSTables() {
        super("compact-sstables", "Compact specific SizeTieredCompactionStrategy SSTables.");
    }

    @Override
    public void configure(Subparser subparser) {
        subparser.addArgument("--keyspace").required(true).help("Keyspace name");
        subparser.addArgument("--sstables").nargs("+").help("SSTable files to compact (*-Data.db)");
    }

    @Override
    public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
        String keyspace = namespace.getString("keyspace");
        List<String> sstables = namespace.getList("sstables");

        JMXNodeTool nodeTool = new JMXNodeTool("127.0.0.1", 7199);
        try {
            nodeTool.compactSSTables(keyspace, Joiner.on(',').join(sstables));
        } finally {
            nodeTool.close();
        }
    }
}
