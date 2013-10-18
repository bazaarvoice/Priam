package com.netflix.priam.aws;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Class to iterate over prefixes (S3 Common prefixes) upto
 * the token element in the path. The abstract path generated by this class
 * is partial (does not have all data).
 */
public class EBSPrefixIterator implements Iterator<AbstractBackupPath> {
    private static final Logger logger = LoggerFactory.getLogger(EBSPrefixIterator.class);
    private final CassandraConfiguration cassandraConfiguration;
    private final AmazonConfiguration amazonConfiguration;
    private final BackupConfiguration backupConfiguration;
    private final Provider<AbstractBackupPath> pathProvider;
    private Iterator<AbstractBackupPath> iterator;

    private String backupRoot = "";
    private String clusterPath = "";
    private SimpleDateFormat datefmt = new SimpleDateFormat("yyyyMMdd");
    private File[] fileListing;
    Date date;

    @Inject
    public EBSPrefixIterator(CassandraConfiguration cassandraConfiguration, AmazonConfiguration amazonConfiguration, BackupConfiguration backupConfiguration, Provider<AbstractBackupPath> pathProvider, Date date) {
        this.cassandraConfiguration = cassandraConfiguration;
        this.amazonConfiguration = amazonConfiguration;
        this.backupConfiguration = backupConfiguration;
        this.pathProvider = pathProvider;
        this.date = date;
        String path = "";

        if (StringUtils.isNotBlank(backupConfiguration.getRestorePrefix())) {
            path = backupConfiguration.getRestorePrefix();
        }

        String[] paths = path.split(String.valueOf(EBSBackupPath.PATH_SEP));
        backupRoot = paths[0];
        this.clusterPath = remotePrefix(path);
        iterator = createIterator();
    }

    private void initListing() {

        File listing = new File(backupRoot + clusterPath);
        logger.info("Using cluster prefix for searching tokens: " + clusterPath);
        fileListing = listing.listFiles();

    }

    private Iterator<AbstractBackupPath> createIterator() {
        if (null == fileListing) {
            initListing();
        }

        List<AbstractBackupPath> temp = Lists.newArrayList();
        for (File file : fileListing ) {
            if (pathExistsForDate(file.getPath(), datefmt.format(date))) {
                AbstractBackupPath path = pathProvider.get();
                path.parsePartialPrefix(file.getPath());
                temp.add(path);
            }
        }
        return temp.iterator();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public AbstractBackupPath next() {
        return iterator.next();
    }

    @Override
    public void remove() {
    }

    /**
     * Get remote prefix up to the token
     */
    private String remotePrefix(String location) {
        StringBuffer buff = new StringBuffer();
        String[] elements = location.split(String.valueOf(EBSBackupPath.PATH_SEP));
        if (elements.length <= 1) {
            buff.append(backupConfiguration.getBaseDir()).append(EBSBackupPath.PATH_SEP);
            buff.append(amazonConfiguration.getRegionName()).append(EBSBackupPath.PATH_SEP);
            buff.append(cassandraConfiguration.getClusterName()).append(EBSBackupPath.PATH_SEP);
        } else {
            assert elements.length >= 4 : "Too few elements in path " + location;
            buff.append(elements[1]).append(EBSBackupPath.PATH_SEP);
            buff.append(elements[2]).append(EBSBackupPath.PATH_SEP);
            buff.append(elements[3]).append(EBSBackupPath.PATH_SEP);
        }
        return buff.toString();
    }

    /**
     * Check to see if the path exists for the date
     */
    private boolean pathExistsForDate(String tprefix, String datestr) {

        boolean pathExists = new File(backupRoot + tprefix + datestr).exists();

        return pathExists;

    }

}