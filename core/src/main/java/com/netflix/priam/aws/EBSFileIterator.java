package com.netflix.priam.aws;

import com.google.common.collect.Lists;
import com.google.inject.Provider;
import com.netflix.priam.backup.AbstractBackupPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Iterator representing list of backup files available on S3
 */
public class EBSFileIterator implements Iterator<AbstractBackupPath> {
    private static final Logger logger = LoggerFactory.getLogger(EBSFileIterator.class);
    private final Provider<AbstractBackupPath> pathProvider;
    private final Date start;
    private final Date till;
    private Iterator<AbstractBackupPath> iterator;
    private File objectListing;

    public EBSFileIterator(Provider<AbstractBackupPath> pathProvider, String path, Date start, Date till) {
        this.start = start;
        this.till = till;
        this.pathProvider = pathProvider;

        objectListing = new File(pathProvider.get().remotePrefix(start, till, path));
        iterator = createIterator();

    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    private Iterator<AbstractBackupPath> createIterator() {
        List<AbstractBackupPath> temp = Lists.newArrayList();

        assert null != objectListing.listFiles() : "No files to yielded from your EBS volume!";

        for ( File nextFile : objectListing.listFiles() ) {
            AbstractBackupPath path = pathProvider.get();
            path.parseRemote(nextFile.getName());
            logger.info("New file " + nextFile.getName() + " path = " + path.getRemotePath() + " " + start + " end: " + till + " my " + path.getTime());
            if ((path.getTime().after(start) && path.getTime().before(till)) || path.getTime().equals(start)) {
                temp.add(path);
                logger.debug("Added file " + nextFile.getName());
            }
        }

        return temp.iterator();
    }

    @Override
    public AbstractBackupPath next() {
        return iterator.next();
    }

    @Override
    public void remove() {
        throw new IllegalStateException();
    }
}
